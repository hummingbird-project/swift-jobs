//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2025 the Hummingbird authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See hummingbird/CONTRIBUTORS.txt for the list of Hummingbird authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Atomics
import Jobs
import Logging
import NIOCore
import ServiceLifecycle
import Synchronization

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Main workflow engine
/// Workflow execution engine with scheduling support
public struct WorkflowEngine<Queue: WorkflowQueueDriver>: Sendable, WorkflowEngineProtocol {
    internal let jobQueue: JobQueue<Queue>
    internal let logger: Logger
    private let workflowRegistry: WorkflowRegistry
    internal let internalRegistry: ActivityRegistry
    private let sleepCompletions: SleepCompletionStorage<Queue.JobID>
    private let activityCompletions: ActivityCompletionStorage
    private var jobSchedule: JobSchedule

    public init(
        queue: Queue,
        logger: Logger,
        activities: [any ActivityContainer] = []
    ) {
        self.logger = logger
        self.workflowRegistry = WorkflowRegistry()
        self.internalRegistry = ActivityRegistry()
        self.jobSchedule = JobSchedule()

        // Create storage first
        let completionStorage = SleepCompletionStorage<Queue.JobID>()
        let activityStorage = ActivityCompletionStorage()
        self.sleepCompletions = completionStorage
        self.activityCompletions = activityStorage

        // Build JobQueue with callback middleware for sleep and activity handling
        self.jobQueue = JobQueue(queue, logger: logger) {
            SleepCallbackMiddleware<Queue.JobID>(storage: completionStorage)
            ActivityCallbackMiddleware(storage: activityStorage)
            WorkflowCompletionMiddleware(repository: queue.workflowRepository)
        }

        // Register activities from containers
        internalRegistry.registerActivities(from: activities)

        self.registerWorkflowCoordinator()
        self.registerActivityExecutor()
        self.registerDelayJob()
        self.registerScheduledWorkflowJob()

    }

    /// Wait for a sleep operation to complete via callback
    internal func waitForSleep(sleepKey: String) async throws {
        try await sleepCompletions.waitForSleep(sleepKey: sleepKey)
    }

    /// Wait for an activity to complete via callback then retrieve result from storage
    internal func waitForActivity<Output: Codable & Sendable>(activityId: String, expectedType: Output.Type) async throws -> Output {
        // Wait for middleware notification that activity completed
        try await activityCompletions.waitForActivity(activityId: activityId)

        // Now retrieve the actual result from storage
        if let result = try await jobQueue.queue.getResult(ActivityID(activityId), resultType: Output.self) {
            switch result {
            case .success(let value):
                return value
            case .failure(let errorInfo):
                // Convert stored error info back to proper exception
                if errorInfo.isCancellation {
                    let cancelledFailure = WorkflowCancelledFailure(message: errorInfo.message, workflowId: WorkflowID("unknown"))
                    throw ActivityFailure(activityName: activityId, cause: cancelledFailure)
                } else {
                    let cause = WorkflowError.activityFailed(errorInfo.message)
                    throw ActivityFailure(activityName: activityId, cause: cause)
                }
            }
        }

        throw WorkflowError.activityTimeout(activityId)
    }

    /// Register a workflow type with the engine
    /// - Parameter workflowType: The workflow type to register
    public func registerWorkflow<W: WorkflowProtocol>(_ workflowType: W.Type) {
        workflowRegistry.registerWorkflow(workflowType)
        logger.debug("Registered workflow", metadata: ["workflowType": .string(W.workflowName)])
    }

    /// Create a processor for this workflow engine
    /// - Parameter options: Processor options
    /// - Returns: A service that can be used to process jobs
    public func processor(options: JobQueueProcessorOptions = .init()) -> any Service {
        jobQueue.processor(options: options)
    }

    /// Start a new workflow execution
    /// - Parameters:
    ///   - workflowType: Type of workflow to execute
    ///   - input: Input parameters for the workflow
    ///   - workflowId: Optional custom workflow ID
    ///   - options: Workflow execution options
    /// - Returns: The workflow ID for the started workflow
    public func startWorkflow<W: WorkflowProtocol>(
        _ workflowType: W.Type,
        input: W.Input,
        workflowId: WorkflowID? = nil,
        options: WorkflowOptions = .init()
    ) async throws -> WorkflowID {
        let id = workflowId ?? WorkflowID()

        logger.debug(
            "🎯 Starting workflow",
            metadata: [
                "workflowId": .string(id.value),
                "workflowType": .string(W.workflowName),
            ]
        )

        // Create workflow execution using repository
        let inputBuffer = try workflowRegistry.encode(input)
        let execution = WorkflowExecution(
            id: id,
            workflowName: W.workflowName,
            inputData: inputBuffer,
            workflowState: nil,
            status: .queued,
            createdAt: .now,
            queuedAt: .now,
            scheduledAt: nil,  // Regular workflows don't have scheduled time
            currentStep: 0,
            totalSteps: 0,
            metadata: [:]
        )

        try await jobQueue.queue.workflowRepository.createExecution(execution)
        logger.debug("✅ Created workflow execution record")

        // Create initial coordinator job
        let coordinatorJob = WorkflowCoordinatorJob(
            workflowId: id,
            workflowType: W.workflowName,
            action: .start,
            stepIndex: 0
        )

        let jobId = try await jobQueue.push(coordinatorJob)
        logger.debug(
            "✅ Queued workflow coordinator job",
            metadata: [
                "jobId": .stringConvertible(jobId),
                "workflowId": .string(id.value),
                "workflowType": .string(W.workflowName),
            ]
        )

        return id
    }

    /// Get workflow execution summary
    /// - Parameter workflowId: The workflow ID to check
    /// - Returns: Current workflow state
    public func getWorkflowSummary(_ workflowId: WorkflowID) async throws -> WorkflowExecutionSummary? {
        try await jobQueue.queue.workflowRepository.getExecutionSummary(workflowId)
    }

    /// Signal a running workflow using type-safe signal parameters
    /// - Parameters:
    ///   - workflowId: The workflow ID to signal
    ///   - signalType: Signal type conforming to SignalParameters
    ///   - input: Signal input data
    public func signalWorkflow<S: SignalParameters>(
        _ workflowId: WorkflowID,
        signalType: S.Type,
        input: S.Input
    ) async throws {
        let signalKey = "workflow_signal:\(workflowId.value):\(S.signalName)"

        let signal = WorkflowSignal(
            signalName: S.signalName,
            data: input,
            timestamp: .now
        )

        let anySignal = try AnyWorkflowSignal(signal)
        let signalBuffer = try workflowRegistry.encode(anySignal)
        let activityResult = ActivityResult<ByteBuffer>.success(signalBuffer)
        try await jobQueue.queue.workflowRepository.saveResult(ActivityID(signalKey), result: activityResult)

        logger.debug(
            "Sent signal to workflow",
            metadata: [
                "workflowId": .string(workflowId.value),
                "signalName": .string(S.signalName),
            ]
        )

        // Check if workflow is waiting for this signal and trigger continuation
        let waitingKey = "workflow_waiting_signal:\(workflowId.value):\(S.signalName)"
        if let _ = try await jobQueue.queue.getMetadata(waitingKey) {
            // Remove the waiting marker
            try await jobQueue.queue.setMetadata(key: waitingKey, value: ByteBuffer())

            // Trigger workflow continuation
            let continuationJob = WorkflowCoordinatorJob(
                workflowId: workflowId,
                workflowType: "",  // Will be filled from workflow state
                action: .continueFromActivity,
                stepIndex: 0  // Will be updated from workflow state
            )

            _ = try await jobQueue.push(continuationJob)
        }
    }

    /// Cancel a running workflow
    /// - Parameter workflowId: The workflow ID to cancel
    public func cancelWorkflow(_ workflowId: WorkflowID) async throws {
        let state = try await getInternalWorkflowState(workflowId)
        guard [WorkflowStatus.running, WorkflowStatus.queued].contains(state.status) else {
            logger.debug(
                "Workflow cancellation skipped - already in terminal state",
                metadata: [
                    "workflowId": .string(workflowId.value),
                    "currentStatus": .string(state.status.rawValue),
                ]
            )
            return  // Already completed or failed
        }

        // Push a cancellation job to properly handle running activities
        let cancellationJob = WorkflowCoordinatorJob(
            workflowId: workflowId,
            workflowType: state.workflowName,
            action: .cancel,
            stepIndex: state.currentStep
        )

        _ = try await jobQueue.push(cancellationJob)

        logger.debug(
            "Workflow cancellation requested",
            metadata: ["workflowId": .string(workflowId.value)]
        )
    }

    // MARK: - Private Methods

    /// Get internal workflow state directly
    internal func getInternalWorkflowState(_ workflowId: WorkflowID) async throws -> WorkflowExecutionSummary {
        guard let summary = try await jobQueue.queue.workflowRepository.getExecutionSummary(workflowId) else {
            throw WorkflowError.executionNotFound(workflowId)
        }
        return summary
    }

    private func registerWorkflowCoordinator() {
        jobQueue.registerJob(
            parameters: WorkflowCoordinatorJob.self,
            retryStrategy: StepRetryPolicy.default.strategy
        ) { params, context in
            try await self.handleWorkflowCoordination(params, context: context)
        }
    }

    /// Schedule a workflow to run at specified intervals
    /// Schedule a workflow to run according to the specified schedule
    /// This method accumulates schedules in the internal JobSchedule
    ///
    /// - Parameters:
    ///   - workflowType: The workflow type to schedule
    ///   - input: The input data for the workflow
    ///   - schedule: When to run the workflow
    ///   - scheduleId: Optional unique identifier for this schedule
    ///   - options: Workflow execution options
    public mutating func scheduleWorkflow<W: WorkflowProtocol>(
        _ workflowType: W.Type,
        input: W.Input,
        schedule: Schedule,
        scheduleId: String? = nil,
        options: WorkflowOptions = .init()
    ) throws {
        let scheduledJob = try ScheduledWorkflowExecutionJob(
            workflowType: workflowType,
            input: input,
            scheduleId: scheduleId,
            options: options
        )

        jobSchedule.addJob(scheduledJob, schedule: schedule)
    }

    /// Create a scheduler service for running scheduled workflows
    /// Uses the internal accumulated job schedule
    /// - Returns: A scheduler service that can be added to ServiceGroup
    public func createSchedulerService() async -> JobSchedule.Scheduler<Queue> {
        await jobSchedule.scheduler(on: jobQueue)
    }

    private func registerScheduledWorkflowJob() {
        // Register handler for scheduled workflow jobs
        jobQueue.registerJob(
            parameters: ScheduledWorkflowExecutionJob.self,
            retryStrategy: StepRetryPolicy.default.strategy
        ) { params, context in
            try await self.handleScheduledWorkflowExecution(params, context: context)
        }
    }

    private func registerActivityExecutor() {
        // Register activity executor with context-aware retry strategy
        jobQueue.registerJob(
            parameters: ActivityExecutionJob.self,
            retryStrategy: ContextAwareActivityRetryStrategy()
        ) { params, context in
            try await self.handleActivityExecution(params, context: context)
        }
    }

    private func handleWorkflowCoordination(
        _ job: WorkflowCoordinatorJob,
        context: JobExecutionContext
    ) async throws {
        let workflowLockKey = "workflow_lock:\(job.workflowId.value)"
        let jobIdBuffer = try workflowRegistry.encode(context.jobID)

        context.logger.debug(
            "Processing workflow coordination",
            metadata: [
                "workflowId": .string(job.workflowId.value),
                "action": .string(job.action.rawValue),
            ]
        )

        // Cancellation operations bypass the workflow lock to ensure they can always execute
        if job.action == .cancel {
            // Bypass workflow lock for cancellation operations
        } else {
            context.logger.debug("Attempting to acquire workflow lock")

            // Try to acquire workflow execution lock
            let lockAcquired = try await jobQueue.queue.acquireLock(
                key: workflowLockKey,
                id: jobIdBuffer,
                expiresIn: 300  // 5 minutes
            )

            if !lockAcquired {
                context.logger.debug(
                    "Workflow already locked by another job, skipping",
                    metadata: [
                        "workflowId": .string(job.workflowId.value),
                        "currentJobId": .string(context.jobID),
                    ]
                )
                return
            }
        }

        defer {
            // Release workflow lock when done (only if we acquired it)
            if job.action != .cancel {
                Task {
                    try? await jobQueue.queue.releaseLock(key: workflowLockKey, id: jobIdBuffer)
                }
            }
        }

        switch job.action {
        case .start:
            try await startWorkflowExecution(job, context: context)

        case .continueFromActivity:
            try await continueWorkflowExecution(job, context: context)

        case .complete:
            try await completeWorkflow(job, context: context)

        case .fail:
            try await failWorkflow(job, context: context)

        case .cancel:
            try await cancelWorkflowExecution(job, context: context)
        }
    }

    private func startWorkflowExecution(
        _ job: WorkflowCoordinatorJob,
        context: JobExecutionContext
    ) async throws {
        context.logger.debug(
            "🎬 Starting workflow execution",
            metadata: [
                "workflowId": .string(job.workflowId.value),
                "workflowType": .string(job.workflowType),
                "action": .string(job.action.rawValue),
                "stepIndex": .stringConvertible(job.stepIndex),
                "jobId": .stringConvertible(context.jobID),
            ]
        )

        do {
            // Mark workflow as running
            try await jobQueue.queue.workflowRepository.updateExecutionStatus(
                job.workflowId,
                status: .running,
                startTime: .now,
                endTime: nil,
                error: nil
            )

            // Retrieve actual workflow execution to get input data
            guard let execution = try await jobQueue.queue.workflowRepository.getExecution(job.workflowId) else {
                context.logger.error(
                    "❌ No workflow execution found",
                    metadata: [
                        "workflowId": .string(job.workflowId.value),
                        "workflowType": .string(job.workflowType),
                    ]
                )
                throw WorkflowError.executionNotFound(job.workflowId)
            }
            let inputBuffer = execution.inputData

            context.logger.debug(
                "✅ Found input data for workflow",
                metadata: [
                    "workflowId": .string(job.workflowId.value),
                    "inputSize": .stringConvertible(inputBuffer.readableBytes),
                ]
            )

            // Use scheduledAt from execution record if it was a scheduled workflow
            let scheduledAt: Date? = execution.scheduledAt ?? context.nextScheduledAt

            // Create workflow execution context
            let workflowContext = WorkflowExecutionContext(
                workflowId: job.workflowId,
                workflowType: job.workflowType,
                jobQueue: jobQueue,
                logger: context.logger,
                allocator: ByteBufferAllocator(),
                workflowRegistry: workflowRegistry,
                queuedAt: execution.queuedAt,
                scheduledAt: scheduledAt,
                currentStep: job.stepIndex,
                startTime: .now,
                sleepEngine: self
            )

            // Execute workflow using the registry
            let _ = try await workflowRegistry.executeWorkflow(
                workflowName: job.workflowType,
                input: inputBuffer,
                context: workflowContext
            )

            // Output is already handled by the registry's executeWorkflow method
            logger.debug("Workflow execution completed successfully")

            // Calculate totalSteps based on highest step index reached
            let totalSteps = try await calculateTotalSteps(job.workflowId)

            // Mark workflow as completed - output was already stored by the executor
            try await jobQueue.queue.workflowRepository.updateExecutionStatus(
                job.workflowId,
                status: .completed,
                startTime: nil,
                endTime: .now,
                error: nil
            )

            // Update execution with final step counts
            try await updateWorkflowStepCounts(job.workflowId, totalSteps: totalSteps)

            // Create a completion job to trigger child workflow callbacks
            let completionJob = WorkflowCoordinatorJob(
                workflowId: job.workflowId,
                workflowType: job.workflowType,
                action: .complete,
                stepIndex: job.stepIndex
            )
            try await jobQueue.push(completionJob)
            context.logger.info("📝 Pushed completion job for workflow callbacks", metadata: ["workflowId": .string(job.workflowId.value)])

            // Cleanup workflow queries
            cleanupWorkflowQueries(job.workflowId)

            context.logger.info(
                "Workflow execution completed",
                metadata: [
                    "workflowId": .string(job.workflowId.value),
                    "workflowType": .string(job.workflowType),
                ]
            )

        } catch {
            // Mark workflow as failed
            try await jobQueue.queue.workflowRepository.updateExecutionStatus(
                job.workflowId,
                status: .failed,
                startTime: nil,
                endTime: .now,
                error: error.localizedDescription
            )

            // Create a failure completion job to trigger child workflow callbacks
            let failureJob = WorkflowCoordinatorJob(
                workflowId: job.workflowId,
                workflowType: job.workflowType,
                action: .fail,
                stepIndex: job.stepIndex
            )
            try await jobQueue.push(failureJob)
            context.logger.info("📝 Pushed failure job for workflow callbacks", metadata: ["workflowId": .string(job.workflowId.value)])

            // Cleanup workflow queries
            cleanupWorkflowQueries(job.workflowId)

            context.logger.error(
                "Workflow execution failed",
                metadata: [
                    "workflowId": .string(job.workflowId.value),
                    "workflowType": .string(job.workflowType),
                    "error": .string(error.localizedDescription),
                ]
            )

            throw error
        }
    }

    private func continueWorkflowExecution(
        _ job: WorkflowCoordinatorJob,
        context: JobExecutionContext
    ) async throws {
        context.logger.info(
            "Continuing workflow execution",
            metadata: [
                "workflowId": .string(job.workflowId.value),
                "stepIndex": .stringConvertible(job.stepIndex),
            ]
        )

        // Update current step in execution
        try await updateCurrentStep(job.workflowId, currentStep: job.stepIndex)

        do {
            // Get current workflow execution from repository
            guard let execution = try await jobQueue.queue.workflowRepository.getExecution(job.workflowId) else {
                throw WorkflowError.executionNotFound(job.workflowId)
            }

            // Only continue if workflow is still running
            guard execution.status == .running else {
                context.logger.debug(
                    "Workflow continuation skipped - workflow not running",
                    metadata: [
                        "workflowId": .string(job.workflowId.value),
                        "status": .string(execution.status.rawValue),
                    ]
                )
                return
            }

            // Retrieve actual workflow execution to get input data for continuation
            guard let execution = try await jobQueue.queue.workflowRepository.getExecution(job.workflowId) else {
                throw WorkflowError.executionNotFound(job.workflowId)
            }
            let inputBuffer = execution.inputData

            // Create workflow execution context for continuation
            let workflowContext = WorkflowExecutionContext(
                workflowId: job.workflowId,
                workflowType: job.workflowType,
                jobQueue: jobQueue,
                logger: context.logger,
                allocator: ByteBufferAllocator(),
                workflowRegistry: workflowRegistry,
                queuedAt: context.queuedAt,
                scheduledAt: context.nextScheduledAt,
                currentStep: job.stepIndex,
                startTime: .now,
                sleepEngine: self
            )

            // Execute workflow using the registry
            let _ = try await workflowRegistry.executeWorkflow(
                workflowName: job.workflowType,
                input: inputBuffer,
                context: workflowContext
            )

            // Output is already handled by the registry's executeWorkflow method
            logger.debug("Workflow continuation completed successfully")

            // Calculate totalSteps based on highest step index reached
            let totalSteps = try await calculateTotalSteps(job.workflowId)

            // Mark workflow as completed - output was already stored by the executor
            try await jobQueue.queue.workflowRepository.updateExecutionStatus(
                job.workflowId,
                status: .completed,
                startTime: nil,
                endTime: .now,
                error: nil
            )

            // Update execution with final step counts
            try await updateWorkflowStepCounts(job.workflowId, totalSteps: totalSteps)

            // Create a completion job to trigger child workflow callbacks
            let completionJob = WorkflowCoordinatorJob(
                workflowId: job.workflowId,
                workflowType: job.workflowType,
                action: .complete,
                stepIndex: job.stepIndex
            )
            try await jobQueue.push(completionJob)
            context.logger.info("📝 Pushed completion job for workflow callbacks", metadata: ["workflowId": .string(job.workflowId.value)])

        } catch {
            context.logger.error(
                "Workflow continuation failed",
                metadata: [
                    "workflowId": .string(job.workflowId.value),
                    "stepIndex": .stringConvertible(job.stepIndex),
                    "error": .string(error.localizedDescription),
                ]
            )
            throw error
        }
    }

    private func completeWorkflow(
        _ job: WorkflowCoordinatorJob,
        context: JobExecutionContext
    ) async throws {
        // Update workflow status to completed using repository
        try await jobQueue.queue.workflowRepository.updateExecutionStatus(
            job.workflowId,
            status: .completed,
            startTime: nil,
            endTime: .now,
            error: nil
        )

        context.logger.debug(
            "Workflow marked as completed",
            metadata: ["workflowId": .string(job.workflowId.value)]
        )
    }

    private func failWorkflow(
        _ job: WorkflowCoordinatorJob,
        context: JobExecutionContext
    ) async throws {
        // Update workflow status to failed using repository
        try await jobQueue.queue.workflowRepository.updateExecutionStatus(
            job.workflowId,
            status: .failed,
            startTime: nil,
            endTime: .now,
            error: "Workflow failed"
        )

        context.logger.error(
            "Workflow marked as failed",
            metadata: ["workflowId": .string(job.workflowId.value)]
        )
    }

    private func cancelWorkflowExecution(
        _ job: WorkflowCoordinatorJob,
        context: JobExecutionContext
    ) async throws {
        context.logger.debug(
            "Processing workflow cancellation",
            metadata: [
                "workflowId": .string(job.workflowId.value)
            ]
        )

        let state = try await getInternalWorkflowState(job.workflowId)

        // Check if workflow has already completed
        if [WorkflowStatus.completed, .failed, .cancelled].contains(state.status) {
            context.logger.debug(
                "Workflow already in terminal state, skipping cancellation",
                metadata: [
                    "workflowId": .string(job.workflowId.value),
                    "currentStatus": .string(state.status.rawValue),
                ]
            )
            return
        }

        // Preserve completed steps, cancel running/pending steps
        // Query all steps for this workflow to determine their current status
        var completedSteps = 0
        var cancelledSteps = 0
        var runningSteps = 0

        let currentStep = state.currentStep

        // Process steps from 0 to currentStep (inclusive, as currentStep might be running)
        for stepIndex in 0...currentStep {
            if let step = try await jobQueue.queue.workflowRepository.getStep(job.workflowId, stepIndex: stepIndex) {
                switch step.status {
                case .succeeded:
                    // Keep completed steps as-is
                    completedSteps += 1
                    context.logger.debug(
                        "Preserving completed step during workflow cancellation",
                        metadata: [
                            "workflowId": .string(job.workflowId.value),
                            "stepIndex": .stringConvertible(stepIndex),
                            "activityId": .string(step.activityId ?? "unknown"),
                        ]
                    )

                case .running:
                    // Cancel currently running steps
                    var cancelledStep = step
                    cancelledStep.status = .cancelled
                    cancelledStep.completedAt = Date.now
                    cancelledStep.error = "Workflow cancelled by user"

                    try await jobQueue.queue.workflowRepository.updateStep(cancelledStep)
                    runningSteps += 1

                    context.logger.debug(
                        "Cancelled running step during workflow cancellation",
                        metadata: [
                            "workflowId": .string(job.workflowId.value),
                            "stepIndex": .stringConvertible(stepIndex),
                            "activityId": .string(step.activityId ?? "unknown"),
                        ]
                    )

                case .queued, .pending, .assigned:
                    // Cancel pending/queued/assigned steps (they never started or are about to start)
                    var cancelledStep = step
                    cancelledStep.status = .cancelled
                    cancelledStep.completedAt = Date.now
                    cancelledStep.error = "Workflow cancelled before execution"

                    try await jobQueue.queue.workflowRepository.updateStep(cancelledStep)
                    cancelledSteps += 1

                case .failed, .cancelled, .timedOut, .cancelling, .backoff, .skipped:
                    // Already in terminal state or being handled, leave as-is
                    break
                }
            }
        }

        context.logger.debug(
            "Workflow step cancellation summary",
            metadata: [
                "workflowId": .string(job.workflowId.value),
                "completedSteps": .stringConvertible(completedSteps),
                "runningStepsCancelled": .stringConvertible(runningSteps),
                "pendingStepsCancelled": .stringConvertible(cancelledSteps),
            ]
        )

        // Already handled above - workflow is in terminal state, cancellation was skipped

        // Cancel pending sleep operations
        // Cancel pending sleep operations
        sleepCompletions.cancelPendingSleeps(workflowId: job.workflowId)

        // Cancel scheduled sleep jobs using CancellableJobQueue interface
        let scheduledJobs = sleepCompletions.getScheduledJobs(workflowId: job.workflowId)
        for jobId in scheduledJobs {
            do {
                try await jobQueue.cancelJob(jobID: jobId)
            } catch {
                context.logger.debug(
                    "Failed to cancel scheduled sleep job",
                    metadata: [
                        "workflowId": .string(job.workflowId.value),
                        "error": .string(String(describing: error)),
                    ]
                )
            }
        }
        sleepCompletions.clearScheduledJobs(workflowId: job.workflowId)

        // Update workflow status to cancelled using repository
        try await jobQueue.queue.workflowRepository.updateExecutionStatus(
            job.workflowId,
            status: .cancelled,
            startTime: nil,
            endTime: .now,
            error: "Workflow cancelled by user"
        )

        // Notify completion registry that workflow is cancelled
        // Notify completion registry that workflow is cancelled
        WorkflowCompletionRegistry.shared.failWorkflow(
            workflowId: job.workflowId,
            error: WorkflowError.workflowCancelled(job.workflowId)
        )

        context.logger.debug(
            "Workflow execution cancelled",
            metadata: ["workflowId": .string(job.workflowId.value)]
        )
    }

    /// Run a workflow and wait for completion
    public func runWorkflow<W: WorkflowProtocol>(
        _ workflowType: W.Type,
        input: W.Input,
        workflowId: WorkflowID? = nil,
        options: WorkflowOptions = .init()
    ) async throws -> W.Output {
        // Start the workflow
        let id = try await startWorkflow(workflowType, input: input, workflowId: workflowId, options: options)

        // Wait for completion using event-driven approach
        let status: WorkflowExecutionSummary
        if let timeout = options.timeout {
            status = try await waitForWorkflowCompletionEventDriven(id, timeout: timeout)
        } else {
            status = try await waitForWorkflowCompletionEventDrivenIndefinitely(id)
        }

        // Check final status
        switch status.status {
        case .completed:
            // Get the full execution to access output
            guard let execution = try await jobQueue.queue.workflowRepository.getExecution(id) else {
                throw WorkflowError.executionNotFound(id)
            }

            guard let resultBuffer = execution.result else {
                throw WorkflowError.noOutput(id)
            }

            // Decode the result using the registry
            return try workflowRegistry.decode(resultBuffer, as: W.Output.self)

        case .failed:
            let errorMessage = status.error ?? "Unknown workflow error"
            throw WorkflowError.workflowFailed(errorMessage)

        case .cancelled:
            // For cancelled workflows, check if they have output (graceful cancellation)
            // Workflows can return meaningful results even when cancelled
            guard let execution = try await jobQueue.queue.workflowRepository.getExecution(id) else {
                throw WorkflowError.executionNotFound(id)
            }

            if let resultBuffer = execution.result {
                // Decode the result using the registry
                let output = try workflowRegistry.decode(resultBuffer, as: W.Output.self)
                // Workflow was cancelled but produced meaningful output (graceful cancellation)
                // This handles cases like OrderProcessingWorkflow.createCancelledOutput()
                // which returns completed steps and cancellation status
                logger.debug(
                    "Returning output from cancelled workflow (graceful cancellation)",
                    metadata: ["workflowId": .string(id.value)]
                )
                return output
            } else {
                // Workflow was cancelled without producing output (forceful cancellation)
                logger.debug(
                    "Cancelled workflow has no output available",
                    metadata: ["workflowId": .string(id.value)]
                )
                throw WorkflowError.workflowCancelled(id)
            }

        default:
            throw WorkflowError.unexpectedStatus(id, status.status)
        }
    }

    /// Wait for workflow completion using event-driven approach with timeout
    private func waitForWorkflowCompletionEventDriven(
        _ workflowId: WorkflowID,
        timeout: Duration
    ) async throws -> WorkflowExecutionSummary {
        // Check if workflow is already completed
        if let status = try await getWorkflowSummary(workflowId) {
            switch status.status {
            case .completed, .failed, .cancelled:
                return status
            case .running, .queued:
                break  // Continue with event-driven waiting
            }
        }

        // Create completion stream for event-driven waiting
        let (stream, continuation) = WorkflowCompletionRegistry.makeStream(workflowId: workflowId.value)

        // Register the continuation in the global registry
        WorkflowCompletionRegistry.shared.register(workflowId: workflowId, continuation: continuation)

        // Create timeout task
        let timeoutTask = Task {
            try await Task.sleep(for: timeout)

            // Try to cancel the actual workflow execution
            do {
                try await self.cancelWorkflow(workflowId)
                // If cancellation succeeds, it will handle the notification
            } catch {
                // If cancellation fails (e.g., workflow already completed),
                // notify about the timeout directly
                WorkflowCompletionRegistry.shared.failWorkflow(
                    workflowId: workflowId,
                    error: WorkflowError.timeout(workflowId, timeout)
                )
            }
        }

        defer {
            timeoutTask.cancel()
            WorkflowCompletionRegistry.shared.cancelWorkflow(workflowId: workflowId)
        }

        // Wait for workflow completion via event stream
        for try await result in stream {
            switch result {
            case .success(_):
                // Get final status for return
                guard let status = try await getWorkflowSummary(workflowId) else {
                    throw WorkflowError.executionNotFound(workflowId)
                }
                return status
            case .failure(let error):
                throw error
            }
        }

        throw WorkflowError.workflowFailed("Workflow completion stream ended unexpectedly")
    }

    /// Wait for workflow completion using event-driven approach without timeout
    private func waitForWorkflowCompletionEventDrivenIndefinitely(
        _ workflowId: WorkflowID
    ) async throws -> WorkflowExecutionSummary {
        // Check if workflow is already completed
        if let status = try await getWorkflowSummary(workflowId) {
            switch status.status {
            case .completed, .failed, .cancelled:
                return status
            case .running, .queued:
                break  // Continue with event-driven waiting
            }
        }

        // Create completion stream for event-driven waiting
        let (stream, continuation) = WorkflowCompletionRegistry.makeStream(workflowId: workflowId.value)

        // Register the continuation in the global registry
        WorkflowCompletionRegistry.shared.register(workflowId: workflowId, continuation: continuation)

        defer {
            WorkflowCompletionRegistry.shared.cancelWorkflow(workflowId: workflowId)
        }

        // Wait for workflow completion via event stream
        for try await result in stream {
            switch result {
            case .success(_):
                // Get final status for return
                guard let status = try await getWorkflowSummary(workflowId) else {
                    throw WorkflowError.executionNotFound(workflowId)
                }
                return status
            case .failure(let error):
                throw error
            }
        }

        throw WorkflowError.workflowFailed("Workflow completion stream ended unexpectedly")
    }

    /// Calculate total steps for a workflow by finding the highest step index
    private func calculateTotalSteps(_ workflowId: WorkflowID) async throws -> Int {
        // Get all step indices for this workflow from the repository
        var maxStepIndex = 0
        var stepIndex = 0

        // Iterate through steps to find the highest index
        while true {
            if let _ = try await jobQueue.queue.workflowRepository.getStep(workflowId, stepIndex: stepIndex) {
                maxStepIndex = stepIndex
                stepIndex += 1
            } else {
                break
            }
        }

        // Total steps is maxStepIndex + 1 (since we start from 0)
        return maxStepIndex + 1
    }

    /// Update workflow execution with correct step counts
    private func updateWorkflowStepCounts(_ workflowId: WorkflowID, totalSteps: Int) async throws {
        // Get current execution
        guard var execution = try await jobQueue.queue.workflowRepository.getExecution(workflowId) else {
            return
        }

        // Update totalSteps
        execution.totalSteps = totalSteps

        // Save updated execution
        try await jobQueue.queue.workflowRepository.updateExecution(execution)
    }

    /// Update current step in workflow execution
    private func updateCurrentStep(_ workflowId: WorkflowID, currentStep: Int) async throws {
        // Get current execution
        guard var execution = try await jobQueue.queue.workflowRepository.getExecution(workflowId) else {
            return
        }

        // Update currentStep
        execution.currentStep = currentStep

        // Save updated execution
        try await jobQueue.queue.workflowRepository.updateExecution(execution)
    }

    private func handleActivityExecution(
        _ job: ActivityExecutionJob,
        context: JobExecutionContext
    ) async throws {
        // Set task-local context for retry strategy access
        try await ActivityExecutionJob.$current.withValue(job) {
            context.logger.debug(
                "⚡ Executing activity",
                metadata: [
                    "activityName": .string(job.activityName),
                    "activityId": .string(job.activityId.value),
                    "workflowId": .string(job.workflowId.value),
                ]
            )

            do {
                let activityContext = ActivityExecutionContext(
                    activityJob: job,
                    jobContext: context,
                    jobId: context.jobID
                )

                let resultBuffer = try await internalRegistry.executeActivity(
                    name: job.activityName,
                    inputBuffer: job.inputBuffer,
                    context: activityContext
                )

                // Store activity result as ByteBuffer directly to avoid double encoding
                let activityResult = ActivityResult<ByteBuffer>.success(resultBuffer)
                try await jobQueue.queue.workflowRepository.saveResult(job.activityId, result: activityResult)

                context.logger.debug(
                    "✅ Activity completed successfully",
                    metadata: [
                        "activityId": .string(job.activityId.value),
                        "activityName": .string(job.activityName),
                    ]
                )

                // Activity completion is handled by ActivityCallbackMiddleware
                // which notifies the workflow via waitForActivity() mechanism

            } catch {
                // Store activity failure using repository with proper error information
                let errorInfo = ActivityErrorInfo(from: error)
                let failureResult = ActivityResult<ByteBuffer>.failure(errorInfo)
                try await jobQueue.queue.workflowRepository.saveResult(job.activityId, result: failureResult)

                context.logger.error(
                    "Activity execution failed",
                    metadata: [
                        "activityId": .string(job.activityId.value),
                        "activityName": .string(job.activityName),
                        "attempt": .stringConvertible(context.attempt),
                        "error": .string(error.localizedDescription),
                    ]
                )

                // Re-throw to let job queue handle retry logic
                throw error
            }
        }  // Close task-local context
    }

    /// Register DelayJob for workflow sleep functionality
    private func registerDelayJob() {
        // Register minimal WorkflowDelayJob handler - completion is handled by WorkflowSleepMiddleware
        jobQueue.registerJob(parameters: WorkflowDelayJob.self) { job, context in
            // Job completes immediately when executed (after the delayUntil time)
            // WorkflowSleepMiddleware handles the completion notification via onCompletedJob
            context.logger.debug(
                "WorkflowDelayJob executed - completion handled by middleware",
                metadata: [
                    "jobID": .string(context.jobID),
                    "sleepId": .string(job.sleepId),
                    "workflowId": .string(job.workflowId.value),
                    "sleepKey": .string(job.sleepKey),
                ]
            )
        }
    }

    /// Track a scheduled sleep job for later cancellation
    internal func trackSleepJob(workflowId: WorkflowID, jobId: any Sendable & CustomStringConvertible) async {
        if let queueJobId = jobId as? Queue.JobID {
            sleepCompletions.trackScheduledJob(workflowId: workflowId, jobId: queueJobId)
        }
    }

    /// Handle scheduled workflow execution
    private func handleScheduledWorkflowExecution(
        _ job: ScheduledWorkflowExecutionJob,
        context: JobExecutionContext
    ) async throws {
        // Generate workflow ID - use schedule ID directly for searchability
        let workflowId: WorkflowID

        if let scheduleId = job.scheduleId {
            // Use schedule ID as both workflow and run ID for predictable searching
            workflowId = WorkflowID(workflowId: scheduleId, runId: scheduleId)
        } else {
            // Generate unique ID for workflows without schedule ID
            let timestamp = Int64(Date().timeIntervalSince1970 * 1000)  // milliseconds
            workflowId = WorkflowID(workflowId: "\(job.workflowTypeName)_scheduled_\(timestamp)")
        }

        context.logger.debug(
            "Starting scheduled workflow via proper coordination flow",
            metadata: [
                "workflowType": .string(job.workflowTypeName),
                "scheduledWorkflowId": .string(workflowId.value),
                "scheduledAt": .stringConvertible(context.nextScheduledAt ?? context.queuedAt),
            ]
        )

        // Create workflow execution using repository (like startWorkflow does)
        let execution = WorkflowExecution(
            id: workflowId,
            workflowName: job.workflowTypeName,
            inputData: job.inputBuffer,
            workflowState: nil,
            status: .queued,
            createdAt: .now,
            queuedAt: context.queuedAt,
            scheduledAt: context.nextScheduledAt,
            currentStep: 0,
            totalSteps: 0,
            metadata: [:]
        )

        try await jobQueue.queue.workflowRepository.createExecution(execution)

        // Use the proper workflow coordination flow
        let coordinatorJob = WorkflowCoordinatorJob(
            workflowId: workflowId,
            workflowType: job.workflowTypeName,
            action: .start,
            stepIndex: 0
        )

        _ = try await jobQueue.push(coordinatorJob)

        context.logger.debug(
            "Scheduled workflow queued for coordination",
            metadata: [
                "workflowType": .string(job.workflowTypeName),
                "workflowId": .string(workflowId.value),
            ]
        )
    }
}

/// Shared storage for sleep completions
private final class SleepCompletionStorage<JobID: Sendable & CustomStringConvertible>: Sendable {
    private let completions: Mutex<[String: CheckedContinuation<Void, Error>]> = .init([:])
    private let scheduledJobs: Mutex<[WorkflowID: [JobID]]> = .init([:])

    func waitForSleep(sleepKey: String) async throws {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            completions.withLock { sleeps in
                sleeps[sleepKey] = continuation
            }
        }
    }

    func completeSleep(sleepKey: String, result: Result<Void, any Error>) {
        completions.withLock { sleeps in
            if let continuation = sleeps.removeValue(forKey: sleepKey) {
                switch result {
                case .success:
                    continuation.resume()
                case .failure(let error):
                    continuation.resume(throwing: error)
                }
            }
        }
    }

    func trackScheduledJob(workflowId: WorkflowID, jobId: JobID) {
        scheduledJobs.withLock { jobs in
            jobs[workflowId, default: []].append(jobId)
        }
    }

    func cancelPendingSleeps(workflowId: WorkflowID) {
        let workflowPrefix = "workflow_sleep:\(workflowId.value):"
        completions.withLock { sleeps in
            let keysToCancel = sleeps.keys.filter { $0.hasPrefix(workflowPrefix) }
            for key in keysToCancel {
                if let continuation = sleeps.removeValue(forKey: key) {
                    continuation.resume(throwing: WorkflowError.workflowCancelled(workflowId))
                }
            }
        }
    }

    func getScheduledJobs(workflowId: WorkflowID) -> [JobID] {
        scheduledJobs.withLock { jobs in
            jobs[workflowId] ?? []
        }
    }

    func clearScheduledJobs(workflowId: WorkflowID) {
        scheduledJobs.withLock { jobs in
            _ = jobs.removeValue(forKey: workflowId)
        }
    }
}

/// Simple middleware to handle WorkflowDelayJob completions with callback
private final class SleepCallbackMiddleware<JobID: Sendable & CustomStringConvertible>: JobMiddleware {
    let storage: SleepCompletionStorage<JobID>

    init(storage: SleepCompletionStorage<JobID>) {
        self.storage = storage
    }

    func onCompletedJob(job: any JobInstanceProtocol, result: Result<Void, any Error>, context: JobCompletedQueueContext) async {
        // Handle WorkflowDelayJob completions
        guard job.parameters is WorkflowDelayJob else { return }
        let delayJob = job.parameters as! WorkflowDelayJob
        storage.completeSleep(sleepKey: delayJob.sleepKey, result: result)
    }
}

/// Shared storage for activity completions
private final class ActivityCompletionStorage: Sendable {
    private let completions: Mutex<[String: CheckedContinuation<Void, Error>]> = .init([:])

    func waitForActivity(activityId: String) async throws {
        // Wait for middleware notification that activity completed
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            completions.withLock { activities in
                activities[activityId] = continuation
            }
        }
    }

    func completeActivity(activityId: String) {
        completions.withLock { activities in
            if let continuation = activities.removeValue(forKey: activityId) {
                continuation.resume()
            }
        }
    }

    func failActivity(activityId: String, error: Error) {
        completions.withLock { activities in
            if let continuation = activities.removeValue(forKey: activityId) {
                continuation.resume(throwing: error)
            }
        }
    }
}

/// Simple middleware to handle ActivityExecution completions with callback
private final class ActivityCallbackMiddleware: JobMiddleware {
    let storage: ActivityCompletionStorage

    init(storage: ActivityCompletionStorage) {
        self.storage = storage
    }

    func onCompletedJob(job: any JobInstanceProtocol, result: Result<Void, any Error>, context: JobCompletedQueueContext) async {
        // Handle ActivityExecution completions
        guard job.parameters is ActivityExecutionJob else { return }
        let activityJob = job.parameters as! ActivityExecutionJob

        switch result {
        case .success:
            // Activity completed successfully - retrieve the actual result from storage
            storage.completeActivity(activityId: activityJob.activityId.value)
        case .failure(let error):
            storage.failActivity(activityId: activityJob.activityId.value, error: error)
        }
    }
}

/// Unified middleware to handle both child and top-level workflow completion events
///
/// Detects when WorkflowCoordinator jobs complete and:
/// - For child workflows: notifies parent workflows via ChildWorkflowRegistry
/// - For top-level workflows: notifies runWorkflow calls via WorkflowCompletionRegistry
private final class WorkflowCompletionMiddleware: JobMiddleware {

    private let repository: any WorkflowRepository

    init(repository: any WorkflowRepository) {
        self.repository = repository
    }

    func onCompletedJob(job: any JobInstanceProtocol, result: Result<Void, any Error>, context: JobCompletedQueueContext) async {
        // Handle WorkflowCoordinator completions
        guard let coordinatorJob = job.parameters as? WorkflowCoordinatorJob else {
            return
        }

        // Only handle completion actions (not start actions)
        guard coordinatorJob.action == .complete || coordinatorJob.action == .fail else {
            return
        }

        // Get workflow execution summary to determine if it's a child or top-level workflow
        guard let summary = try? await repository.getExecutionSummary(coordinatorJob.workflowId) else {
            return
        }

        if summary.parentId != nil {
            // This is a child workflow - notify parent via ChildWorkflowRegistry
            await handleChildWorkflowCompletion(coordinatorJob: coordinatorJob, result: result)
        } else {
            // This is a top-level workflow - notify runWorkflow calls via WorkflowCompletionRegistry
            await handleTopLevelWorkflowCompletion(coordinatorJob: coordinatorJob, result: result)
        }
    }

    private func handleChildWorkflowCompletion(coordinatorJob: WorkflowCoordinatorJob, result: Result<Void, any Error>) async {
        switch result {
        case .success:
            // Child workflow completed successfully
            await handleChildWorkflowSuccess(coordinatorJob: coordinatorJob)
        case .failure(let error):
            // Child workflow failed
            ChildWorkflowRegistry.shared.failWorkflow(childWorkflowId: coordinatorJob.workflowId, error: error)
        }
    }

    private func handleTopLevelWorkflowCompletion(coordinatorJob: WorkflowCoordinatorJob, result: Result<Void, any Error>) async {
        switch result {
        case .success:
            // Top-level workflow completed successfully
            await handleTopLevelWorkflowSuccess(coordinatorJob: coordinatorJob)
        case .failure(let error):
            // Top-level workflow failed
            WorkflowCompletionRegistry.shared.failWorkflow(workflowId: coordinatorJob.workflowId, error: error)
        }
    }

    private func handleChildWorkflowSuccess(coordinatorJob: WorkflowCoordinatorJob) async {
        do {
            // Retrieve the workflow output from the repository
            if let outputBuffer = try await getWorkflowOutput(coordinatorJob.workflowId) {
                ChildWorkflowRegistry.shared.completeWorkflow(
                    childWorkflowId: coordinatorJob.workflowId,
                    result: outputBuffer
                )
            } else {
                // No output found - complete with empty result
                let emptyResult = ByteBufferAllocator().buffer(capacity: 0)
                ChildWorkflowRegistry.shared.completeWorkflow(
                    childWorkflowId: coordinatorJob.workflowId,
                    result: emptyResult
                )
            }
        } catch {
            // Failed to retrieve output - treat as failure
            ChildWorkflowRegistry.shared.failWorkflow(
                childWorkflowId: coordinatorJob.workflowId,
                error: error
            )
        }
    }

    private func handleTopLevelWorkflowSuccess(coordinatorJob: WorkflowCoordinatorJob) async {
        do {
            // Retrieve the workflow output from the repository
            if let outputBuffer = try await getWorkflowOutput(coordinatorJob.workflowId) {
                WorkflowCompletionRegistry.shared.completeWorkflow(
                    workflowId: coordinatorJob.workflowId,
                    result: outputBuffer
                )
            } else {
                // No output found - complete with empty result
                let emptyResult = ByteBufferAllocator().buffer(capacity: 0)
                WorkflowCompletionRegistry.shared.completeWorkflow(
                    workflowId: coordinatorJob.workflowId,
                    result: emptyResult
                )
            }
        } catch {
            // Failed to retrieve output - treat as failure
            WorkflowCompletionRegistry.shared.failWorkflow(
                workflowId: coordinatorJob.workflowId,
                error: error
            )
        }
    }

    private func getWorkflowOutput(_ workflowId: WorkflowID) async throws -> ByteBuffer? {
        // Get execution summary to check status
        guard let summary = try await repository.getExecutionSummary(workflowId) else {
            return nil
        }

        // Only return output if workflow completed successfully
        guard summary.status == .completed else {
            return nil
        }

        // Get the full execution to access its result field
        guard let execution = try await repository.getExecution(workflowId) else {
            return nil
        }

        return execution.result
    }
}

/// Extension to make the engine discoverable
extension WorkflowEngine: CustomStringConvertible {
    public var description: String { "WorkflowEngine<\(String(describing: Queue.self))>" }
}

/// Context-aware retry strategy that accesses job parameters through task-local storage
private struct ContextAwareActivityRetryStrategy: JobRetryStrategy {
    func shouldRetry(attempt: Int, error: Error) -> Bool {
        // Try to access current ActivityExecutionJob through task-local context
        if let currentJob = ActivityExecutionJob.current {
            let retryPolicy = currentJob.retryPolicy ?? StepRetryPolicy.default
            let strategy = retryPolicy.strategy
            return strategy.shouldRetry(attempt: attempt, error: error)
        }

        // Fallback to default workflow-aware strategy
        let defaultStrategy = StepRetryPolicy.default.strategy
        return defaultStrategy.shouldRetry(attempt: attempt, error: error)
    }

    func calculateBackoff(attempt: Int) -> Duration {
        // Try to access current ActivityExecutionJob through task-local context
        if let currentJob = ActivityExecutionJob.current {
            let retryPolicy = currentJob.retryPolicy ?? StepRetryPolicy.default
            let strategy = retryPolicy.strategy
            return strategy.calculateBackoff(attempt: attempt)
        }

        // Fallback to default workflow-aware strategy
        let defaultStrategy = StepRetryPolicy.default.strategy
        return defaultStrategy.calculateBackoff(attempt: attempt)
    }
}

/// Extension to support task-local access to current ActivityExecutionJob
extension ActivityExecutionJob {
    @TaskLocal
    static var current: ActivityExecutionJob?
}

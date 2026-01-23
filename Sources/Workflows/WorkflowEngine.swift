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

    /// Signal a running workflow
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

    /// Register the workflow coordinator job handler
    private func registerWorkflowCoordinator() {
        jobQueue.registerJob(
            parameters: WorkflowCoordinatorJob.self,
            retryStrategy: StepRetryPolicy.default.strategy
        ) { params, context in
            try await self.handleWorkflowCoordination(params, context: context)
        }
    }

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

    /// Register the scheduled workflow execution job handler
    private func registerScheduledWorkflowJob() {
        jobQueue.registerJob(
            parameters: ScheduledWorkflowExecutionJob.self,
            retryStrategy: StepRetryPolicy.default.strategy
        ) { params, context in
            try await self.handleScheduledWorkflowExecution(params, context: context)
        }
    }

    /// Register the activity execution job handler with per-job retry strategies
    private func registerActivityExecutor() {
        // Create base job definition for ActivityExecutionJob
        let baseJobDefinition = JobDefinition<ActivityExecutionJob>(
            parameters: ActivityExecutionJob.self,
            retryStrategy: .dontRetry,  // Placeholder - will be replaced by job-specific strategies
            timeout: nil
        ) { params, context in
            try await self.handleActivityExecution(params, context: context)
        }

        // Register custom builder that creates job instances with job-specific retry strategies
        let customBuilder: @Sendable (Decoder) throws -> any JobInstanceProtocol = { decoder in
            let data = try JobInstanceData<ActivityExecutionJob>(from: decoder)
            let activityJob = data.parameters

            // Create a job definition with the activity's specific retry strategy
            let jobSpecificDefinition = JobDefinition<ActivityExecutionJob>(
                parameters: ActivityExecutionJob.self,
                retryStrategy: activityJob.retryPolicy.strategy,
                timeout: baseJobDefinition.timeout,
                execute: baseJobDefinition.execute
            )

            return try JobInstance<ActivityExecutionJob>(job: jobSpecificDefinition, data: data)
        }

        // Register the custom builder using the workflow queue driver
        jobQueue.queue.registerCustomJobBuilder(
            name: baseJobDefinition.name,
            builder: customBuilder
        )
    }

    private func handleWorkflowCoordination(
        _ job: WorkflowCoordinatorJob,
        context: JobExecutionContext
    ) async throws {
        context.logger.debug(
            "Processing workflow coordination",
            metadata: [
                "workflowId": .string(job.workflowId.value),
                "action": .string(job.action.rawValue),
            ]
        )

        // Execute the workflow action with appropriate locking
        try await executeWorkflowActionWithLock(job, context: context)
    }

    /// Execute workflow action with appropriate locking strategy
    private func executeWorkflowActionWithLock(
        _ job: WorkflowCoordinatorJob,
        context: JobExecutionContext
    ) async throws {
        // Cancellation operations bypass the workflow lock to ensure they can always execute
        if job.action == .cancel {
            try await executeWorkflowAction(job, context: context)
            return
        }

        // All other operations require workflow lock
        let workflowLockKey = "workflow_lock:\(job.workflowId.value)"
        let jobIdBuffer = try workflowRegistry.encode(context.jobID)

        context.logger.debug("Attempting to acquire workflow lock")

        let lockAcquired = try await jobQueue.queue.acquireLock(
            key: workflowLockKey,
            id: jobIdBuffer,
            expiresIn: 300  // 5 minutes
        )

        guard lockAcquired else {
            context.logger.debug(
                "Workflow already locked by another job, skipping",
                metadata: [
                    "workflowId": .string(job.workflowId.value),
                    "currentJobId": .string(context.jobID),
                ]
            )
            return
        }

        defer {
            /// We should not need a Task here for Swift 6.3
            Task {
                try? await jobQueue.queue.releaseLock(key: workflowLockKey, id: jobIdBuffer)
            }
        }

        try await executeWorkflowAction(job, context: context)
    }

    /// Execute the appropriate workflow action based on the job type
    private func executeWorkflowAction(
        _ job: WorkflowCoordinatorJob,
        context: JobExecutionContext
    ) async throws {
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
            context.logger.debug("📝 Pushed completion job for workflow callbacks", metadata: ["workflowId": .string(job.workflowId.value)])

            // Cleanup workflow queries
            cleanupWorkflowQueries(job.workflowId)

            context.logger.debug(
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
            context.logger.debug("📝 Pushed failure job for workflow callbacks", metadata: ["workflowId": .string(job.workflowId.value)])

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
        context.logger.debug(
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
            context.logger.debug("📝 Pushed completion job for workflow callbacks", metadata: ["workflowId": .string(job.workflowId.value)])

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
        let status = try await waitForWorkflowCompletionEventDriven(id, timeout: options.timeout)

        // Process the final result
        return try await processWorkflowResult(id, status: status, outputType: W.Output.self)
    }

    /// Process workflow execution result and extract typed output
    private func processWorkflowResult<Output: Codable & Sendable>(
        _ workflowId: WorkflowID,
        status: WorkflowExecutionSummary,
        outputType: Output.Type
    ) async throws -> Output {
        switch status.status {
        case .completed:
            return try await extractWorkflowOutput(workflowId, outputType: outputType)

        case .failed:
            let errorMessage = status.error ?? "Unknown workflow error"
            throw WorkflowError.workflowFailed(errorMessage)

        case .cancelled:
            return try await handleCancelledWorkflowOutput(workflowId, outputType: outputType)

        default:
            throw WorkflowError.unexpectedStatus(workflowId, status.status)
        }
    }

    /// Extract output from a completed workflow execution
    private func extractWorkflowOutput<Output: Codable & Sendable>(
        _ workflowId: WorkflowID,
        outputType: Output.Type
    ) async throws -> Output {
        guard let execution = try await jobQueue.queue.workflowRepository.getExecution(workflowId) else {
            throw WorkflowError.executionNotFound(workflowId)
        }

        guard let resultBuffer = execution.result else {
            throw WorkflowError.noOutput(workflowId)
        }

        return try workflowRegistry.decode(resultBuffer, as: outputType)
    }

    /// Handle output from a cancelled workflow (graceful vs forceful cancellation)
    private func handleCancelledWorkflowOutput<Output: Codable & Sendable>(
        _ workflowId: WorkflowID,
        outputType: Output.Type
    ) async throws -> Output {
        guard let execution = try await jobQueue.queue.workflowRepository.getExecution(workflowId) else {
            throw WorkflowError.executionNotFound(workflowId)
        }

        if let resultBuffer = execution.result {
            // Graceful cancellation - workflow produced meaningful output
            let output = try workflowRegistry.decode(resultBuffer, as: outputType)
            logger.debug(
                "Returning output from cancelled workflow (graceful cancellation)",
                metadata: ["workflowId": .string(workflowId.value)]
            )
            return output
        } else {
            // Forceful cancellation - no output available
            logger.debug(
                "Cancelled workflow has no output available",
                metadata: ["workflowId": .string(workflowId.value)]
            )
            throw WorkflowError.workflowCancelled(workflowId)
        }
    }

    /// Wait for workflow completion using event-driven approach with optional timeout
    private func waitForWorkflowCompletionEventDriven(
        _ workflowId: WorkflowID,
        timeout: Duration? = nil
    ) async throws -> WorkflowExecutionSummary {
        // Check if workflow is already completed
        if let status = try await getWorkflowSummary(workflowId) {
            switch status.status {
            case .completed, .failed, .cancelled:
                return status
            case .running, .queued, .sleeping:
                break  // Continue with event-driven waiting
            }
        }

        // Create completion stream for event-driven waiting
        let (stream, continuation) = WorkflowCompletionRegistry.makeStream(workflowId: workflowId.value)

        // Register the continuation in the global registry
        WorkflowCompletionRegistry.shared.register(workflowId: workflowId, continuation: continuation)

        // Create timeout task if timeout is specified
        let timeoutTask: Task<Void, Never>? = timeout.map { timeout in
            Task {
                do {
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
                } catch {
                    // Task was cancelled or sleep failed - ignore
                }
            }
        }

        defer {
            timeoutTask?.cancel()
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
    }

    /// Register the workflow delay job handler
    /// Completion is handled by WorkflowSleepMiddleware
    private func registerDelayJob() {
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

/// Extension to make the engine discoverable
extension WorkflowEngine: CustomStringConvertible {
    public var description: String { "WorkflowEngine<\(String(describing: Queue.self))>" }
}

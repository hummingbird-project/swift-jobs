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
import Logging
import NIOCore
import ServiceLifecycle
import Synchronization

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Registry for workflow types
public final class WorkflowRegistry: Sendable {
    private let workflowTypes: Mutex<[String: any WorkflowFactory]> = .init([:])

    public init() {}

    /// Register a workflow type
    /// - Parameter workflowType: The workflow type to register
    public func registerWorkflow<W: WorkflowProtocol>(_ workflowType: W.Type) {
        let factory = ConcreteWorkflowFactory<W>()
        workflowTypes.withLock { types in
            precondition(types[W.workflowName] == nil, "There is a workflow already registered under name \"\(W.workflowName)\"")
            types[W.workflowName] = factory
        }
    }

    /// Create a workflow instance
    /// - Parameters:
    ///   - typeName: Name of the workflow type
    ///   - input: Serialized input data
    /// - Returns: Workflow executor
    internal func createWorkflow(
        typeName: String,
        input: ByteBuffer
    ) throws -> any WorkflowExecutor {
        try workflowTypes.withLock { types in
            guard let factory = types[typeName] else {
                throw WorkflowError.unknownWorkflowType(typeName)
            }
            return try factory.create(input: input)
        }
    }

}

/// Factory protocol for creating workflow instances
internal protocol WorkflowFactory: Sendable {
    func create(input: ByteBuffer) throws -> any WorkflowExecutor
}

/// Concrete factory implementation
internal struct ConcreteWorkflowFactory<W: WorkflowProtocol>: WorkflowFactory {
    func create(input: ByteBuffer) throws -> any WorkflowExecutor {
        let decodedInput = try JSONDecoder().decode(W.Input.self, from: input)
        return ConcreteWorkflowExecutor<W>(workflowType: W.self, input: decodedInput)
    }
}

/// Protocol for workflow execution
internal protocol WorkflowExecutor: Sendable {
    func run(context: WorkflowExecutionContext) async throws -> ByteBuffer
}

/// Concrete workflow executor
internal struct ConcreteWorkflowExecutor<W: WorkflowProtocol>: WorkflowExecutor {
    let workflowType: W.Type
    let input: W.Input

    func run(context: WorkflowExecutionContext) async throws -> ByteBuffer {
        // Create workflow instance using type
        let workflow = workflowType.init()
        let output = try await workflow.run(input: input, context: context)
        return try JSONEncoder().encodeAsByteBuffer(output, allocator: ByteBufferAllocator())
    }
}

/// Job parameters for scheduled workflow execution (type-erased)
internal struct ScheduledWorkflowExecutionJob: JobParameters, Codable {
    public static let jobName = "ScheduledWorkflowExecution"

    let workflowTypeName: String
    let inputBuffer: ByteBuffer
    let scheduleId: String?

    internal init<W: WorkflowProtocol>(
        workflowType: W.Type,
        input: W.Input,
        scheduleId: String? = nil,
        options: WorkflowOptions = .init()
    ) throws {
        self.workflowTypeName = W.workflowName
        self.inputBuffer = try JSONEncoder().encodeAsByteBuffer(input, allocator: ByteBufferAllocator())
        self.scheduleId = scheduleId
    }
}

/// Main workflow engine
/// Workflow execution engine with scheduling support
public final class WorkflowEngine<Queue: JobQueueDriver & JobMetadataDriver>: Sendable {
    private let jobQueue: JobQueue<Queue>
    private let logger: Logger
    private let workflowRegistry: WorkflowRegistry
    private let internalRegistry: ActivityRegistry

    public init(
        jobQueue: JobQueue<Queue>,
        logger: Logger,
        activities: [any ActivityContainer] = []
    ) {
        self.jobQueue = jobQueue
        self.logger = logger
        self.workflowRegistry = WorkflowRegistry()
        self.internalRegistry = ActivityRegistry()

        // Register activities from containers
        internalRegistry.registerActivities(from: activities)

        self.registerWorkflowCoordinator()
        self.registerActivityExecutor()
        self.registerDelayJob()
    }

    /// Register a workflow type with the engine
    /// - Parameter workflowType: The workflow type to register
    public func registerWorkflow<W: WorkflowProtocol>(_ workflowType: W.Type) {
        workflowRegistry.registerWorkflow(workflowType)
        logger.debug("Registered workflow", metadata: ["workflowType": .string(W.workflowName)])
    }

    /// Start a new workflow execution
    /// - Parameters:
    ///   - workflowType: Type of workflow to execute
    ///   - input: Input parameters for the workflow
    ///   - workflowId: Optional custom workflow ID
    ///   - options: Workflow execution options
    /// - Returns: The workflow execution ID
    public func startWorkflow<W: WorkflowProtocol>(
        _ workflowType: W.Type,
        input: W.Input,
        workflowId: WorkflowID? = nil,
        options: WorkflowOptions = .init()
    ) async throws -> WorkflowID {
        let id = workflowId ?? WorkflowID()

        // Create workflow state using ByteBuffer
        let inputBuffer = try JSONEncoder().encodeAsByteBuffer(input, allocator: ByteBufferAllocator())
        let workflowState = WorkflowState(
            id: id,
            workflowType: W.workflowName,
            input: inputBuffer,
            status: .running,
            queuedAt: .now,
            scheduledAt: nil,
            startTime: .now
        )

        // Store workflow state in job metadata
        let stateBuffer = try JSONEncoder().encodeAsByteBuffer(
            workflowState,
            allocator: ByteBufferAllocator()
        )

        try await jobQueue.queue.setMetadata(
            key: "workflow:\(id.value)",
            value: stateBuffer
        )

        // Create initial coordinator job
        let coordinatorJob = WorkflowCoordinatorJob(
            workflowId: id,
            workflowType: W.workflowName,
            action: .start,
            stepIndex: 0
        )

        _ = try await jobQueue.push(coordinatorJob)

        logger.debug(
            "Started workflow",
            metadata: [
                "workflowId": .string(id.value),
                "workflowType": .string(W.workflowName),
            ]
        )

        return id
    }

    /// Get workflow execution status
    /// - Parameter workflowId: The workflow ID to check
    /// - Returns: Current workflow state
    public func getWorkflowStatus(_ workflowId: WorkflowID) async throws -> WorkflowExecutionStatus {
        let key = "workflow:\(workflowId.value)"
        guard let stateBuffer = try await jobQueue.queue.getMetadata(key) else {
            throw WorkflowError.executionNotFound(workflowId)
        }

        let workflowState = try JSONDecoder().decode(WorkflowState.self, from: stateBuffer)
        return WorkflowExecutionStatus(from: workflowState)
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
        let signalBuffer = try JSONEncoder().encodeAsByteBuffer(anySignal, allocator: ByteBufferAllocator())
        try await jobQueue.queue.setMetadata(key: signalKey, value: signalBuffer)

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
        guard state.status == .running else {
            return  // Already completed or failed
        }

        // Push a cancellation job to properly handle running activities
        let cancellationJob = WorkflowCoordinatorJob(
            workflowId: workflowId,
            workflowType: state.workflowType,
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
    internal func getInternalWorkflowState(_ workflowId: WorkflowID) async throws -> WorkflowState {
        let key = "workflow:\(workflowId.value)"
        guard let stateBuffer = try await jobQueue.queue.getMetadata(key) else {
            throw WorkflowError.executionNotFound(workflowId)
        }

        return try JSONDecoder().decode(WorkflowState.self, from: stateBuffer)
    }

    private func registerWorkflowCoordinator() {
        jobQueue.registerJob(parameters: WorkflowCoordinatorJob.self) { params, context in
            try await self.handleWorkflowCoordination(params, context: context)
        }
        registerScheduledWorkflowJob()
    }

    /// Schedule a workflow to run at specified intervals
    /// - Parameters:
    ///   - workflowType: The workflow type to schedule
    ///   - input: Input parameters for the workflow
    ///   - schedule: When to run the workflow (e.g., .daily(hour: 9), .hourly(minute: 30))
    ///   - scheduleId: Optional schedule identifier (will append timestamp for uniqueness)
    ///   - options: Workflow execution options
    /// - Returns: The job schedule for monitoring/cancellation
    public func scheduleWorkflow<W: WorkflowProtocol>(
        _ workflowType: W.Type,
        input: W.Input,
        schedule: Schedule,
        scheduleId: String? = nil,
        options: WorkflowOptions = .init()
    ) throws -> JobSchedule {
        let scheduledJob = try ScheduledWorkflowExecutionJob(
            workflowType: workflowType,
            input: input,
            scheduleId: scheduleId,
            options: options
        )

        var jobSchedule = JobSchedule()
        jobSchedule.addJob(scheduledJob, schedule: schedule)
        return jobSchedule
    }

    /// Create a scheduler service for running scheduled workflows
    /// - Parameter jobSchedule: The job schedule to run
    /// - Returns: A scheduler service that can be added to ServiceGroup
    public func createSchedulerService(with jobSchedule: JobSchedule) async -> JobSchedule.Scheduler<Queue> {
        await jobSchedule.scheduler(on: jobQueue)
    }

    private func registerScheduledWorkflowJob() {
        // Register handler for scheduled workflow jobs
        jobQueue.registerJob(parameters: ScheduledWorkflowExecutionJob.self) { params, context in
            try await self.handleScheduledWorkflowExecution(params, context: context)
        }
    }

    private func registerActivityExecutor() {
        jobQueue.registerJob(parameters: ActivityExecutionJob.self) { params, context in
            try await self.handleActivityExecution(params, context: context)
        }
    }

    private func handleWorkflowCoordination(
        _ job: WorkflowCoordinatorJob,
        context: JobExecutionContext
    ) async throws {
        let workflowLockKey = "workflow_lock:\(job.workflowId.value)"
        let jobIdBuffer = try JSONEncoder().encodeAsByteBuffer(context.jobID, allocator: ByteBufferAllocator())

        context.logger.debug(
            "Processing workflow coordination",
            metadata: [
                "workflowId": .string(job.workflowId.value),
                "action": .string(job.action.rawValue),
                "stepIndex": .stringConvertible(job.stepIndex),
            ]
        )

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

        defer {
            // Release workflow lock when done
            Task {
                try? await jobQueue.queue.releaseLock(key: workflowLockKey, id: jobIdBuffer)
            }
        }

        switch job.action {
        case .start:
            try await startWorkflowExecution(job, context: context)

        case .continueFromActivity:
            try await continueWorkflowExecution(job, context: context)

        case .executeParallelStep:
            try await executeParallelStep(job, context: context)

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
        // Get workflow state
        let state = try await getInternalWorkflowState(job.workflowId)

        // Create workflow executor
        let executor = try workflowRegistry.createWorkflow(
            typeName: job.workflowType,
            input: state.input
        )

        // Create workflow execution context
        let workflowContext = WorkflowExecutionContext(
            workflowId: job.workflowId,
            workflowType: job.workflowType,
            jobQueue: jobQueue,
            logger: context.logger,
            queuedAt: state.queuedAt,
            scheduledAt: state.scheduledAt,
            currentStep: state.currentStep,
            startTime: state.startTime
        )

        do {
            // Execute workflow with context
            let outputBuffer = try await executor.run(context: workflowContext)

            // Update workflow state with successful completion
            var updatedState = state
            updatedState.status = .completed
            updatedState.endTime = .now
            updatedState.output = outputBuffer

            // Mark context as completed for timing tracking
            workflowContext.markCompleted()

            let stateBuffer = try JSONEncoder().encodeAsByteBuffer(
                updatedState,
                allocator: ByteBufferAllocator()
            )

            try await jobQueue.queue.setMetadata(
                key: "workflow:\(job.workflowId.value)",
                value: stateBuffer
            )

            context.logger.debug(
                "Workflow completed successfully",
                metadata: ["workflowId": .string(job.workflowId.value)]
            )

        } catch {
            // Update workflow state with failure
            var updatedState = state
            updatedState.status = .failed
            updatedState.endTime = .now
            updatedState.error = error.localizedDescription

            let stateBuffer = try JSONEncoder().encodeAsByteBuffer(
                updatedState,
                allocator: ByteBufferAllocator()
            )

            try await jobQueue.queue.setMetadata(
                key: "workflow:\(job.workflowId.value)",
                value: stateBuffer
            )

            context.logger.error(
                "Workflow execution failed",
                metadata: [
                    "workflowId": .string(job.workflowId.value),
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
        // Get workflow state
        var state = try await getInternalWorkflowState(job.workflowId)

        // Check if workflow was cancelled
        guard state.status == .running else {
            context.logger.debug(
                "Workflow not running, skipping continuation",
                metadata: [
                    "workflowId": .string(job.workflowId.value),
                    "status": .string(state.status.rawValue),
                ]
            )
            return
        }

        // Merge activity results from the job
        for (key, value) in job.activityResults {
            state.activityResults[key] = value
        }

        // Update step history with completion of previous step
        if let _ = job.activityResult, let _ = state.stepHistory.last {
            let stepIndex = state.stepHistory.count - 1
            state.stepHistory[stepIndex].status = .completed
            state.stepHistory[stepIndex].endTime = .now
        }

        // Update current step
        state.currentStep = job.stepIndex

        // Create workflow executor
        let executor = try workflowRegistry.createWorkflow(
            typeName: job.workflowType,
            input: state.input
        )

        // Create workflow execution context with activity results for resumption
        let workflowContext = WorkflowExecutionContext(
            workflowId: job.workflowId,
            workflowType: job.workflowType,
            jobQueue: jobQueue,
            logger: context.logger,
            queuedAt: state.queuedAt,
            scheduledAt: state.scheduledAt,
            currentStep: job.stepIndex,
            startTime: state.startTime
        )

        do {
            // Continue workflow execution from where it left off
            let outputBuffer = try await executor.run(context: workflowContext)

            // Update workflow state with completion
            var updatedState = state
            updatedState.status = .completed
            updatedState.endTime = .now
            updatedState.output = outputBuffer

            // Mark context as completed for timing tracking
            workflowContext.markCompleted()

            let stateBuffer = try JSONEncoder().encodeAsByteBuffer(
                updatedState,
                allocator: ByteBufferAllocator()
            )

            try await jobQueue.queue.setMetadata(
                key: "workflow:\(job.workflowId.value)",
                value: stateBuffer
            )

            context.logger.debug(
                "Workflow continued and completed",
                metadata: ["workflowId": .string(job.workflowId.value)]
            )

        } catch {
            // Update workflow state with failure
            var updatedState = state
            updatedState.status = .failed
            updatedState.endTime = .now
            updatedState.error = error.localizedDescription

            let stateBuffer = try JSONEncoder().encodeAsByteBuffer(
                updatedState,
                allocator: ByteBufferAllocator()
            )

            try await jobQueue.queue.setMetadata(
                key: "workflow:\(job.workflowId.value)",
                value: stateBuffer
            )

            context.logger.error(
                "Workflow continuation failed",
                metadata: [
                    "workflowId": .string(job.workflowId.value),
                    "error": .string(error.localizedDescription),
                ]
            )

            throw error
        }
    }

    private func executeParallelStep(
        _ job: WorkflowCoordinatorJob,
        context: JobExecutionContext
    ) async throws {
        // Get workflow state
        let state = try await getInternalWorkflowState(job.workflowId)

        // Create workflow executor
        let executor = try workflowRegistry.createWorkflow(
            typeName: job.workflowType,
            input: state.input
        )

        // Create workflow execution context for parallel execution
        let workflowContext = WorkflowExecutionContext(
            workflowId: job.workflowId,
            workflowType: job.workflowType,
            jobQueue: jobQueue,
            logger: context.logger,
            queuedAt: context.queuedAt,
            scheduledAt: context.nextScheduledAt,
            currentStep: state.currentStep,
            startTime: state.startTime
        )

        // Execute parallel step - run workflow normally, parallel execution handled by workflow code
        do {
            let result = try await executor.run(context: workflowContext)
            let outputBuffer = try JSONEncoder().encodeAsByteBuffer(result, allocator: ByteBufferAllocator())

            // Update workflow state
            var updatedState = state
            updatedState.status = .completed
            updatedState.endTime = .now
            updatedState.output = outputBuffer

            // Mark context as completed for timing tracking
            workflowContext.markCompleted()

            let stateBuffer = try JSONEncoder().encodeAsByteBuffer(
                updatedState,
                allocator: ByteBufferAllocator()
            )

            try await jobQueue.queue.setMetadata(
                key: "workflow:\(job.workflowId.value)",
                value: stateBuffer
            )

            context.logger.debug(
                "Parallel step completed",
                metadata: ["workflowId": .string(job.workflowId.value)]
            )

        } catch {
            // Handle parallel execution failure
            var updatedState = state
            updatedState.status = .failed
            updatedState.endTime = .now
            updatedState.error = error.localizedDescription

            let stateBuffer = try JSONEncoder().encodeAsByteBuffer(
                updatedState,
                allocator: ByteBufferAllocator()
            )

            try await jobQueue.queue.setMetadata(
                key: "workflow:\(job.workflowId.value)",
                value: stateBuffer
            )

            throw error
        }
    }

    private func completeWorkflow(
        _ job: WorkflowCoordinatorJob,
        context: JobExecutionContext
    ) async throws {
        var state = try await getInternalWorkflowState(job.workflowId)
        state.status = .completed
        state.endTime = .now

        let stateBuffer = try JSONEncoder().encodeAsByteBuffer(
            state,
            allocator: ByteBufferAllocator()
        )

        try await jobQueue.queue.setMetadata(
            key: "workflow:\(job.workflowId.value)",
            value: stateBuffer
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
        var state = try await getInternalWorkflowState(job.workflowId)
        state.status = .failed
        state.endTime = .now

        let stateBuffer = try JSONEncoder().encodeAsByteBuffer(
            state,
            allocator: ByteBufferAllocator()
        )

        try await jobQueue.queue.setMetadata(
            key: "workflow:\(job.workflowId.value)",
            value: stateBuffer
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
        var state = try await getInternalWorkflowState(job.workflowId)

        // Mark all running steps as cancelled
        for i in state.stepHistory.indices {
            if state.stepHistory[i].status == .running {
                state.stepHistory[i].status = .cancelled
                state.stepHistory[i].endTime = .now
                state.stepHistory[i].error = "Workflow cancelled"
            }
        }

        state.status = .cancelled
        state.endTime = .now
        state.error = "Workflow cancelled by user"

        let stateBuffer = try JSONEncoder().encodeAsByteBuffer(
            state,
            allocator: ByteBufferAllocator()
        )

        try await jobQueue.queue.setMetadata(
            key: "workflow:\(job.workflowId.value)",
            value: stateBuffer
        )

        context.logger.debug(
            "Workflow execution cancelled",
            metadata: ["workflowId": .string(job.workflowId.value)]
        )
    }

    /// Run a workflow and wait for completion
    public func run<W: WorkflowProtocol>(
        _ workflowType: W.Type,
        input: W.Input,
        workflowId: WorkflowID? = nil,
        options: WorkflowOptions = .init()
    ) async throws -> W.Output {
        // Start the workflow
        let id = try await startWorkflow(workflowType, input: input, workflowId: workflowId, options: options)

        // Wait for completion
        let status = try await waitForWorkflowCompletion(
            id,
            timeout: options.timeout ?? .seconds(300)  // 5 minute default timeout
        )

        // Check final status
        switch status.status {
        case .completed:
            guard let outputBuffer = status.output else {
                throw WorkflowError.noOutput(id)
            }
            return try JSONDecoder().decode(W.Output.self, from: outputBuffer)

        case .failed:
            let errorMessage = status.error ?? "Unknown workflow error"
            throw WorkflowError.workflowFailed(errorMessage)

        case .cancelled:
            throw WorkflowError.workflowCancelled(id)

        default:
            throw WorkflowError.unexpectedStatus(id, status.status)
        }
    }

    /// Wait for workflow completion with polling
    private func waitForWorkflowCompletion(
        _ workflowId: WorkflowID,
        timeout: Duration
    ) async throws -> WorkflowExecutionStatus {
        let startTime = Date()
        let timeoutInterval = TimeInterval(timeout.components.seconds) + (TimeInterval(timeout.components.attoseconds) / 1_000_000_000_000_000_000)

        while Date().timeIntervalSince(startTime) < timeoutInterval {
            let status = try await getWorkflowStatus(workflowId)

            switch status.status {
            case .completed, .failed, .cancelled:
                return status
            case .running:
                // Continue polling
                try await Task.sleep(for: .milliseconds(100))
            }
        }

        throw WorkflowError.timeout(workflowId, timeout)
    }

    private func handleActivityExecution(
        _ job: ActivityExecutionJob,
        context: JobExecutionContext
    ) async throws {
        context.logger.debug(
            "Executing activity",
            metadata: [
                "activityName": .string(job.activityName),
                "activityId": .string(job.activityId),
                "workflowId": .string(job.workflowId.value),
            ]
        )

        do {
            // Execute activity using the global registry
            let result = try await internalRegistry.executeActivity(
                name: job.activityName,
                parameters: job.parameters
            )

            // Store activity result in metadata
            let resultBuffer = try JSONEncoder().encodeAsByteBuffer(
                result,
                allocator: ByteBufferAllocator()
            )

            try await jobQueue.queue.setMetadata(
                key: "activity_result:\(job.activityId)",
                value: resultBuffer
            )

            context.logger.debug(
                "Activity completed successfully",
                metadata: [
                    "activityId": .string(job.activityId),
                    "activityName": .string(job.activityName),
                ]
            )

            // Check if workflow is already running before triggering continuation
            let workflowLockKey = "workflow_lock:\(job.workflowId.value)"
            if let _ = try await jobQueue.queue.getMetadata(workflowLockKey) {
                context.logger.debug(
                    "Workflow already running, skipping continuation",
                    metadata: [
                        "workflowId": .string(job.workflowId.value)
                    ]
                )
                return
            }

            // Trigger workflow continuation for restart safety
            let continuationJob = WorkflowCoordinatorJob(
                workflowId: job.workflowId,
                workflowType: job.workflowType,
                action: .continueFromActivity,
                stepIndex: job.stepIndex + 1
            )

            try await jobQueue.push(continuationJob)

            context.logger.debug(
                "Triggered workflow continuation",
                metadata: [
                    "workflowId": .string(job.workflowId.value),
                    "stepIndex": .stringConvertible(job.stepIndex + 1),
                ]
            )

        } catch {
            // Store activity failure
            let failureResult = try AnyActivityResult(failure: error.localizedDescription)
            let resultBuffer = try JSONEncoder().encodeAsByteBuffer(
                failureResult,
                allocator: ByteBufferAllocator()
            )

            try await jobQueue.queue.setMetadata(
                key: "activity_result:\(job.activityId)",
                value: resultBuffer
            )

            context.logger.error(
                "Activity execution failed",
                metadata: [
                    "activityId": .string(job.activityId),
                    "activityName": .string(job.activityName),
                    "error": .string(error.localizedDescription),
                ]
            )

            // Check if workflow is already running before triggering continuation
            let workflowLockKey = "workflow_lock:\(job.workflowId.value)"
            if let _ = try await jobQueue.queue.getMetadata(workflowLockKey) {
                context.logger.debug(
                    "Workflow already running, skipping continuation after failure",
                    metadata: [
                        "workflowId": .string(job.workflowId.value)
                    ]
                )
                throw error
            }

            // Even on failure, trigger workflow continuation so it can handle the error
            let continuationJob = WorkflowCoordinatorJob(
                workflowId: job.workflowId,
                workflowType: job.workflowType,
                action: .continueFromActivity,
                stepIndex: job.stepIndex + 1
            )

            try await jobQueue.push(continuationJob)

            context.logger.debug(
                "Triggered workflow continuation after activity failure",
                metadata: [
                    "workflowId": .string(job.workflowId.value),
                    "stepIndex": .stringConvertible(job.stepIndex + 1),
                ]
            )

            throw error
        }
    }

    /// Register DelayJob for workflow sleep functionality
    private func registerDelayJob() {
        // Register standard DelayJob
        jobQueue.registerJob(parameters: DelayJob.self) { job, context in
            // Job completes immediately when executed (after the delayUntil time)
            // Store a completion marker in metadata for the sleep polling
            let resultKey = "delay_result:\(context.jobID)"
            let completionBuffer = try JSONEncoder().encodeAsByteBuffer("completed", allocator: ByteBufferAllocator())

            try await self.jobQueue.queue.setMetadata(key: resultKey, value: completionBuffer)

            context.logger.debug(
                "DelayJob completed and stored completion marker",
                metadata: [
                    "jobID": .string(context.jobID),
                    "resultKey": .string(resultKey),
                ]
            )
        }

        // Register WorkflowSleepJob for durable workflow sleep
        jobQueue.registerJob(parameters: WorkflowSleepJob.self) { job, context in
            // Sleep for the specified duration using Task.sleep
            // The job system handles the durable scheduling part
            try await Task.sleep(for: job.duration)

            // Store sleep completion marker in metadata using the sleep key
            let completionBuffer = try JSONEncoder().encodeAsByteBuffer("completed", allocator: ByteBufferAllocator())
            try await self.jobQueue.queue.setMetadata(key: job.sleepKey, value: completionBuffer)

            context.logger.debug(
                "WorkflowSleepJob completed and stored completion marker",
                metadata: [
                    "jobID": .string(context.jobID),
                    "sleepId": .string(job.sleepId),
                    "workflowId": .string(job.workflowId.value),
                    "duration": .stringConvertible(job.duration),
                ]
            )
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
            "Executing scheduled workflow",
            metadata: [
                "workflowType": .string(job.workflowTypeName),
                "scheduledWorkflowId": .string(workflowId.value),
                "scheduledAt": .stringConvertible(context.nextScheduledAt ?? context.queuedAt),
            ]
        )

        // Create and execute workflow using type-erased approach
        _ = try workflowRegistry.createWorkflow(
            typeName: job.workflowTypeName,
            input: job.inputBuffer
        )

        // Store workflow state
        let workflowState = WorkflowState(
            id: workflowId,
            workflowType: job.workflowTypeName,
            input: job.inputBuffer,
            status: .running,
            queuedAt: context.queuedAt,
            scheduledAt: context.nextScheduledAt
        )

        let stateKey = "workflow:\(workflowId.value)"
        let stateBuffer = try JSONEncoder().encodeAsByteBuffer(workflowState, allocator: ByteBufferAllocator())
        try await jobQueue.queue.setMetadata(key: stateKey, value: stateBuffer)

        // Start workflow execution
        let coordinatorJob = WorkflowCoordinatorJob(
            workflowId: workflowId,
            workflowType: job.workflowTypeName,
            action: .start,
            stepIndex: 0
        )

        _ = try await jobQueue.push(coordinatorJob)
    }
}

/// Extension to make the engine discoverable
extension WorkflowEngine: CustomStringConvertible {
    public var description: String { "WorkflowEngine<\(String(describing: Queue.self))>" }
}

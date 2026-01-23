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
import Synchronization

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Type-erased signal for storage
internal struct AnyWorkflowSignal: Codable {
    let signalName: String
    let dataBuffer: ByteBuffer?
    let timestamp: Date

    init<T: Codable & Sendable>(_ signal: WorkflowSignal<T>, allocator: ByteBufferAllocator = ByteBufferAllocator()) throws {
        self.signalName = signal.signalName
        if let data = signal.data {
            self.dataBuffer = try JSONEncoder().encodeAsByteBuffer(data, allocator: allocator)
        } else {
            self.dataBuffer = nil
        }
        self.timestamp = signal.timestamp
    }

    func decode<T: Codable & Sendable>(as type: T.Type) throws -> T? {
        guard let dataBuffer = dataBuffer else { return nil }
        return try JSONDecoder().decode(T.self, from: dataBuffer)
    }
}

/// Protocol for signal parameters
public protocol SignalParameters: Sendable {
    associatedtype Input: Codable & Sendable

    /// Name of the signal
    /// Defaults to the type name (e.g., "UpdateStatusSignal")
    static var signalName: String { get }
}

/// Default implementation using type name
extension SignalParameters {
    public static var signalName: String {
        String(describing: self)
    }
}

/// Signal data structure
internal struct WorkflowSignal<T: Codable & Sendable>: Codable {
    let signalName: String
    let data: T?
    let timestamp: Date
}

/// Update request data structure
internal struct WorkflowUpdateRequest<Input: Codable & Sendable>: Codable {
    let updateId: UUID
    let workflowType: String
    let input: Input
    let timestamp: Date
}

/// Update response data structure
internal struct WorkflowUpdateResponse<Output: Codable & Sendable>: Codable {
    let updateId: UUID
    let result: Output?
    let error: String?
    let timestamp: Date

    init(updateId: UUID, result: Output) {
        self.updateId = updateId
        self.result = result
        self.error = nil
        self.timestamp = Date.now
    }

    init(updateId: UUID, error: String) {
        self.updateId = updateId
        self.result = nil
        self.error = error
        self.timestamp = Date.now
    }
}

/// Context for workflow execution
public final class WorkflowExecutionContext: Sendable {
    /// Workflow execution ID
    public let workflowId: WorkflowID
    /// Workflow type name for continuation after activity completion
    public let workflowType: String
    /// Queue driver for executing activities - package-private to prevent external interference
    package let queueDriver: any WorkflowQueueDriver
    /// Logger for workflow execution
    public let logger: Logger
    /// Query registry for this workflow instance
    internal let queryRegistry: WorkflowQueryRegistry
    /// Shared ByteBuffer allocator for efficient memory management
    internal let allocator: ByteBufferAllocator
    /// Workflow registry for encoding/decoding
    internal let workflowRegistry: WorkflowRegistry
    /// Workflow repository for accessing workflow data
    public var workflowRepository: WorkflowRepository {
        queueDriver
    }
    /// When this workflow was queued for execution
    public let queuedAt: Date
    /// When this workflow was scheduled to run (nil for immediate workflows)
    public let scheduledAt: Date?
    /// When this workflow execution started
    public let startTime: Date
    /// When this workflow execution completed (if applicable) - thread-safe atomic
    private let _endTime: ManagedAtomic<UInt64>
    /// Current step in the workflow - tracks activity execution order for replay safety
    private let _currentStep: ManagedAtomic<Int>
    /// Workflow engine for handling sleep operations
    private let sleepEngine: (any WorkflowEngineProtocol & Sendable)?

    /// Current step in the workflow
    public var currentStep: Int {
        get { _currentStep.load(ordering: .relaxed) }
        set { _currentStep.store(newValue, ordering: .relaxed) }
    }

    /// When this workflow execution completed (if applicable)
    public var endTime: Date? {
        let timestamp = _endTime.load(ordering: .relaxed)
        return timestamp == 0 ? nil : Date(timeIntervalSince1970: TimeInterval(timestamp) / 1000.0)
    }

    /// Mark workflow execution as completed
    internal func markCompleted() {
        let timestamp = UInt64(Date.now.timeIntervalSince1970 * 1000.0)
        _endTime.store(timestamp, ordering: .relaxed)
    }

    /// Duration of workflow execution (if completed)
    public var runDuration: TimeInterval? {
        guard let endTime else { return nil }
        return endTime.timeIntervalSince(startTime)
    }

    /// Time between when workflow was scheduled and when it actually started executing
    public var schedulingDelay: TimeInterval? {
        guard let scheduledAt else { return nil }
        return startTime.timeIntervalSince(scheduledAt)
    }

    /// Total time from queueing to completion (if completed)
    public var totalLifetime: TimeInterval? {
        guard let endTime else { return nil }
        return endTime.timeIntervalSince(queuedAt)
    }

    internal init<Queue: WorkflowQueueDriver>(
        workflowId: WorkflowID,
        workflowType: String,
        jobQueue: JobQueue<Queue>,
        logger: Logger,
        allocator: ByteBufferAllocator,
        workflowRegistry: WorkflowRegistry,
        queuedAt: Date,
        scheduledAt: Date? = nil,
        currentStep: Int = 0,
        startTime: Date = Date.now,
        sleepEngine: (any WorkflowEngineProtocol & Sendable)?
    ) {
        self.workflowId = workflowId
        self.workflowType = workflowType
        self.queueDriver = jobQueue.queue
        self.logger = logger
        self.allocator = allocator
        self.workflowRegistry = workflowRegistry
        self.queryRegistry = WorkflowQueryRegistry(logger: logger)
        self.queuedAt = queuedAt
        self.scheduledAt = scheduledAt
        self.startTime = startTime
        self._endTime = ManagedAtomic(0)
        self._currentStep = ManagedAtomic(currentStep)
        self.sleepEngine = sleepEngine
    }

    /// Push a job to the queue (helper method for child workflows)
    package func pushJob<Parameters: JobParameters>(_ job: Parameters) async throws -> any Sendable & CustomStringConvertible {
        try await queueDriver.pushWorkflowJob(job)
    }

    /// Get activity result directly from repository - returns typed result
    internal func getActivityResult<T: Codable & Sendable>(_ activityId: ActivityID, resultType: T.Type) async throws -> ActivityResult<T>? {
        try await queueDriver.getResult(activityId, resultType: resultType)
    }

    /// Execute an activity and wait for its completion
    /// - Parameters:
    ///   - activityType: Type of the activity to execute (must conform to ActivityParameters)
    ///   - input: Input parameters for the activity
    ///   - options: Activity execution options
    /// - Returns: The activity's output result
    public func executeActivity<A: ActivityParameters>(
        _ activityType: A.Type,
        input: A.Input,
        options: ActivityOptions = .init(),
        isolation: isolated (any Actor)? = #isolation
    ) async throws -> A.Output {
        try await executeActivity(
            activityName: A.activityName,
            input: input,
            options: options,
            isolation: isolation
        )
    }

    /// Execute an activity within this workflow by name (backward compatibility)
    /// - Parameters:
    ///   - activityName: Name of the activity to execute
    ///   - input: Input parameters for the activity
    ///   - options: Activity execution options
    /// - Returns: The activity result
    public func executeActivity<Input: Codable & Sendable, Output: Codable & Sendable>(
        activityName: String,
        input: Input,
        options: ActivityOptions,
        isolation: isolated (any Actor)? = #isolation
    ) async throws -> Output {
        // Create deterministic activity ID based on activity name and input hash
        // This ensures replay safety and handles parallel activities correctly
        let encoder = JSONEncoder()
        encoder.outputFormatting = .sortedKeys  // Ensure deterministic field ordering
        let inputBuffer = try encoder.encodeAsByteBuffer(input, allocator: allocator)
        let inputHash = WorkflowUtilities.deterministicHash(buffer: inputBuffer, activityName: activityName)
        let activityId = "\(workflowId.runId):\(activityName):\(inputHash)"

        logger.debug(
            "Executing activity",
            metadata: [
                "activityName": .string(activityName),
                "activityId": .string(activityId),
                "workflowId": .string(workflowId.value),
            ]
        )

        // Check if this activity already completed (replay safety)
        if let result = try await queueDriver.getResult(ActivityID(activityId), resultType: Output.self) {
            switch result {
            case .success(let value):
                logger.debug(
                    "Activity already completed (replay safety)",
                    metadata: [
                        "activityId": .string(activityId),
                        "activityName": .string(activityName),
                    ]
                )
                currentStep += 1
                return value
            case .failure(let errorInfo):
                logger.error(
                    "Activity had previously failed",
                    metadata: [
                        "activityId": .string(activityId),
                        "activityName": .string(activityName),
                        "error": .string(errorInfo.message),
                        "errorType": .string(errorInfo.errorType),
                    ]
                )
                currentStep += 1
                // Check if the error is due to cancellation using proper type information
                if errorInfo.isCancellation {
                    let cancelledFailure = WorkflowCancelledFailure(message: errorInfo.message, workflowId: workflowId)
                    throw ActivityFailure(activityName: activityName, cause: cancelledFailure)
                } else {
                    let cause = WorkflowError.activityFailed(errorInfo.message)
                    throw ActivityFailure(activityName: activityName, cause: cause)
                }
            }
        }

        // Activity hasn't run yet, execute it
        // Get current step and increment for next activity
        let currentStepIndex = currentStep
        currentStep += 1

        // Create activity execution job with workflow type for restart safety
        let activityJob = try ActivityExecutionJob(
            activityId: ActivityID(activityId),
            workflowId: workflowId,
            activityName: activityName,
            input: input,
            timeout: options.startToCloseTimeout,
            retryPolicy: options.retryPolicy,
            workflowType: workflowType,
            stepIndex: currentStepIndex
        )

        // Push activity job to queue
        _ = try await queueDriver.pushWorkflowJob(activityJob)

        logger.debug(
            "Activity job queued",
            metadata: [
                "activityId": .string(activityId)
            ]
        )

        // Wait for activity completion via middleware callback - no polling needed!
        guard let engine = sleepEngine else {
            throw WorkflowError.workflowFailed("Sleep engine not available")
        }
        return try await engine.waitForActivity(activityId: activityId, expectedType: Output.self)
    }

    /// Wait for an external signal to be sent to this workflow
    /// - Parameters:
    ///   - signalType: Signal type conforming to SignalParameters
    ///   - timeout: Optional timeout duration
    /// - Returns: The signal input data
    @discardableResult
    public func waitForSignal<S: SignalParameters>(
        _ signalType: S.Type,
        timeout: Duration? = nil
    ) async throws -> S.Input? {
        try await waitForSignal(
            S.signalName,
            ofType: S.Input.self,
            timeout: timeout
        )
    }

    /// Sleep for a specified duration using WorkflowDelayJob with middleware-based completion
    /// - Parameter duration: How long to sleep
    public func sleep(for duration: Duration) async throws {
        // Generate deterministic sleep ID based on current step for restart safety
        let sleepId = "sleep_\(currentStep)"
        let sleepKey = "workflow_sleep:\(workflowId.value):\(sleepId)"

        // Get current step and increment for next activity
        let currentStepIndex = currentStep
        currentStep += 1

        // Mark workflow as sleeping before starting sleep operation
        try await queueDriver.workflowRepository.updateExecutionStatus(
            workflowId,
            status: .sleeping,
            startTime: nil,
            endTime: nil,
            error: nil
        )

        // Create a WorkflowDelayJob that will be handled by middleware when it completes
        let delayJob = WorkflowDelayJob(
            sleepKey: sleepKey,
            workflowId: workflowId,
            sleepId: sleepId
        )
        let delayUntil = Date.now.addingTimeInterval(TimeInterval(duration.timeInterval))

        // Schedule the delay job to execute at delayUntil time
        let jobId = try await queueDriver.pushWorkflowJob(delayJob, delayUntil: delayUntil)

        // Track the scheduled job for potential cancellation
        if let engine = sleepEngine {
            await engine.trackSleepJob(workflowId: workflowId, jobId: jobId)
        }

        logger.debug(
            "Sleep job scheduled with delayUntil - waiting for middleware completion",
            metadata: [
                "sleepId": .string(sleepId),
                "delayUntil": .stringConvertible(delayUntil),
                "stepIndex": .stringConvertible(currentStepIndex),
            ]
        )

        // Wait for the DelayJob to complete via middleware notification
        // middleware will resume when job completes
        guard let engine = sleepEngine else {
            throw WorkflowError.workflowFailed("Sleep engine not available")
        }

        do {
            try await engine.waitForSleep(sleepKey: sleepKey)

            // Mark workflow as running again after sleep completes
            // This will generate a "resumed" event since we're transitioning from sleeping to running
            try await queueDriver.workflowRepository.updateExecutionStatus(
                workflowId,
                status: .running,
                startTime: nil,
                endTime: nil,
                error: nil
            )
        } catch {
            // If sleep fails, restore running status
            try await queueDriver.workflowRepository.updateExecutionStatus(
                workflowId,
                status: .running,
                startTime: nil,
                endTime: nil,
                error: nil
            )
            throw error
        }
    }

    /// Internal string-based signal wait implementation
    private func waitForSignal<T: Codable & Sendable>(
        _ signalName: String,
        ofType type: T.Type,
        timeout: Duration? = nil
    ) async throws -> T? {
        let signalKey = "workflow_signal:\(workflowId.value):\(signalName)"
        let waitingKey = "workflow_waiting_signal:\(workflowId.value):\(signalName)"

        logger.debug(
            "Waiting for signal",
            metadata: [
                "workflowId": .string(workflowId.value),
                "signalName": .string(signalName),
            ]
        )

        // Check if signal already exists
        if let result = try await queueDriver.getResult(ActivityID(signalKey), resultType: AnyWorkflowSignal.self) {
            switch result {
            case .success(let anySignal):
                // Remove the signal after consumption
                try await queueDriver.deleteResult(ActivityID(signalKey))

                logger.debug(
                    "Signal already available",
                    metadata: [
                        "workflowId": .string(workflowId.value),
                        "signalName": .string(signalName),
                    ]
                )
                return try anySignal.decode(as: type)
            case .failure:
                break
            }
        }

        // Mark as waiting for signal
        try await queueDriver.saveResult(ActivityID(waitingKey), result: ActivityResult<String>.success("waiting"))

        // Poll for the signal
        let startTime = Date.now
        let endTime = timeout.map { startTime.addingTimeInterval(TimeInterval(duration: $0)) } ?? Date.distantFuture

        // Poll for the signal

        while Date.now < endTime {
            if let result = try await queueDriver.getResult(ActivityID(signalKey), resultType: AnyWorkflowSignal.self) {
                switch result {
                case .success(let anySignal):
                    // Clean up
                    try await queueDriver.deleteResult(ActivityID(signalKey))
                    try await queueDriver.deleteResult(ActivityID(waitingKey))

                    logger.debug(
                        "Received signal",
                        metadata: [
                            "workflowId": .string(workflowId.value),
                            "signalName": .string(signalName),
                        ]
                    )
                    return try anySignal.decode(as: type)
                case .failure:
                    break
                }
            }
            try await Task.sleep(for: .milliseconds(100))
        }

        // Clean up waiting marker on timeout
        try await queueDriver.deleteResult(ActivityID(waitingKey))

        if timeout != nil {
            throw WorkflowError.signalTimeout(signalName)
        }

        return nil
    }

    /// Wait for a condition to be met using event-driven continuation pattern
    /// - Parameters:
    ///   - timeout: Optional timeout duration (nil = wait indefinitely)
    ///   - condition: Closure that returns true when condition is met
    public func awaitCondition(timeout: Duration? = nil, condition: @Sendable @escaping () -> Bool) async throws {
        // Check condition immediately first
        if condition() {
            return
        }

        let conditionId = UUID().uuidString

        logger.debug(
            "Waiting for condition",
            metadata: [
                "workflowId": .string(workflowId.value),
                "conditionId": .string(conditionId),
            ]
        )

        return try await withCheckedThrowingContinuation { continuation in
            Task {
                // Register continuation in global registry
                await WorkflowConditionRegistry.shared.register(
                    conditionId: conditionId,
                    workflowId: workflowId,
                    condition: condition,
                    continuation: continuation
                )

                // Set up timeout if specified
                if let timeout = timeout {
                    Task {
                        let timeoutSeconds =
                            Double(timeout.components.seconds) + Double(timeout.components.attoseconds) / 1_000_000_000_000_000_000
                        try await Task.sleep(for: .seconds(timeoutSeconds))

                        await WorkflowConditionRegistry.shared.failCondition(
                            conditionId: conditionId,
                            error: WorkflowError.conditionTimeout(timeout)
                        )
                    }
                }
            }
        }
    }

    /// Store workflow output with proper types
    /// This should be called by ConcreteWorkflowExecutor when workflow completes
    /// Store workflow output when execution completes
    /// - Parameters:
    ///   - input: Input data that was passed to the workflow
    ///   - output: Output data returned by the workflow
    ///   - inputType: Type of the input for type safety
    ///   - outputType: Type of the output for type safety
    public func storeWorkflowOutput<Input: Codable & Sendable, Output: Codable & Sendable>(
        input: Input,
        output: Output,
        inputType: Input.Type,
        outputType: Output.Type
    ) async throws {
        // Get the current workflow execution
        guard var execution = try await queueDriver.workflowRepository.getExecution(workflowId) else {
            logger.error("Failed to find workflow execution for output storage")
            return
        }

        // Encode the output using the registry for consistent serialization
        execution.result = try workflowRegistry.encode(output)

        // Update the execution with the output
        try await queueDriver.workflowRepository.updateExecution(execution)

        logger.debug(
            "Stored workflow output successfully",
            metadata: [
                "workflowId": .string(workflowId.value),
                "outputType": .string(String(describing: outputType)),
            ]
        )
    }

}

/// Workflow delay job that uses delayUntil for precise timing
internal struct WorkflowDelayJob: JobParameters {
    public static let jobName = "WorkflowDelayJob"

    let sleepKey: String
    let workflowId: WorkflowID
    let sleepId: String

    public init(sleepKey: String, workflowId: WorkflowID, sleepId: String) {
        self.sleepKey = sleepKey
        self.workflowId = workflowId
        self.sleepId = sleepId
    }
}

/// Protocol for workflow engines that can handle sleep and activity operations
protocol WorkflowEngineProtocol: Sendable {
    func waitForSleep(sleepKey: String) async throws
    func waitForActivity<Output: Codable & Sendable>(activityId: String, expectedType: Output.Type) async throws -> Output
    func trackSleepJob(workflowId: WorkflowID, jobId: any Sendable & CustomStringConvertible) async
}

/// Registry for managing workflow condition continuations using the same pattern as other registries
public final class WorkflowConditionRegistry: Sendable {
    public static let shared = WorkflowConditionRegistry()

    private let conditions: Mutex<[String: ConditionEntry]> = .init([:])
    private let logger = Logger(label: "WorkflowConditionRegistry")

    private struct ConditionEntry: Sendable {
        let workflowId: WorkflowID
        let condition: @Sendable () -> Bool
        let continuation: CheckedContinuation<Void, Error>
    }

    private init() {}

    /// Register a condition continuation
    public func register(
        conditionId: String,
        workflowId: WorkflowID,
        condition: @Sendable @escaping () -> Bool,
        continuation: CheckedContinuation<Void, Error>
    ) async {
        let entry = ConditionEntry(
            workflowId: workflowId,
            condition: condition,
            continuation: continuation
        )

        conditions.withLock {
            $0[conditionId] = entry
        }

        logger.debug(
            "Registered condition",
            metadata: [
                "conditionId": .string(conditionId),
                "workflowId": .string(workflowId.value),
            ]
        )
    }

    /// Evaluate all conditions for a workflow and complete any that are now true
    public func evaluateConditions(for workflowId: WorkflowID) {
        let workflowConditions = conditions.withLock {
            $0.filter { _, entry in
                entry.workflowId == workflowId
            }
        }

        for (conditionId, entry) in workflowConditions {
            if entry.condition() {
                // Remove and complete the condition
                let removedEntry = conditions.withLock {
                    $0.removeValue(forKey: conditionId)
                }

                if let removedEntry = removedEntry {
                    removedEntry.continuation.resume()

                    logger.debug(
                        "Condition met, completed",
                        metadata: [
                            "conditionId": .string(conditionId),
                            "workflowId": .string(workflowId.value),
                        ]
                    )
                }
            }
        }
    }

    /// Fail a condition with an error
    public func failCondition(conditionId: String, error: Error) async {
        let entry = conditions.withLock {
            $0.removeValue(forKey: conditionId)
        }

        if let entry = entry {
            entry.continuation.resume(throwing: error)

            logger.debug(
                "Failed condition",
                metadata: [
                    "conditionId": .string(conditionId),
                    "workflowId": .string(entry.workflowId.value),
                ]
            )
        }
    }

    /// Clean up all conditions for a workflow
    public func cleanup(workflowId: WorkflowID) async {
        let workflowConditions = conditions.withLock { conditions in
            let entries = conditions.filter { _, entry in
                entry.workflowId == workflowId
            }

            for (conditionId, _) in entries {
                conditions.removeValue(forKey: conditionId)
            }

            return entries
        }

        let error = WorkflowError.workflowNotAccessible(workflowId)
        for (_, entry) in workflowConditions {
            entry.continuation.resume(throwing: error)
        }

        if !workflowConditions.isEmpty {
            logger.debug(
                "Cleaned up conditions",
                metadata: [
                    "workflowId": .string(workflowId.value),
                    "count": .stringConvertible(workflowConditions.count),
                ]
            )
        }
    }
}

/// Additional workflow errors
extension WorkflowError {
    static func signalTimeout(_ signalName: String) -> WorkflowError {
        .workflowFailed("Signal '\(signalName)' timed out")
    }

    static func updateTimeout(_ workflowId: WorkflowID, _ timeout: Duration) -> WorkflowError {
        .workflowFailed("Update to workflow '\(workflowId.value)' timed out after \(timeout)")
    }

    static func conditionTimeout(_ timeout: Duration) -> WorkflowError {
        .workflowFailed("Condition not met within timeout of \(timeout)")
    }

    static func invalidState(_ message: String) -> WorkflowError {
        .workflowFailed("Invalid state: \(message)")
    }
}

extension TimeInterval {
    init(duration: Duration) {
        self = Double(duration.components.seconds) + Double(duration.components.attoseconds) / 1_000_000_000_000_000_000
    }
}

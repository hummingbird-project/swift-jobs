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

    init<T: Codable & Sendable>(_ signal: WorkflowSignal<T>) throws {
        self.signalName = signal.signalName
        if let data = signal.data {
            self.dataBuffer = try JSONEncoder().encodeAsByteBuffer(data, allocator: ByteBufferAllocator())
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
    static var signalName: String { get }
}

/// Signal data structure
internal struct WorkflowSignal<T: Codable & Sendable>: Codable {
    let signalName: String
    let data: T?
    let timestamp: Date
}

/// Context for workflow execution
public final class WorkflowExecutionContext: Sendable {
    /// Workflow execution ID
    public let workflowId: WorkflowID
    /// Workflow type name for continuation after activity completion
    public let workflowType: String
    /// Job queue for executing activities - package-private to prevent external interference
    package let jobQueue: any JobQueueProtocol & Sendable
    /// Direct access to metadata storage - private for internal workflow operations
    private let metadataQueue: any JobMetadataDriver
    /// Logger for workflow execution
    public let logger: Logger
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

    internal init<Queue: JobQueueDriver & JobMetadataDriver>(
        workflowId: WorkflowID,
        workflowType: String,
        jobQueue: JobQueue<Queue>,
        logger: Logger,
        queuedAt: Date,
        scheduledAt: Date? = nil,
        currentStep: Int = 0,
        startTime: Date = .now
    ) {
        self.workflowId = workflowId
        self.workflowType = workflowType
        self.jobQueue = jobQueue
        self.metadataQueue = jobQueue.queue
        self.logger = logger
        self.queuedAt = queuedAt
        self.scheduledAt = scheduledAt
        self.startTime = startTime
        self._endTime = ManagedAtomic(0)
        self._currentStep = ManagedAtomic(currentStep)
    }

    /// Execute an activity using type-safe activity parameters
    /// - Parameters:
    ///   - activityType: Activity type conforming to ActivityParameters
    ///   - input: Input parameters for the activity
    ///   - options: Activity execution options
    /// - Returns: The activity result
    @discardableResult
    public func executeActivity<A: ActivityParameters>(
        _ activityType: A.Type,
        input: A.Input,
        options: ActivityOptions = .init()
    ) async throws -> A.Output {
        try await executeActivity(
            activityName: A.activityName,
            input: input,
            options: options
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
        options: ActivityOptions
    ) async throws -> Output {
        // Create deterministic activity ID based on activity name and input hash
        // This ensures replay safety and handles parallel activities correctly
        let encoder = JSONEncoder()
        encoder.outputFormatting = .sortedKeys  // Ensure deterministic field ordering
        let inputBuffer = try encoder.encodeAsByteBuffer(input, allocator: ByteBufferAllocator())
        let inputHash = deterministicHash(buffer: inputBuffer, activityName: activityName)
        let activityId = "\(workflowId.runId):\(activityName):\(inputHash)"

        logger.debug(
            "Executing activity",
            metadata: [
                "activityName": .string(activityName),
                "activityId": .string(activityId),
                "workflowId": .string(workflowId.value),
            ]
        )

        // Serialize activity input using ByteBuffer
        let parametersBuffer = try JSONEncoder().encodeAsByteBuffer(input, allocator: ByteBufferAllocator())

        // Serialize retry policy if provided
        let retryPolicyBuffer: ByteBuffer?
        if let retryPolicy = options.retryPolicy {
            let strategyName = String(describing: type(of: retryPolicy))
            retryPolicyBuffer = try JSONEncoder().encodeAsByteBuffer(strategyName, allocator: ByteBufferAllocator())
        } else {
            retryPolicyBuffer = nil
        }

        // Check if this activity already completed (replay safety)
        let resultKey = "activity_result:\(activityId)"
        if let resultBuffer = try await metadataQueue.getMetadata(resultKey) {
            let anyResult = try JSONDecoder().decode(AnyActivityResult.self, from: resultBuffer)
            let result: ActivityResult<Output> = try anyResult.decode(as: Output.self)

            switch result {
            case .success(let value):
                logger.debug(
                    "Activity already completed (replay safety)",
                    metadata: [
                        "activityId": .string(activityId),
                        "activityName": .string(activityName),
                    ]
                )
                // Increment step counter and return cached result
                currentStep += 1
                return value
            case .failure(let error):
                logger.error(
                    "Activity had previously failed",
                    metadata: [
                        "activityId": .string(activityId),
                        "activityName": .string(activityName),
                        "error": .string(error),
                    ]
                )
                currentStep += 1
                throw WorkflowError.activityFailed(error)
            }
        }

        // Activity hasn't run yet, execute it
        // Get current step and increment for next activity
        let currentStepIndex = currentStep
        currentStep += 1

        // Create activity execution job with workflow type for restart safety
        let activityJob = ActivityExecutionJob(
            activityId: activityId,
            workflowId: workflowId,
            activityName: activityName,
            parameters: parametersBuffer,
            timeout: options.startToCloseTimeout,
            retryPolicy: retryPolicyBuffer,
            workflowType: workflowType,
            stepIndex: currentStepIndex
        )

        // Push activity job to queue
        let jobId = try await jobQueue.push(activityJob)

        logger.debug(
            "Activity job queued",
            metadata: [
                "activityId": .string(activityId),
                "jobId": .stringConvertible(jobId),
            ]
        )

        // Wait for activity completion
        return try await waitForActivityCompletion(
            activityId: activityId,
            expectedType: Output.self
        )
    }

    /// Wait for an activity to complete and return its result
    /// This method polls for activity completion since replay safety is handled earlier
    private func waitForActivityCompletion<Output: Codable & Sendable>(
        activityId: String,
        expectedType: Output.Type
    ) async throws -> Output {
        let resultKey = "activity_result:\(activityId)"

        // Poll for activity completion
        let maxWaitTime = Date.now.addingTimeInterval(300)  // 5 minute timeout
        while Date.now < maxWaitTime {
            if let resultBuffer = try await metadataQueue.getMetadata(resultKey) {
                let anyResult = try JSONDecoder().decode(AnyActivityResult.self, from: resultBuffer)
                let result: ActivityResult<Output> = try anyResult.decode(as: Output.self)

                switch result {
                case .success(let value):
                    logger.debug(
                        "Activity completed successfully",
                        metadata: [
                            "activityId": .string(activityId)
                        ]
                    )
                    return value
                case .failure(let error):
                    logger.error(
                        "Activity failed",
                        metadata: [
                            "activityId": .string(activityId),
                            "error": .string(error),
                        ]
                    )
                    throw WorkflowError.activityFailed(error)
                }
            }

            // Wait before polling again
            try await Task.sleep(for: .milliseconds(100))
        }

        throw WorkflowError.activityTimeout(activityId)
    }

    /// Wait for an external signal to be sent to this workflow using type-safe signal parameters
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

    /// Sleep for a specified duration using a custom SleepJob
    /// - Parameter duration: How long to sleep
    public func sleep(for duration: Duration) async throws {
        // Generate unique sleep ID for restart safety
        let sleepId = UUID().uuidString
        let sleepKey = "workflow_sleep:\(workflowId.value):\(sleepId)"

        // Check if sleep already completed (restart safety)
        if let _ = try await metadataQueue.getMetadata(sleepKey) {
            return  // Sleep was already completed before restart
        }

        // Create a SleepJob with the duration and sleep tracking info
        let sleepJob = WorkflowSleepJob(
            sleepId: sleepId,
            workflowId: workflowId,
            duration: duration,
            sleepKey: sleepKey
        )

        // Push the sleep job to the queue
        _ = try await jobQueue.push(sleepJob)

        // Wait for the sleep to complete by polling the sleep completion key
        let endTime = Date.now.addingTimeInterval(TimeInterval(duration: duration) + 10)

        while Date.now < endTime {
            if let _ = try await metadataQueue.getMetadata(sleepKey) {
                return  // Sleep completed
            }
            try await Task.sleep(for: .milliseconds(100))
        }

        throw WorkflowError.workflowTimedOut
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
        if let signalBuffer = try await metadataQueue.getMetadata(signalKey) {
            let anySignal = try JSONDecoder().decode(AnyWorkflowSignal.self, from: signalBuffer)

            // Remove the signal after consumption
            try await metadataQueue.setMetadata(key: signalKey, value: ByteBuffer())

            logger.debug(
                "Signal already available",
                metadata: [
                    "workflowId": .string(workflowId.value),
                    "signalName": .string(signalName),
                ]
            )

            return try anySignal.decode(as: type)
        }

        // Mark that we're waiting for this signal
        let waitingMarker = "waiting"
        let waitingBuffer = try JSONEncoder().encodeAsByteBuffer(waitingMarker, allocator: ByteBufferAllocator())
        try await metadataQueue.setMetadata(key: waitingKey, value: waitingBuffer)

        // Poll for the signal
        let startTime = Date.now
        let endTime = timeout.map { startTime.addingTimeInterval(TimeInterval(duration: $0)) } ?? Date.distantFuture

        while Date.now < endTime {
            if let signalBuffer = try await metadataQueue.getMetadata(signalKey) {
                let anySignal = try JSONDecoder().decode(AnyWorkflowSignal.self, from: signalBuffer)

                // Clean up
                try await metadataQueue.setMetadata(key: signalKey, value: ByteBuffer())
                try await metadataQueue.setMetadata(key: waitingKey, value: ByteBuffer())

                logger.debug(
                    "Received signal",
                    metadata: [
                        "workflowId": .string(workflowId.value),
                        "signalName": .string(signalName),
                    ]
                )

                return try anySignal.decode(as: type)
            }

            try await Task.sleep(for: .milliseconds(100))
        }

        // Clean up waiting marker on timeout
        try await metadataQueue.setMetadata(key: waitingKey, value: ByteBuffer())

        if timeout != nil {
            throw WorkflowError.signalTimeout(signalName)
        }

        return nil
    }

    /// Simple deterministic hash function for activity IDs
    /// This replaces Swift's Hasher which is non-deterministic across restarts
    private func deterministicHash(buffer: ByteBuffer, activityName: String) -> String {
        var hash: UInt64 = 14_695_981_039_346_656_037  // FNV-1a offset basis
        let prime: UInt64 = 1_099_511_628_211  // FNV-1a prime

        let bytes = buffer.getBytes(at: 0, length: buffer.readableBytes) ?? []

        for byte in bytes {
            hash ^= UInt64(byte)
            hash = hash &* prime
        }

        return String(hash, radix: 16)
    }
}

/// Workflow sleep job that handles timing internally
internal struct WorkflowSleepJob: JobParameters {
    public static let jobName = "WorkflowSleepJob"

    let sleepId: String
    let workflowId: WorkflowID
    let duration: Duration
    let sleepKey: String

    public init(sleepId: String, workflowId: WorkflowID, duration: Duration, sleepKey: String) {
        self.sleepId = sleepId
        self.workflowId = workflowId
        self.duration = duration
        self.sleepKey = sleepKey
    }
}

/// Additional workflow errors
extension WorkflowError {
    static func activityTimeout(_ activityId: String) -> WorkflowError {
        .activityFailed("Activity \(activityId) timed out")
    }

    static func signalTimeout(_ signalName: String) -> WorkflowError {
        .workflowFailed("Signal '\(signalName)' timed out")
    }
}

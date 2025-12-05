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

import Jobs
import NIOCore

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Workflow execution state with typed input and output from WorkflowProtocol
public struct WorkflowState<Input: Codable & Sendable, Output: Codable & Sendable>: Codable, Sendable {
    /// Unique workflow execution identifier
    public let id: WorkflowID
    /// Type name of the workflow
    public let workflowType: String
    /// Workflow input data
    public let input: Input
    /// Current execution status
    public var status: WorkflowStatus
    /// When the workflow was queued
    public let queuedAt: Date
    /// When the workflow was scheduled (if applicable)
    public let scheduledAt: Date?
    /// When the workflow started
    public let startTime: Date
    /// When the workflow completed (if applicable)
    public var endTime: Date?
    /// Current step index in the workflow
    public var currentStep: Int
    /// Step execution history for resumption and replay
    public var stepHistory: [WorkflowStepExecution]
    /// Error message if workflow failed
    public var error: String?
    /// Workflow output data (if completed)
    public var output: Output?

    public init(
        id: WorkflowID,
        workflowType: String,
        input: Input,
        status: WorkflowStatus = .running,
        queuedAt: Date = .now,
        scheduledAt: Date? = nil,
        startTime: Date = .now,
        currentStep: Int = 0
    ) {
        self.id = id
        self.workflowType = workflowType
        self.input = input
        self.status = status
        self.queuedAt = queuedAt
        self.scheduledAt = scheduledAt
        self.startTime = startTime
        self.currentStep = currentStep
        self.stepHistory = []
        self.endTime = nil
        self.error = nil
        self.output = nil
    }
}

/// Tracks execution of individual workflow steps
public struct WorkflowStepExecution: Codable, Sendable {
    /// Workflow execution ID this step belongs to
    public let workflowId: WorkflowID
    /// Step index in the workflow
    public let stepIndex: Int
    /// Human-readable step name/identifier
    public let stepName: String
    /// Type of step (activity, parallel, condition, etc.)
    public let stepType: WorkflowStepType
    /// When this step was created
    public let createdAt: Date
    /// When this step was queued for execution
    public var queuedAt: Date?
    /// When this step started executing
    public var startedAt: Date?
    /// When this step completed (if applicable)
    public var completedAt: Date?
    /// Step execution status
    public var status: WorkflowStepStatus
    /// Activity ID for activity steps
    public let activityId: String?
    /// Worker ID that executed this step
    public var workerId: String?
    /// Current retry attempt (0 for first attempt)
    public var retryAttempt: Int
    /// Maximum number of retry attempts allowed
    public let maxRetries: Int
    /// Error if step failed
    public var error: String?
    /// Step input data (encoded as JSON)
    public var input: Data?
    /// Step output data (encoded as JSON)
    public var output: Data?
    /// Step timeout duration
    public let timeout: Duration?
    /// Priority level for step execution
    public let priority: Int
    /// Additional metadata for this step
    public var metadata: [String: String]

    public init(
        workflowId: WorkflowID,
        stepIndex: Int,
        stepName: String,
        stepType: WorkflowStepType,
        createdAt: Date = .now,
        activityId: String? = nil,
        maxRetries: Int = 3,
        timeout: Duration? = nil,
        priority: Int = 1,
        metadata: [String: String] = [:]
    ) {
        self.workflowId = workflowId
        self.stepIndex = stepIndex
        self.stepName = stepName
        self.stepType = stepType
        self.createdAt = createdAt
        self.queuedAt = nil
        self.startedAt = nil
        self.completedAt = nil
        self.status = .pending
        self.activityId = activityId
        self.workerId = nil
        self.retryAttempt = 0
        self.maxRetries = maxRetries
        self.error = nil
        self.input = nil
        self.output = nil
        self.timeout = timeout
        self.priority = priority
        self.metadata = metadata
    }
}

/// Types of workflow steps
public enum WorkflowStepType: String, Codable, Sendable {
    case activity
    case parallel
    case condition
    case delay
    case signal
    case fork
    case join
    case loop
    case subworkflow
}

/// Status of workflow step execution
public enum WorkflowStepStatus: String, Codable, Sendable {
    /// Step has been created but not yet queued
    case pending = "PENDING"
    /// Step is queued for execution
    case queued = "QUEUED"
    /// Step has been assigned to a worker
    case assigned = "ASSIGNED"
    /// Step is currently running
    case running = "RUNNING"
    /// Step completed successfully
    case succeeded = "SUCCEEDED"
    /// Step failed
    case failed = "FAILED"
    /// Step was cancelled
    case cancelled = "CANCELLED"
    /// Step is being cancelled
    case cancelling = "CANCELLING"
    /// Step is in backoff state before retry
    case backoff = "BACKOFF"
    /// Step was skipped due to conditions
    case skipped = "SKIPPED"
    /// Step timed out
    case timedOut = "TIMED_OUT"
}

// MARK: - Step Execution Helpers

extension WorkflowStepExecution {
    /// Mark step as queued
    public mutating func markQueued() {
        self.status = .queued
        self.queuedAt = .now
    }

    /// Mark step as assigned to a worker
    public mutating func markAssigned(to workerId: String) {
        self.status = .assigned
        self.workerId = workerId
    }

    /// Mark step as started
    public mutating func markStarted() {
        self.status = .running
        self.startedAt = .now
    }

    /// Mark step as completed successfully
    public mutating func markCompleted(output: Data? = nil) {
        self.status = .succeeded
        self.completedAt = .now
        self.output = output
    }

    /// Mark step as failed
    public mutating func markFailed(error: String) {
        self.status = .failed
        self.completedAt = .now
        self.error = error
    }

    /// Mark step as cancelled
    public mutating func markCancelled() {
        self.status = .cancelled
        self.completedAt = .now
    }

    /// Mark step as timed out
    public mutating func markTimedOut() {
        self.status = .timedOut
        self.completedAt = .now
        self.error = "Step execution timed out"
    }

    /// Mark step as skipped
    public mutating func markSkipped(reason: String? = nil) {
        self.status = .skipped
        self.completedAt = .now
        if let reason = reason {
            self.metadata["skip_reason"] = reason
        }
    }

    /// Increment retry attempt
    public mutating func incrementRetry() {
        self.retryAttempt += 1
        self.status = .backoff
    }

    /// Check if step can be retried
    public var canRetry: Bool {
        retryAttempt < maxRetries && (status == .failed || status == .timedOut)
    }

    /// Get execution duration if step has completed
    public var duration: TimeInterval? {
        guard let startedAt = startedAt,
            let completedAt = completedAt
        else {
            return nil
        }
        return completedAt.timeIntervalSince(startedAt)
    }

    /// Check if step is in a terminal state
    public var isTerminal: Bool {
        switch status {
        case .succeeded, .failed, .cancelled, .skipped, .timedOut:
            return true
        case .pending, .queued, .assigned, .running, .cancelling, .backoff:
            return false
        }
    }
}

/// Job that coordinates workflow execution
public struct WorkflowCoordinatorJob: JobParameters {
    public static let jobName = "WorkflowCoordinator"

    /// The workflow execution ID
    public let workflowId: WorkflowID
    /// Type of workflow being executed
    public let workflowType: String
    /// Action to perform
    public let action: WorkflowAction
    /// Current step index
    public let stepIndex: Int

    public init(
        workflowId: WorkflowID,
        workflowType: String,
        action: WorkflowAction,
        stepIndex: Int
    ) {
        self.workflowId = workflowId
        self.workflowType = workflowType
        self.action = action
        self.stepIndex = stepIndex
    }
}

/// Actions that the workflow coordinator can perform
public enum WorkflowAction: String, Codable, Sendable {
    /// Start a new workflow execution
    case start
    /// Continue execution after an activity completes
    case continueFromActivity
    /// Mark workflow as completed
    case complete
    /// Mark workflow as failed
    case fail
    /// Mark workflow as cancelled
    case cancel
}

/// Job that executes an activity within a workflow
public struct ActivityExecutionJob: JobParameters {
    public static var jobName: String { "ActivityExecution" }

    /// Unique identifier for this activity execution
    public let activityId: ActivityID
    /// The workflow this activity belongs to
    public let workflowId: WorkflowID
    /// Name of the activity to execute
    public let activityName: String
    /// Activity input data (serialized as ByteBuffer for type erasure)
    public let inputBuffer: ByteBuffer
    /// Timeout for activity execution
    public let timeout: Duration?
    /// Retry policy type name
    public let retryPolicyType: String?
    /// Workflow type for continuation after activity completion
    public let workflowType: String
    /// Current step index for continuation
    public let stepIndex: Int

    public init<Input: Codable & Sendable>(
        activityId: ActivityID,
        workflowId: WorkflowID,
        activityName: String,
        input: Input,
        timeout: Duration? = nil,
        retryPolicyType: String? = nil,
        workflowType: String,
        stepIndex: Int
    ) throws {
        self.activityId = activityId
        self.workflowId = workflowId
        self.activityName = activityName
        self.inputBuffer = try JSONEncoder().encodeAsByteBuffer(input, allocator: ByteBufferAllocator())
        self.timeout = timeout
        self.retryPolicyType = retryPolicyType
        self.workflowType = workflowType
        self.stepIndex = stepIndex
    }

}

/// Result of an activity execution
/// Result of activity execution with proper error type information
public enum ActivityResult<T: Codable & Sendable>: Codable, Sendable {
    case success(T)
    case failure(ActivityErrorInfo)
}

/// Error information for activity failures
public struct ActivityErrorInfo: Codable, Sendable {
    /// Human-readable error message
    public let message: String
    /// Error type name for type checking
    public let errorType: String
    /// Whether this error represents cancellation
    public let isCancellation: Bool

    public init(message: String, errorType: String, isCancellation: Bool = false) {
        self.message = message
        self.errorType = errorType
        self.isCancellation = isCancellation
    }

    /// Create from Swift Error
    public init(from error: Error) {
        self.message = error.localizedDescription
        self.errorType = String(describing: type(of: error))
        self.isCancellation = error is CancellationError || error is WorkflowCancelledFailure
    }
}

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

import NIOCore

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Workflow execution state stored in job metadata
internal struct WorkflowState: Codable, Sendable {
    /// Unique workflow execution identifier
    public let id: WorkflowID
    /// Type name of the workflow
    public let workflowType: String
    /// Serialized input data
    public let input: ByteBuffer
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
    /// Activity results for resumption
    public var activityResults: [String: ByteBuffer]
    /// Error message if workflow failed
    public var error: String?
    /// Serialized output data (if completed)
    public var output: ByteBuffer?

    internal init(
        id: WorkflowID,
        workflowType: String,
        input: ByteBuffer,
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
        self.activityResults = [:]
        self.endTime = nil
        self.error = nil
        self.output = nil
    }
}

/// Tracks execution of individual workflow steps
public struct WorkflowStepExecution: Codable, Sendable {
    /// Step index in the workflow
    public let stepIndex: Int
    /// Type of step (activity, parallel, condition, etc.)
    public let stepType: WorkflowStepType
    /// When this step started
    public let startTime: Date
    /// When this step completed (if applicable)
    public var endTime: Date?
    /// Step execution status
    public var status: WorkflowStepStatus
    /// Activity ID for activity steps
    public let activityId: String?
    /// Error if step failed
    public var error: String?

    public init(
        stepIndex: Int,
        stepType: WorkflowStepType,
        startTime: Date = .now,
        activityId: String? = nil
    ) {
        self.stepIndex = stepIndex
        self.stepType = stepType
        self.startTime = startTime
        self.endTime = nil
        self.status = .running
        self.activityId = activityId
        self.error = nil
    }
}

/// Types of workflow steps
public enum WorkflowStepType: String, Codable, Sendable {
    case activity
    case parallel
    case condition
    case delay
    case signal
}

/// Status of individual workflow steps
public enum WorkflowStepStatus: String, Codable, Sendable {
    case running
    case completed
    case failed
    case cancelled
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
    /// Activity result data (if continuing from activity)
    public let activityResult: ByteBuffer?
    /// Activity results for resumption
    public let activityResults: [String: ByteBuffer]

    public init(
        workflowId: WorkflowID,
        workflowType: String,
        action: WorkflowAction,
        stepIndex: Int,
        activityResult: ByteBuffer? = nil,
        activityResults: [String: ByteBuffer] = [:]
    ) {
        self.workflowId = workflowId
        self.workflowType = workflowType
        self.action = action
        self.stepIndex = stepIndex
        self.activityResult = activityResult
        self.activityResults = activityResults
    }
}

/// Actions that the workflow coordinator can perform
public enum WorkflowAction: String, Codable, Sendable {
    /// Start a new workflow execution
    case start
    /// Continue execution after an activity completes
    case continueFromActivity
    /// Execute a parallel step
    case executeParallelStep
    /// Mark workflow as completed
    case complete
    /// Mark workflow as failed
    case fail
    /// Cancel a running workflow
    case cancel
}

/// Job that executes an activity within a workflow
public struct ActivityExecutionJob: JobParameters {
    public static let jobName = "ActivityExecution"

    /// Unique identifier for this activity execution
    public let activityId: String
    /// The workflow this activity belongs to
    public let workflowId: WorkflowID
    /// Name of the activity to execute
    public let activityName: String
    /// Serialized activity parameters
    public let parameters: ByteBuffer
    /// Timeout for activity execution
    public let timeout: Duration?
    /// Serialized retry policy
    public let retryPolicy: ByteBuffer?
    /// Workflow type for continuation after activity completion
    public let workflowType: String
    /// Current step index for continuation
    public let stepIndex: Int

    internal init(
        activityId: String,
        workflowId: WorkflowID,
        activityName: String,
        parameters: ByteBuffer,
        timeout: Duration? = nil,
        retryPolicy: ByteBuffer? = nil,
        workflowType: String,
        stepIndex: Int
    ) {
        self.activityId = activityId
        self.workflowId = workflowId
        self.activityName = activityName
        self.parameters = parameters
        self.timeout = timeout
        self.retryPolicy = retryPolicy
        self.workflowType = workflowType
        self.stepIndex = stepIndex
    }
}

/// Result of an activity execution
public enum ActivityResult<T: Codable & Sendable>: Codable, Sendable {
    case success(T)
    case failure(String)
}

/// Type-erased activity result for storage
public struct AnyActivityResult: Codable, Sendable {
    public let isSuccess: Bool
    public let data: ByteBuffer

    public init<T: Codable & Sendable>(success value: T) throws {
        self.isSuccess = true
        self.data = try JSONEncoder().encodeAsByteBuffer(value, allocator: ByteBufferAllocator())
    }

    public init(failure error: String) throws {
        self.isSuccess = false
        self.data = try JSONEncoder().encodeAsByteBuffer(error, allocator: ByteBufferAllocator())
    }

    public func decode<T: Codable & Sendable>(as type: T.Type) throws -> ActivityResult<T> {
        if isSuccess {
            let value = try JSONDecoder().decode(T.self, from: data)
            return .success(value)
        } else {
            let error = try JSONDecoder().decode(String.self, from: data)
            return .failure(error)
        }
    }
}

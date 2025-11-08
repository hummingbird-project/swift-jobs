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

/// Base protocol for all workflow definitions
public protocol WorkflowProtocol: Sendable {
    /// Input type for the workflow
    associatedtype Input: Codable & Sendable
    /// Output type for the workflow
    associatedtype Output: Codable & Sendable

    /// Unique name for this workflow type
    /// Defaults to the type name (e.g., "OrderProcessingWorkflow")
    static var workflowName: String { get }

    /// Initialize workflow instance
    ///
    /// Required for the factory pattern. Most workflows are stateless structs
    /// and can implement this as: `init() {}`, but it's not required in your implementation.
    init()

    /// Execute the workflow with the given input
    /// - Parameters:
    ///   - input: The workflow input parameters
    ///   - context: The workflow execution context
    /// - Returns: The workflow output
    func run(input: Input, context: WorkflowExecutionContext) async throws -> Output
}

/// Status of a workflow execution
public enum WorkflowStatus: String, Codable, Sendable {
    case queued
    case running
    case completed
    case failed
    case cancelled
}

/// Options for workflow execution
public struct WorkflowOptions: Sendable {
    /// Maximum time the workflow can run before timing out
    public let timeout: Duration?
    /// Custom retry strategy for the workflow
    public let retryPolicy: (any JobRetryStrategy)?
    /// Custom workflow ID prefix
    public let workflowIdPrefix: String?

    public init(
        timeout: Duration? = nil,
        retryPolicy: (any JobRetryStrategy)? = nil,
        workflowIdPrefix: String? = nil
    ) {
        self.timeout = timeout
        self.retryPolicy = retryPolicy
        self.workflowIdPrefix = workflowIdPrefix
    }
}

/// Public workflow execution status information
public struct WorkflowExecutionStatus<Input: Codable & Sendable, Output: Codable & Sendable>: Sendable {
    /// Unique workflow execution identifier
    public let id: WorkflowID
    /// Type name of the workflow
    public let workflowType: String
    /// Current execution status
    public let status: WorkflowStatus
    /// When the workflow started
    public let startTime: Date
    /// When the workflow completed (if applicable)
    public let endTime: Date?
    /// Current step in the workflow
    public let currentStep: Int
    /// Error message if workflow failed
    public let error: String?
    /// Whether the workflow has output available
    public let hasOutput: Bool
    /// Serialized output data (if available)
    public let output: Output?

    internal init(from workflowState: WorkflowState<Input, Output>) {
        self.id = workflowState.id
        self.workflowType = workflowState.workflowType
        self.status = workflowState.status
        self.startTime = workflowState.startTime
        self.endTime = workflowState.endTime
        self.currentStep = workflowState.currentStep
        self.error = workflowState.error
        self.hasOutput = workflowState.output != nil
        self.output = workflowState.output
    }
}

/// Errors that can occur during workflow execution
public enum WorkflowError: Error {
    case unknownWorkflowType(String)
    case executionNotFound(WorkflowID)
    case invalidInputType
    case activityFailed(String)
    case activityTimeout(String)
    case activityNotCompleted
    case unknownActivity(String)
    case workflowTimedOut
    case workflowCancelled(WorkflowID)
    case workflowFailed(String)
    case noOutput(WorkflowID)
    case unexpectedStatus(WorkflowID, WorkflowStatus)
    case invalidOperation(String)
    case timeout(WorkflowID, Duration)
    case validationFailed(String)
    case invalidActivityInput
    case invalidActivityOutput
}

/// Error thrown when workflow or activity is cancelled
public struct WorkflowCancelledFailure: Error, Sendable {
    public let message: String
    public let workflowId: WorkflowID?

    public init(message: String = "Operation was cancelled", workflowId: WorkflowID? = nil) {
        self.message = message
        self.workflowId = workflowId
    }
}

/// Error wrapper for activity failures with underlying cause
public struct ActivityFailure: Error, Sendable {
    public let activityName: String
    public let cause: Error

    public init(activityName: String, cause: Error) {
        self.activityName = activityName
        self.cause = cause
    }
}

/// Error wrapper for child workflow failures with underlying cause
public struct ChildWorkflowFailure: Error, Sendable {
    public let workflowName: String
    public let workflowId: WorkflowID
    public let cause: Error

    public init(workflowName: String, workflowId: WorkflowID, cause: Error) {
        self.workflowName = workflowName
        self.workflowId = workflowId
        self.cause = cause
    }
}

extension WorkflowError: LocalizedError {
    public var errorDescription: String? {
        switch self {
        case .unknownWorkflowType(let type):
            return "Unknown workflow type: \(type)"
        case .executionNotFound(let workflowId):
            return "Workflow execution not found: \(workflowId)"
        case .invalidInputType:
            return "Invalid input type for workflow"
        case .activityFailed(let error):
            return "Activity failed: \(error)"
        case .activityTimeout(let activityId):
            return "Activity timed out: \(activityId)"
        case .activityNotCompleted:
            return "Activity not completed"
        case .unknownActivity(let name):
            return "Unknown activity: \(name)"
        case .workflowTimedOut:
            return "Workflow execution timed out"
        case .workflowCancelled(let workflowId):
            return "Workflow was cancelled: \(workflowId)"
        case .workflowFailed(let error):
            return "Workflow failed: \(error)"
        case .noOutput(let workflowId):
            return "No output available for workflow: \(workflowId)"
        case .unexpectedStatus(let workflowId, let status):
            return "Unexpected workflow status \(status) for workflow: \(workflowId)"
        case .invalidOperation(let message):
            return "Invalid operation: \(message)"
        case .timeout(let workflowId, let duration):
            return "Workflow \(workflowId) timed out after \(duration)"
        case .validationFailed(let message):
            return "Workflow validation failed: \(message)"
        case .invalidActivityInput:
            return "Invalid activity input type"
        case .invalidActivityOutput:
            return "Invalid activity output type"
        }
    }
}

// MARK: - Default Implementation

extension WorkflowProtocol {
    /// Default implementation uses the type name as workflow name
    /// This can be overridden by implementing workflowName explicitly
    public static var workflowName: String {
        String(describing: Self.self)
    }
}

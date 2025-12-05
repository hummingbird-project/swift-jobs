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

// MARK: - Workflow Definition Repository

/// Repository for managing workflow definitions and versions
public protocol WorkflowDefinitionRepository: Sendable {

    /// Register a workflow definition
    /// - Parameter definition: The workflow definition to register
    func registerWorkflow(_ definition: WorkflowDefinition) async throws

    /// Get workflow definition by name
    /// - Parameter name: Workflow name
    /// - Returns: Workflow definition if found
    func getWorkflow(name: String) async throws -> WorkflowDefinition?

    /// List all registered workflows
    /// - Returns: Array of workflow definitions
    func listWorkflows() async throws -> [WorkflowDefinition]

    /// Delete workflow definition
    /// - Parameter name: Workflow name to delete
    func deleteWorkflow(name: String) async throws
}

// MARK: - Workflow Execution Repository

/// Repository for managing workflow executions (runs)
public protocol WorkflowExecutionRepository: Sendable {

    /// Create a new workflow execution
    /// - Parameter execution: The workflow execution to create
    func createExecution(_ execution: WorkflowExecution) async throws

    /// Get workflow execution by ID
    /// - Parameter id: Workflow execution ID
    /// - Returns: Workflow execution if found
    func getExecution(_ id: WorkflowID) async throws -> WorkflowExecution?

    /// Get typed workflow run by ID
    /// - Parameters:
    ///   - id: Workflow execution ID
    ///   - inputType: Type of workflow input
    ///   - outputType: Type of workflow output
    /// - Returns: Typed workflow run if found
    func getWorkflowRun<Input: Codable & Sendable, Output: Codable & Sendable>(
        _ id: WorkflowID,
        inputType: Input.Type,
        outputType: Output.Type
    ) async throws -> WorkflowRun<Input, Output>?

    /// Update workflow execution
    /// - Parameter execution: Updated workflow execution
    func updateExecution(_ execution: WorkflowExecution) async throws

    /// Delete workflow execution
    /// - Parameter id: Workflow execution ID
    func deleteExecution(_ id: WorkflowID) async throws

    /// List workflow executions with filtering
    /// - Parameter options: Filtering and pagination options
    /// - Returns: List of workflow executions
    func listExecutions(_ options: ListWorkflowExecutionsOptions) async throws -> WorkflowExecutionList

    /// Count executions by status
    /// - Parameter status: Workflow status to count
    /// - Returns: Number of executions with the given status
    func countExecutionsByStatus(_ status: WorkflowStatus) async throws -> Int

}

// MARK: - Workflow Step Repository

/// Repository for managing workflow step executions
public protocol WorkflowStepRepository: Sendable {

    /// Save workflow step execution
    /// - Parameter step: The step execution to save
    func saveStep(_ step: WorkflowStepExecution) async throws

    /// Get workflow step by workflow ID and step index
    /// - Parameters:
    ///   - workflowId: Workflow execution ID
    ///   - stepIndex: Step index
    /// - Returns: Step execution if found
    func getStep(_ workflowId: WorkflowID, stepIndex: Int) async throws -> WorkflowStepExecution?

    /// Get all steps for a workflow execution
    /// - Parameter workflowId: Workflow execution ID
    /// - Returns: Array of step executions ordered by step index
    func getSteps(_ workflowId: WorkflowID) async throws -> [WorkflowStepExecution]

    /// Update workflow step
    /// - Parameter step: Updated step execution
    func updateStep(_ step: WorkflowStepExecution) async throws

    /// Delete all steps for a workflow execution
    /// - Parameter workflowId: Workflow execution ID
    func deleteSteps(_ workflowId: WorkflowID) async throws
}

// MARK: - Activity Result Repository

/// Repository for managing activity execution results
public protocol ActivityResultRepository: Sendable {

    /// Save activity result
    /// - Parameters:
    ///   - activityId: Unique activity execution ID
    ///   - result: The activity result to save
    func saveResult<Result: Codable & Sendable>(
        _ activityId: ActivityID,
        result: ActivityResult<Result>
    ) async throws

    /// Get activity result by ID
    /// - Parameters:
    ///   - activityId: Activity execution ID
    ///   - resultType: Type of the expected result
    /// - Returns: Activity result if found
    func getResult<Result: Codable & Sendable>(
        _ activityId: ActivityID,
        resultType: Result.Type
    ) async throws -> ActivityResult<Result>?

    /// Delete activity result
    /// - Parameter activityId: Activity execution ID
    func deleteResult(_ activityId: ActivityID) async throws

    /// Clean up activity results older than specified date
    /// - Parameter before: Delete results older than this date
    /// - Returns: Number of results deleted
    func cleanupResults(before: Date) async throws -> Int
}

// MARK: - Workflow History Repository

/// Repository for managing workflow execution history and events
public protocol WorkflowHistoryRepository: Sendable {

    /// Record a workflow execution event
    /// - Parameter event: The workflow event to record
    func recordWorkflowEvent(_ event: WorkflowExecutionEvent) async throws

    /// Record a step execution event
    /// - Parameter event: The step event to record
    func recordStepEvent(_ event: WorkflowStepEvent) async throws

    /// Get workflow execution history
    /// - Parameters:
    ///   - workflowId: Workflow execution ID
    ///   - options: History query options
    /// - Returns: List of workflow events
    func getWorkflowHistory(_ workflowId: WorkflowID, options: WorkflowHistoryOptions) async throws -> [WorkflowExecutionEvent]

    /// Get step execution history
    /// - Parameters:
    ///   - workflowId: Workflow execution ID
    ///   - stepIndex: Optional step index filter
    ///   - options: History query options
    /// - Returns: List of step events
    func getStepHistory(_ workflowId: WorkflowID, stepIndex: Int?, options: WorkflowHistoryOptions) async throws -> [WorkflowStepEvent]

    /// Archive completed step results for long-term storage
    /// - Parameters:
    ///   - workflowId: Workflow execution ID
    ///   - stepIndex: Step index
    ///   - result: Step execution result to archive
    func archiveStepResult(_ workflowId: WorkflowID, stepIndex: Int, result: WorkflowStepExecution) async throws

    /// Get archived step results
    /// - Parameters:
    ///   - workflowId: Workflow execution ID
    ///   - stepIndex: Step index
    /// - Returns: Archived step result if found
    func getArchivedStepResult(_ workflowId: WorkflowID, stepIndex: Int) async throws -> WorkflowStepExecution?

    /// Clean up history events older than specified date
    /// - Parameter before: Delete events older than this date
    /// - Returns: Number of events deleted
    func cleanupHistoryEvents(before: Date) async throws -> Int

    /// Clean up archived results older than specified date
    /// - Parameter before: Delete archived results older than this date
    /// - Returns: Number of archived results deleted
    func cleanupArchivedResults(before: Date) async throws -> Int
}

// MARK: - Combined Workflow Repository

/// Combined repository interface for all workflow-related operations
public protocol WorkflowRepository: WorkflowDefinitionRepository, WorkflowExecutionRepository, WorkflowStepRepository, ActivityResultRepository,
    WorkflowHistoryRepository
{

    /// Clean up completed workflows and their associated data
    /// - Parameter before: Clean up workflows completed before this date
    /// - Returns: Number of workflows cleaned up
    func cleanupCompletedWorkflows(before: Date) async throws -> Int

    /// Clean up failed workflows and their associated data
    /// - Parameter before: Clean up workflows that failed before this date
    /// - Returns: Number of workflows cleaned up
    func cleanupFailedWorkflows(before: Date) async throws -> Int

    // MARK: - Type-Erased Operations for Dynamic Workflow Handling

    /// Get workflow execution summary without requiring compile-time types
    /// - Parameter workflowId: Workflow execution ID
    /// - Returns: Workflow execution summary if found
    func getExecutionSummary(_ workflowId: WorkflowID) async throws -> WorkflowExecutionSummary?

    /// Update workflow execution status without requiring types
    /// - Parameters:
    ///   - workflowId: Workflow execution ID
    ///   - status: New workflow status
    ///   - startTime: Optional start time if transitioning to running
    ///   - endTime: Optional end time if completing
    ///   - error: Optional error message if failing
    func updateExecutionStatus(
        _ workflowId: WorkflowID,
        status: WorkflowStatus,
        startTime: Date?,
        endTime: Date?,
        error: String?
    ) async throws

}

// MARK: - Supporting Data Structures

/// Workflow definition containing metadata and configuration
public struct WorkflowDefinition: Codable, Sendable {
    /// Unique workflow name
    public let name: String
    /// Human-readable description
    public let description: String?
    /// Workflow version
    public let version: String
    /// When this definition was created
    public let createdAt: Date
    /// Whether this workflow is active
    public var isActive: Bool
    /// Timeout for the entire workflow
    public let timeout: Duration?
    /// Retry policy for the workflow
    public let retryPolicy: String?
    /// Additional metadata
    public let metadata: [String: String]

    public init(
        name: String,
        description: String? = nil,
        version: String = "1.0.0",
        createdAt: Date = .now,
        isActive: Bool = true,
        timeout: Duration? = nil,
        retryPolicy: String? = nil,
        metadata: [String: String] = [:]
    ) {
        self.name = name
        self.description = description
        self.version = version
        self.createdAt = createdAt
        self.isActive = isActive
        self.timeout = timeout
        self.retryPolicy = retryPolicy
        self.metadata = metadata
    }
}

/// Typed workflow run model for SDK/API use
public struct WorkflowRun<Input: Codable & Sendable, Output: Codable & Sendable>: Sendable {
    /// Unique execution identifier
    public let id: WorkflowID
    /// Name of the workflow definition
    public let workflowName: String
    /// Workflow input data (typed)
    public let input: Input
    /// Workflow output data (typed, if completed successfully)
    public var output: Output?
    /// Current execution status
    public var status: WorkflowStatus
    /// When the execution was created
    public let createdAt: Date
    /// When the execution was queued
    public let queuedAt: Date
    /// When the execution started
    public var startedAt: Date?
    /// When the execution completed
    public var completedAt: Date?
    /// When the execution failed
    public var failedAt: Date? {
        status == .failed ? completedAt : nil
    }
    /// Current step index
    public var currentStep: Int
    /// Total number of steps
    public var totalSteps: Int
    /// Error message if execution failed
    public var error: String?
    /// Parent workflow run ID (if this is a child workflow)
    public let parentId: WorkflowID?
    /// Parent step run ID that spawned this child workflow
    public let parentStepRunId: String?
    /// Child key identifier for this workflow run
    public let childKey: String?
    /// Child index if there are multiple child workflows
    public let childIndex: Int?
    /// Additional metadata for this execution
    public var metadata: [String: String]

    /// Computed property to check if workflow has output
    public var hasOutput: Bool {
        output != nil
    }

    public init(
        id: WorkflowID,
        workflowName: String,
        input: Input,
        output: Output? = nil,
        status: WorkflowStatus = .queued,
        createdAt: Date = .now,
        queuedAt: Date = .now,
        startedAt: Date? = nil,
        completedAt: Date? = nil,
        currentStep: Int = 0,
        totalSteps: Int = 0,
        error: String? = nil,
        parentId: WorkflowID? = nil,
        parentStepRunId: String? = nil,
        childKey: String? = nil,
        childIndex: Int? = nil,
        metadata: [String: String] = [:]
    ) {
        self.id = id
        self.workflowName = workflowName
        self.input = input
        self.output = output
        self.status = status
        self.createdAt = createdAt
        self.queuedAt = queuedAt
        self.startedAt = startedAt
        self.completedAt = completedAt
        self.currentStep = currentStep
        self.totalSteps = totalSteps
        self.error = error
        self.parentId = parentId
        self.parentStepRunId = parentStepRunId
        self.childKey = childKey
        self.childIndex = childIndex
        self.metadata = metadata
    }
}

/// Workflow execution storage model (type-erased with ByteBuffer data)
public struct WorkflowExecution: Codable, Sendable {
    /// Unique execution identifier
    public let id: WorkflowID
    /// Name of the workflow definition
    public let workflowName: String
    /// Workflow input data as ByteBuffer
    public let inputData: ByteBuffer
    /// Current workflow state as ByteBuffer (optional if not set initially)
    public var workflowState: ByteBuffer?
    /// Current execution status
    public var status: WorkflowStatus
    /// When the execution was created
    public let createdAt: Date
    /// When the execution was queued
    public let queuedAt: Date
    /// When the execution was scheduled to run (if it's a scheduled workflow)
    public let scheduledAt: Date?
    /// When the execution started
    public var startedAt: Date?
    /// When the execution completed
    public var completedAt: Date?
    /// Current step index
    public var currentStep: Int
    /// Total number of steps
    public var totalSteps: Int
    /// Error message if execution failed
    public var error: String?
    /// Parent workflow run ID (if this is a child workflow)
    public let parentId: WorkflowID?
    /// Parent step run ID that spawned this child workflow
    public let parentStepRunId: String?
    /// Child key identifier for this workflow run
    public let childKey: String?
    /// Child index if there are multiple child workflows
    public let childIndex: Int?
    /// Execution output data as ByteBuffer (if completed successfully)
    public var result: ByteBuffer?
    /// Additional metadata for this execution
    public var metadata: [String: String]

    /// Alias for result to match test expectations
    public var output: ByteBuffer? {
        get { result }
        set { result = newValue }
    }

    /// Computed property to check if workflow has output
    public var hasOutput: Bool {
        result != nil
    }

    /// When the execution failed
    public var failedAt: Date? {
        status == .failed ? completedAt : nil
    }

    public init(
        id: WorkflowID,
        workflowName: String,
        inputData: ByteBuffer,
        workflowState: ByteBuffer? = nil,
        status: WorkflowStatus,
        createdAt: Date,
        queuedAt: Date,
        scheduledAt: Date? = nil,
        startedAt: Date? = nil,
        completedAt: Date? = nil,
        currentStep: Int = 0,
        totalSteps: Int = 0,
        error: String? = nil,
        parentId: WorkflowID? = nil,
        parentStepRunId: String? = nil,
        childKey: String? = nil,
        childIndex: Int? = nil,
        result: ByteBuffer? = nil,
        metadata: [String: String] = [:]
    ) {
        self.id = id
        self.workflowName = workflowName
        self.inputData = inputData
        self.workflowState = workflowState
        self.status = status
        self.createdAt = createdAt
        self.queuedAt = queuedAt
        self.scheduledAt = scheduledAt
        self.startedAt = startedAt
        self.completedAt = completedAt
        self.currentStep = currentStep
        self.totalSteps = totalSteps
        self.error = error
        self.parentId = parentId
        self.parentStepRunId = parentStepRunId
        self.childKey = childKey
        self.childIndex = childIndex
        self.result = result
        self.metadata = metadata
    }
}

/// Options for listing workflow executions
public struct ListWorkflowExecutionsOptions: Sendable {
    /// Filter by workflow name
    public let workflowName: String?
    /// Filter by status
    public let status: WorkflowStatus?
    /// Filter executions created after this date
    public let createdAfter: Date?
    /// Filter executions created before this date
    public let createdBefore: Date?
    /// Maximum number of results to return
    public let limit: Int?
    /// Number of results to skip
    public let offset: Int?
    /// Sort order
    public let orderBy: ExecutionOrderBy
    /// Sort direction
    public let orderDirection: OrderDirection

    public init(
        workflowName: String? = nil,
        status: WorkflowStatus? = nil,
        createdAfter: Date? = nil,
        createdBefore: Date? = nil,
        limit: Int? = nil,
        offset: Int? = nil,
        orderBy: ExecutionOrderBy = .createdAt,
        orderDirection: OrderDirection = .descending
    ) {
        self.workflowName = workflowName
        self.status = status
        self.createdAfter = createdAfter
        self.createdBefore = createdBefore
        self.limit = limit
        self.offset = offset
        self.orderBy = orderBy
        self.orderDirection = orderDirection
    }
}

/// Result of listing workflow executions
public struct WorkflowExecutionList: Sendable {
    /// Array of execution summaries
    public let executions: [WorkflowExecutionSummary]
    /// Total count without pagination
    public let totalCount: Int

    public init(executions: [WorkflowExecutionSummary], totalCount: Int) {
        self.executions = executions
        self.totalCount = totalCount
    }
}

/// Summary of a workflow execution for list operations
public struct WorkflowExecutionSummary: Codable, Sendable {
    /// Execution ID
    public let id: WorkflowID
    /// Workflow name
    public let workflowName: String
    /// Execution status
    public var status: WorkflowStatus
    /// When created
    public let createdAt: Date
    /// When scheduled to run (if scheduled workflow)
    public let scheduledAt: Date?
    /// When started
    public var startedAt: Date?
    /// When completed
    public var completedAt: Date?
    /// Current step
    public var currentStep: Int
    /// Total steps
    public var totalSteps: Int
    /// Error message if failed
    public var error: String?
    /// Parent workflow ID (if this is a child workflow)
    public let parentId: WorkflowID?

    public init(
        id: WorkflowID,
        workflowName: String,
        status: WorkflowStatus,
        createdAt: Date,
        scheduledAt: Date? = nil,
        startedAt: Date? = nil,
        completedAt: Date? = nil,
        currentStep: Int,
        totalSteps: Int,
        error: String? = nil,
        parentId: WorkflowID? = nil
    ) {
        self.id = id
        self.workflowName = workflowName
        self.status = status
        self.createdAt = createdAt
        self.scheduledAt = scheduledAt
        self.startedAt = startedAt
        self.completedAt = completedAt
        self.currentStep = currentStep
        self.totalSteps = totalSteps
        self.error = error
        self.parentId = parentId
    }
}

/// Order options for execution lists
public enum ExecutionOrderBy: String, Codable, Sendable {
    case createdAt = "created_at"
    case startedAt = "started_at"
    case completedAt = "completed_at"
    case workflowName = "workflow_name"
    case status = "status"
}

/// Sort direction
public enum OrderDirection: String, Codable, Sendable {
    case ascending = "asc"
    case descending = "desc"
}

// MARK: - Workflow History Data Structures

/// Event types for workflow execution tracking
public enum WorkflowExecutionEventType: String, Codable, Sendable {
    case created = "CREATED"
    case queued = "QUEUED"
    case started = "STARTED"
    case paused = "PAUSED"
    case resumed = "RESUMED"
    case completed = "COMPLETED"
    case failed = "FAILED"
    case cancelled = "CANCELLED"
    case timedOut = "TIMED_OUT"
}

/// Event types for workflow step execution tracking
public enum WorkflowStepEventType: String, Codable, Sendable {
    case created = "CREATED"
    case queued = "QUEUED"
    case assigned = "ASSIGNED"
    case started = "STARTED"
    case completed = "COMPLETED"
    case failed = "FAILED"
    case cancelled = "CANCELLED"
    case retrying = "RETRYING"
    case timedOut = "TIMED_OUT"
    case skipped = "SKIPPED"
}

/// Severity levels for workflow events
public enum WorkflowEventSeverity: String, Codable, Sendable {
    case info = "INFO"
    case warning = "WARNING"
    case error = "ERROR"
    case critical = "CRITICAL"
}

/// Workflow execution event for history tracking
public struct WorkflowExecutionEvent: Codable, Sendable {
    /// Unique event ID
    public let id: UUID
    /// Workflow execution ID this event belongs to
    public let workflowId: WorkflowID
    /// Event type
    public let eventType: WorkflowExecutionEventType
    /// Event severity
    public let severity: WorkflowEventSeverity
    /// When the event occurred
    public let timestamp: Date
    /// Optional message describing the event
    public let message: String?
    /// Additional event data
    public let data: [String: String]
    /// Optional error information if this is a failure event
    public let error: String?
    /// Event source (e.g., "workflow-engine", "scheduler", "user")
    public let source: String

    public init(
        id: UUID = UUID(),
        workflowId: WorkflowID,
        eventType: WorkflowExecutionEventType,
        severity: WorkflowEventSeverity = .info,
        timestamp: Date = .now,
        message: String? = nil,
        data: [String: String] = [:],
        error: String? = nil,
        source: String = "workflow-engine"
    ) {
        self.id = id
        self.workflowId = workflowId
        self.eventType = eventType
        self.severity = severity
        self.timestamp = timestamp
        self.message = message
        self.data = data
        self.error = error
        self.source = source
    }
}

/// Workflow step execution event for history tracking
public struct WorkflowStepEvent: Codable, Sendable {
    /// Unique event ID
    public let id: UUID
    /// Workflow execution ID this step belongs to
    public let workflowId: WorkflowID
    /// Step index in the workflow
    public let stepIndex: Int
    /// Step name/identifier
    public let stepName: String
    /// Event type
    public let eventType: WorkflowStepEventType
    /// Event severity
    public let severity: WorkflowEventSeverity
    /// When the event occurred
    public let timestamp: Date
    /// Optional message describing the event
    public let message: String?
    /// Additional event data
    public let data: [String: String]
    /// Optional error information if this is a failure event
    public let error: String?
    /// Retry attempt number (0 for first attempt)
    public let retryAttempt: Int
    /// Worker ID that processed this step (if applicable)
    public let workerId: String?
    /// Event source (e.g., "workflow-engine", "worker", "scheduler")
    public let source: String

    public init(
        id: UUID = UUID(),
        workflowId: WorkflowID,
        stepIndex: Int,
        stepName: String,
        eventType: WorkflowStepEventType,
        severity: WorkflowEventSeverity = .info,
        timestamp: Date = .now,
        message: String? = nil,
        data: [String: String] = [:],
        error: String? = nil,
        retryAttempt: Int = 0,
        workerId: String? = nil,
        source: String = "workflow-engine"
    ) {
        self.id = id
        self.workflowId = workflowId
        self.stepIndex = stepIndex
        self.stepName = stepName
        self.eventType = eventType
        self.severity = severity
        self.timestamp = timestamp
        self.message = message
        self.data = data
        self.error = error
        self.retryAttempt = retryAttempt
        self.workerId = workerId
        self.source = source
    }
}

/// Options for querying workflow history
public struct WorkflowHistoryOptions: Sendable {
    /// Filter by event types
    public let eventTypes: [String]?
    /// Filter by severity levels
    public let severities: [WorkflowEventSeverity]?
    /// Filter events after this timestamp
    public let after: Date?
    /// Filter events before this timestamp
    public let before: Date?
    /// Maximum number of events to return
    public let limit: Int?
    /// Number of events to skip
    public let offset: Int?
    /// Sort order (default: timestamp descending)
    public let orderBy: HistoryOrderBy
    /// Sort direction
    public let orderDirection: OrderDirection

    public init(
        eventTypes: [String]? = nil,
        severities: [WorkflowEventSeverity]? = nil,
        after: Date? = nil,
        before: Date? = nil,
        limit: Int? = nil,
        offset: Int? = nil,
        orderBy: HistoryOrderBy = .timestamp,
        orderDirection: OrderDirection = .descending
    ) {
        self.eventTypes = eventTypes
        self.severities = severities
        self.after = after
        self.before = before
        self.limit = limit
        self.offset = offset
        self.orderBy = orderBy
        self.orderDirection = orderDirection
    }
}

/// Order options for history queries
public enum HistoryOrderBy: String, Codable, Sendable {
    case timestamp = "timestamp"
    case eventType = "event_type"
    case severity = "severity"
    case stepIndex = "step_index"
}

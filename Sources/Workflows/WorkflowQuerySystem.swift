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
import Logging
import NIOCore
import Synchronization

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Protocol for workflows that support queries
/// Must be a class to allow AnyObject storage for live state access
public protocol WorkflowQuery: WorkflowProtocol, AnyObject {
    /// The output type for queries on this workflow
    associatedtype QueryOutput: Codable & Sendable

    /// Handle query requests - framework calls this with the workflow instance
    func handleQuery() throws -> QueryOutput
}

/// Protocol for workflows that support updates
/// Must be a class to allow AnyObject storage for live state access
public protocol WorkflowUpdate: WorkflowProtocol, AnyObject {
    /// The input type for updates on this workflow
    associatedtype UpdateInput: Codable & Sendable
    /// The output type for updates on this workflow
    associatedtype UpdateOutput: Codable & Sendable

    /// Handle update requests - framework calls this with the workflow instance and input
    func handleUpdate(input: UpdateInput) async throws -> UpdateOutput
}

/// Protocol for workflows that support both queries and updates
public protocol WorkflowQueryUpdate: WorkflowQuery & WorkflowUpdate {}

/// Empty input type for queries that don't need parameters
public struct EmptyInput: Codable, Sendable {
    public init() {}
}

// MARK: - Query and Update Registry

/// Registry for managing query and update handlers in a workflow execution context
internal final class WorkflowQueryRegistry: Sendable {
    private let logger: Logger

    internal init(logger: Logger) {
        self.logger = logger
    }

    private let _workflowInstances = Mutex<[String: AnyObject]>([:])

    /// Register a workflow instance for protocol-based queries and updates
    internal func registerWorkflowInstance<W: WorkflowProtocol & AnyObject>(_ workflowInstance: W) {
        // Store the workflow instance for direct method calls
        _workflowInstances.withLock { instances in
            instances[W.workflowName] = workflowInstance
        }

        logger.debug(
            "Registered workflow instance",
            metadata: [
                "workflowType": .string(W.workflowName),
                "supportsQueries": .stringConvertible(workflowInstance is any WorkflowQuery),
                "supportsUpdates": .stringConvertible(workflowInstance is any WorkflowUpdate),
            ]
        )
    }

    /// Get workflow instance for direct method calls
    internal func getWorkflowInstance<W: WorkflowProtocol>(workflowType: W.Type) -> W? {
        _workflowInstances.withLock { instances in
            instances[W.workflowName] as? W
        }
    }
}

// MARK: - Global Active Workflow Registry

/// Global registry to track active workflows for query routing
internal final class ActiveWorkflowRegistry: Sendable {
    private let _activeWorkflows = Mutex<[String: WorkflowQueryRegistry]>([:])

    static let shared = ActiveWorkflowRegistry()

    func registerWorkflow(_ workflowId: WorkflowID, queryRegistry: WorkflowQueryRegistry) {
        _activeWorkflows.withLock { workflows in
            workflows[workflowId.value] = queryRegistry
        }
    }

    func unregisterWorkflow(_ workflowId: WorkflowID) {
        _ = _activeWorkflows.withLock { workflows in
            workflows.removeValue(forKey: workflowId.value)
        }
    }

    func getWorkflowRegistry(_ workflowId: WorkflowID) -> WorkflowQueryRegistry? {
        _activeWorkflows.withLock { workflows in
            workflows[workflowId.value]
        }
    }
}

// MARK: - WorkflowExecutionContext Extensions

extension WorkflowExecutionContext {
    /// Register workflow instance for queries and updates
    public func registerWorkflowInstance<W: WorkflowProtocol & AnyObject>(_ workflowInstance: W) {
        // Register workflow instance for direct method calls
        queryRegistry.registerWorkflowInstance(workflowInstance)

        // Register this workflow's query registry globally for external access
        ActiveWorkflowRegistry.shared.registerWorkflow(workflowId, queryRegistry: queryRegistry)
    }
}

// MARK: - WorkflowEngine Query Extensions

extension WorkflowEngine {

    /// Execute a query on a WorkflowQuery using workflow type
    public func query<W: WorkflowQuery>(
        workflowId: WorkflowID,
        workflowType: W.Type
    ) async throws -> W.QueryOutput {
        // Check if workflow execution exists
        guard let execution = try await jobQueue.queue.workflowRepository.getExecution(workflowId) else {
            throw WorkflowError.executionNotFound(workflowId)
        }

        // For running workflows, use direct query execution
        if execution.status == WorkflowStatus.running {
            guard let queryRegistry = ActiveWorkflowRegistry.shared.getWorkflowRegistry(workflowId) else {
                throw WorkflowError.workflowNotAccessible(workflowId)
            }

            // Get the workflow instance and call handleQuery directly
            guard let workflowInstance = queryRegistry.getWorkflowInstance(workflowType: workflowType) else {
                throw WorkflowError.workflowNotAccessible(workflowId)
            }

            let result = try workflowInstance.handleQuery()

            logger.debug(
                "Workflow query executed successfully",
                metadata: [
                    "workflowId": .string(workflowId.value),
                    "workflowType": .string(W.workflowName),
                ]
            )

            return result
        }

        // For completed workflows, allow querying final state
        if [WorkflowStatus.completed, .failed, .cancelled].contains(execution.status) {
            // TODO: Return stored final state from workflow repository
            // For now, indicate that workflow has completed but could be queryable
            logger.debug(
                "Attempting to query completed workflow - final state not yet implemented",
                metadata: [
                    "workflowId": .string(workflowId.value),
                    "status": .string(execution.status.rawValue),
                ]
            )
            throw WorkflowError.workflowNotAccessible(workflowId)
        }

        // Handle queued workflows
        if execution.status == .queued {
            logger.debug(
                "Cannot query queued workflow that hasn't started yet",
                metadata: [
                    "workflowId": .string(workflowId.value)
                ]
            )
            throw WorkflowError.workflowNotStarted(workflowId)
        }

        // Unknown workflow status
        logger.error(
            "Unexpected workflow status in query",
            metadata: [
                "workflowId": .string(workflowId.value),
                "status": .string(execution.status.rawValue),
            ]
        )
        throw WorkflowError.workflowFailed("Unknown workflow status: \(execution.status)")
    }

    /// Execute an update on a WorkflowUpdate using workflow type
    public func update<W: WorkflowUpdate>(
        workflowId: WorkflowID,
        workflowType: W.Type,
        input: W.UpdateInput
    ) async throws -> W.UpdateOutput {
        // Check if workflow execution exists and is in a state that allows updates
        guard let execution = try await jobQueue.queue.workflowRepository.getExecution(workflowId) else {
            throw WorkflowError.executionNotFound(workflowId)
        }

        // Updates can only be executed on running or queued workflows
        guard [WorkflowStatus.running, WorkflowStatus.queued].contains(execution.status) else {
            throw WorkflowError.workflowNotAccessible(workflowId)
        }

        guard let queryRegistry = ActiveWorkflowRegistry.shared.getWorkflowRegistry(workflowId) else {
            throw WorkflowError.workflowNotAccessible(workflowId)
        }

        // Get the workflow instance and call handleUpdate directly
        guard let workflowInstance = queryRegistry.getWorkflowInstance(workflowType: workflowType) else {
            throw WorkflowError.workflowNotAccessible(workflowId)
        }

        let result = try await workflowInstance.handleUpdate(input: input)

        // Trigger condition re-evaluation after update execution
        WorkflowConditionRegistry.shared.evaluateConditions(for: workflowId)

        logger.debug(
            "Workflow update executed successfully",
            metadata: [
                "workflowId": .string(workflowId.value),
                "workflowType": .string(W.workflowName),
            ]
        )

        return result
    }

    /// Internal cleanup when workflow completes
    internal func cleanupWorkflowQueries(_ workflowId: WorkflowID) {
        ActiveWorkflowRegistry.shared.unregisterWorkflow(workflowId)
        Task {
            await WorkflowConditionRegistry.shared.cleanup(workflowId: workflowId)
        }
    }

}

// MARK: - Error Extensions

extension WorkflowError {
    /// Error indicating an unknown query
    public static func unknownQuery(_ queryName: String) -> WorkflowError {
        .workflowFailed("Unknown query: \(queryName)")
    }

    /// Error when query handler throws an exception
    public static func queryHandlerFailed(_ queryName: String, _ error: String) -> WorkflowError {
        .workflowFailed("Query handler '\(queryName)' failed: \(error)")
    }

    /// Error when workflow is not accessible for queries
    public static func workflowNotAccessible(_ workflowId: WorkflowID) -> WorkflowError {
        .workflowFailed("Workflow \(workflowId.value) is not accessible for queries")
    }

    /// Error when trying to query a terminated workflow
    public static func workflowTerminated(_ workflowId: WorkflowID) -> WorkflowError {
        .workflowFailed("Cannot query terminated workflow: \(workflowId.value)")
    }

    /// Error when trying to query a workflow that hasn't started yet
    public static func workflowNotStarted(_ workflowId: WorkflowID) -> WorkflowError {
        .workflowFailed("Cannot query workflow that hasn't started: \(workflowId.value)")
    }

    /// Error when trying to query a completed workflow
    public static func workflowCompleted(_ workflowId: WorkflowID) -> WorkflowError {
        .workflowFailed("Cannot query completed workflow: \(workflowId.value)")
    }

    /// Error indicating an unknown update
    public static func unknownUpdate(_ updateName: String) -> WorkflowError {
        .workflowFailed("Unknown update: \(updateName)")
    }

    /// Error when update handler throws an exception
    public static func updateHandlerFailed(_ updateName: String, _ error: String) -> WorkflowError {
        .workflowFailed("Update handler '\(updateName)' failed: \(error)")
    }

}

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
    /// Register multiple  query handlers
    func registerQueries(with registry: WorkflowQueryMethodRegistry)
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

///
/// Query names are automatically derived from the struct name, following the same pattern
/// as Activities and Signals for consistency.
///
/// ## Example Usage:
/// ```swift
/// // Simple query (no input parameters)
/// struct GetStatusQuery: WorkflowQueryType {
///     typealias Output = StatusResponse
///     // queryName automatically becomes "GetStatusQuery"
/// }
///
/// // Query with input parameters
/// struct GetOrderDetailsQuery: WorkflowQueryType {
///     struct Input: Codable, Sendable {
///         let orderId: String
///     }
///     typealias Output = OrderDetails
///     // queryName automatically becomes "GetOrderDetailsQuery"
/// }
/// ```
public protocol WorkflowQueryType {
    associatedtype Input: Codable & Sendable = EmptyInput
    associatedtype Output: Codable & Sendable

    /// The name of this query type
    /// Defaults to the type name (e.g., "GetStatusQuery")
    static var queryName: String { get }
}

extension WorkflowQueryType {
    public static var queryName: String {
        String(describing: self)
    }
}

// MARK: - Query and Update Registry

/// Registry for managing query and update handlers in a workflow execution context
/// Registry for method-based query handlers
public final class WorkflowQueryMethodRegistry: Sendable {
    private let _queryHandlers = Mutex<[String: @Sendable (Any) throws -> Any]>([:])

    /// Register a query handler
    public func registerQuery<Q: WorkflowQueryType>(
        _ queryType: Q.Type,
        handler: @escaping @Sendable (Q.Input) throws -> Q.Output
    ) {
        let queryName = Q.queryName
        _queryHandlers.withLock { handlers in
            handlers[queryName] = { input in
                if Q.Input.self == EmptyInput.self {
                    // Handle EmptyInput case
                    let emptyInput = EmptyInput() as! Q.Input
                    return try handler(emptyInput)
                } else {
                    guard let typedInput = input as? Q.Input else {
                        throw WorkflowError.validationFailed("Invalid input type for query '\(queryName)'")
                    }
                    return try handler(typedInput)
                }
            }
        }
    }

    /// Execute a  query
    internal func executeQuery<Q: WorkflowQueryType>(
        queryType: Q.Type,
        input: Q.Input
    ) throws -> Q.Output {
        let queryName = Q.queryName
        let handler = try _queryHandlers.withLock { handlers in
            guard let handler = handlers[queryName] else {
                throw WorkflowError.queryNotFound(queryName)
            }
            return handler
        }

        let result = try handler(input as Any)
        guard let typedResult = result as? Q.Output else {
            throw WorkflowError.queryTypeMismatch(queryName)
        }
        return typedResult
    }

}

internal final class WorkflowQueryRegistry: Sendable {
    private let logger: Logger
    private let methodRegistry: WorkflowQueryMethodRegistry

    internal init(logger: Logger) {
        self.logger = logger
        self.methodRegistry = WorkflowQueryMethodRegistry()
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
    /// Get a workflow instance by type
    internal func getWorkflowInstance<W: WorkflowProtocol & AnyObject>(workflowType: W.Type) -> W? {
        _workflowInstances.withLock { instances in
            instances[W.workflowName] as? W
        }
    }

    /// Execute a method-based query on the registered workflow
    internal func executeMethodQuery<Q: WorkflowQueryType>(
        queryType: Q.Type,
        input: Q.Input
    ) throws -> Q.Output {
        try methodRegistry.executeQuery(
            queryType: queryType,
            input: input
        )
    }

    /// Get the method registry for registering queries
    internal var queryMethodRegistry: WorkflowQueryMethodRegistry {
        methodRegistry
    }
}

// MARK: - Convenience Extensions for Query Registration

extension WorkflowQueryMethodRegistry {
    /// Convenience method for registering a query with no input
    public func registerQuery<Q: WorkflowQueryType>(
        _ queryType: Q.Type,
        handler: @escaping @Sendable () throws -> Q.Output
    ) where Q.Input == EmptyInput {
        registerQuery(queryType) { _ in
            try handler()
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
    internal func registerWorkflowInstance<W: WorkflowProtocol & AnyObject>(_ workflowInstance: W) {
        // Register workflow instance for direct method calls
        queryRegistry.registerWorkflowInstance(workflowInstance)

        // If the workflow supports multiple queries, register them
        if let queryWorkflow = workflowInstance as? any WorkflowQuery {
            queryWorkflow.registerQueries(with: queryRegistry.queryMethodRegistry)
        }

        // Register this workflow's query registry globally for external access
        ActiveWorkflowRegistry.shared.registerWorkflow(workflowId, queryRegistry: queryRegistry)
    }
}

// MARK: - WorkflowEngine Query Extensions

extension WorkflowEngine {

    /// Query a running workflow with no input
    public func queryMethod<Q: WorkflowQueryType>(
        workflowId: WorkflowID,
        queryType: Q.Type
    ) async throws -> Q.Output where Q.Input == EmptyInput {
        try await queryMethod(workflowId: workflowId, queryType: queryType, input: EmptyInput())
    }

    /// Query a running workflow method-based queries
    public func queryMethod<Q: WorkflowQueryType>(
        workflowId: WorkflowID,
        queryType: Q.Type,
        input: Q.Input
    ) async throws -> Q.Output {
        // Get workflow execution
        guard let execution = try await jobQueue.queue.workflowRepository.getExecution(workflowId) else {
            throw WorkflowError.executionNotFound(workflowId)
        }

        // Debug: Log current workflow status
        logger.debug(
            "Query attempt - checking workflow status",
            metadata: [
                "workflowId": .string(workflowId.value),
                "status": .string(execution.status.rawValue),
            ]
        )

        // Only allow queries on running workflows for now
        if execution.status == .running {
            // Try to get the query registry for this workflow
            guard let queryRegistry = ActiveWorkflowRegistry.shared.getWorkflowRegistry(workflowId) else {
                throw WorkflowError.workflowNotAccessible(workflowId)
            }

            // Execute a method-based query
            return try queryRegistry.executeMethodQuery(
                queryType: queryType,
                input: input
            )
        }

        // Handle sleeping workflows - allow queries since they have active query handlers
        if execution.status == .sleeping {
            // Try to get the query registry for this workflow
            guard let queryRegistry = ActiveWorkflowRegistry.shared.getWorkflowRegistry(workflowId) else {
                logger.debug(
                    "Cannot query sleeping workflow - query registry not accessible",
                    metadata: [
                        "workflowId": .string(workflowId.value)
                    ]
                )
                throw WorkflowError.workflowNotAccessible(workflowId)
            }

            // Execute a method-based query on sleeping workflow
            return try queryRegistry.executeMethodQuery(
                queryType: queryType,
                input: input
            )
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
        throw WorkflowError.workflowFailed("Unexpected workflow status: \(execution.status)")
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

        // Updates can only be executed on running, queued, or sleeping workflows
        // Sleeping workflows will be woken up by the update
        guard [WorkflowStatus.running, WorkflowStatus.queued, WorkflowStatus.sleeping].contains(execution.status) else {
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

    /// Error when query method is not found
    public static func queryNotFound(_ queryName: String) -> WorkflowError {
        .workflowFailed("Query method '\(queryName)' not found")
    }

    /// Error when query return type doesn't match expected type
    public static func queryTypeMismatch(_ queryName: String) -> WorkflowError {
        .workflowFailed("Query method '\(queryName)' return type mismatch")
    }

    /// Error when attempting to query a sleeping workflow
    public static func workflowSleeping(_ workflowId: WorkflowID) -> WorkflowError {
        .workflowFailed("Cannot query workflow that is sleeping: \(workflowId.value)")
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

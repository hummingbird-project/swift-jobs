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

import Logging
import NIOCore
import Synchronization

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Unified registry for workflow types - handles both execution and storage queries
/// Follows the same pattern as JobRegistry for type-erased operations
public final class WorkflowRegistry: Sendable {

    /// Type-erased workflow execution function
    private typealias WorkflowExecutor = @Sendable (ByteBuffer, WorkflowExecutionContext) async throws -> ByteBuffer

    /// Type-erased execution summary decoder
    private typealias ExecutionSummaryDecoder = @Sendable (ByteBuffer) throws -> WorkflowExecutionSummary

    /// Type-erased workflow execution decoder (for full execution data)
    private typealias ExecutionDecoder = @Sendable (ByteBuffer) throws -> Any

    /// Registered workflow executors by name
    private let workflowExecutors: Mutex<[String: WorkflowExecutor]> = .init([:])

    /// Registered execution summary decoders by name
    private let summaryDecoders: Mutex<[String: ExecutionSummaryDecoder]> = .init([:])

    /// Registered execution decoders by name (for when full typed data is needed)
    private let executionDecoders: Mutex<[String: ExecutionDecoder]> = .init([:])

    private let logger = Logger(label: "WorkflowRegistry")

    public init() {}

    // MARK: - Registration

    /// Register a workflow type for both execution and storage queries
    /// - Parameter workflowType: The workflow type to register
    public func registerWorkflow<W: WorkflowProtocol>(_ workflowType: W.Type) {
        let workflowName = W.workflowName

        // Register workflow executor
        let executor: WorkflowExecutor = { [logger] inputBuffer, context in
            // Decode input
            let input = try JSONDecoder().decode(W.Input.self, from: inputBuffer)

            // Create and run workflow
            let workflow = W.init()

            // Register workflow instance for queries/updates if it supports them
            // Check for WorkflowQueryUpdate first (which covers both WorkflowQuery and WorkflowUpdate)
            if let queryUpdateWorkflow = workflow as? any WorkflowQueryUpdate {
                context.registerWorkflowInstance(queryUpdateWorkflow)
            } else {
                // Handle individual protocols for workflows that only implement one
                if let queryableWorkflow = workflow as? any WorkflowQuery {
                    context.registerWorkflowInstance(queryableWorkflow)
                }
                if let updatableWorkflow = workflow as? any WorkflowUpdate {
                    context.registerWorkflowInstance(updatableWorkflow)
                }
            }

            logger.debug(
                "Executing workflow",
                metadata: [
                    "workflowName": .string(workflowName),
                    "workflowId": .string(context.workflowId.value),
                ]
            )

            // Execute workflow
            let output = try await workflow.run(input: input, context: context)

            // Store the output with proper types
            try await context.storeWorkflowOutput(
                input: input,
                output: output,
                inputType: W.Input.self,
                outputType: W.Output.self
            )

            // Return encoded output
            return try JSONEncoder().encodeAsByteBuffer(output, allocator: ByteBufferAllocator())
        }

        // Register execution summary decoder
        let summaryDecoder: ExecutionSummaryDecoder = { buffer in
            let execution = try JSONDecoder().decode(WorkflowExecution.self, from: buffer)
            return WorkflowExecutionSummary(
                id: execution.id,
                workflowName: workflowName,
                status: execution.status,
                createdAt: execution.createdAt,
                startedAt: execution.startedAt,
                completedAt: execution.completedAt,
                currentStep: execution.currentStep,
                totalSteps: execution.totalSteps,
                error: execution.error,
                parentId: execution.parentId
            )
        }

        // Register full execution decoder
        let executionDecoder: ExecutionDecoder = { buffer in
            try JSONDecoder().decode(WorkflowExecution.self, from: buffer)
        }

        // Store all decoders
        workflowExecutors.withLock { executors in
            precondition(executors[workflowName] == nil, "There is a workflow already registered under name \"\(workflowName)\"")
            executors[workflowName] = executor
        }

        summaryDecoders.withLock { decoders in
            decoders[workflowName] = summaryDecoder
        }

        executionDecoders.withLock { decoders in
            decoders[workflowName] = executionDecoder
        }

        logger.info("Registered Workflow", metadata: ["workflowName": .string(workflowName)])
    }

    // MARK: - Workflow Execution

    /// Execute a workflow by name
    /// - Parameters:
    ///   - workflowName: Name of the workflow type
    ///   - input: Serialized input data
    ///   - context: Workflow execution context
    /// - Returns: Serialized output data
    public func executeWorkflow(
        workflowName: String,
        input: ByteBuffer,
        context: WorkflowExecutionContext
    ) async throws -> ByteBuffer {
        let executor = try workflowExecutors.withLock { executors in
            guard let executor = executors[workflowName] else {
                throw WorkflowError.unknownWorkflowType(workflowName)
            }
            return executor
        }

        return try await executor(input, context)
    }

    // MARK: - Storage Queries

    /// Decode workflow execution to summary without knowing types
    /// - Parameters:
    ///   - buffer: Serialized workflow execution data
    ///   - workflowName: Name of the workflow type
    /// - Returns: Type-erased workflow execution summary
    public func decodeExecutionSummary(
        buffer: ByteBuffer,
        workflowName: String
    ) throws -> WorkflowExecutionSummary {
        let decoder = try summaryDecoders.withLock { decoders in
            guard let decoder = decoders[workflowName] else {
                throw WorkflowError.unknownWorkflowType(workflowName)
            }
            return decoder
        }

        return try decoder(buffer)
    }

    /// Decode workflow execution to typed execution data
    /// - Parameters:
    ///   - buffer: Serialized workflow execution data
    ///   - workflowName: Name of the workflow type
    /// - Returns: Typed workflow execution (as Any - caller must cast)
    public func decodeExecution(
        buffer: ByteBuffer,
        workflowName: String
    ) throws -> Any {
        let decoder = try executionDecoders.withLock { decoders in
            guard let decoder = decoders[workflowName] else {
                throw WorkflowError.unknownWorkflowType(workflowName)
            }
            return decoder
        }

        return try decoder(buffer)
    }

    // MARK: - Utility Methods

    /// Generic encode method for any Codable type to ByteBuffer
    /// - Parameter value: Any Codable & Sendable value
    /// - Returns: ByteBuffer containing encoded value
    public func encode<T: Codable & Sendable>(_ value: T) throws -> ByteBuffer {
        do {
            let buffer = try JSONEncoder().encodeAsByteBuffer(value, allocator: ByteBufferAllocator())
            logger.trace("✅ Encoded \(T.self): \(buffer.readableBytes) bytes")
            return buffer
        } catch {
            logger.error("❌ Failed to encode \(T.self): \(error)")
            throw error
        }
    }

    /// Generic decode method from ByteBuffer to any Codable type
    /// - Parameters:
    ///   - buffer: ByteBuffer containing encoded data
    ///   - type: The type to decode to
    /// - Returns: Decoded value of the specified type
    public func decode<T: Codable & Sendable>(_ buffer: ByteBuffer, as type: T.Type) throws -> T {
        do {
            let decoded = try JSONDecoder().decode(type, from: buffer)
            logger.trace("✅ Decoded \(T.self): \(buffer.readableBytes) bytes")
            return decoded
        } catch {
            logger.error("❌ Failed to decode \(T.self) from \(buffer.readableBytes) bytes: \(error)")
            // Log the raw data for debugging
            if let jsonString = buffer.getString(at: 0, length: buffer.readableBytes) {
                logger.error("Raw JSON data: \(jsonString)")
            }
            throw error
        }
    }

    /// Get list of registered workflow names
    /// - Returns: Array of registered workflow names
    public func getRegisteredWorkflowNames() -> [String] {
        workflowExecutors.withLock { executors in
            Array(executors.keys).sorted()
        }
    }
}

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
import Synchronization

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Completion stream for workflow execution completion notifications
public typealias WorkflowCompletionStream = AsyncStream<Result<ByteBuffer, Error>>

/// Registry for managing workflow completion notifications
/// Used by runWorkflow to wait for workflow completion via event-driven callbacks instead of polling
public final class WorkflowCompletionRegistry: Sendable {

    /// Shared global registry instance
    public static let shared = WorkflowCompletionRegistry()

    /// Active workflow completion continuations
    private let continuations: Mutex<[String: AsyncStream<Result<ByteBuffer, Error>>.Continuation]> = .init([:])

    private init() {}

    /// Register a workflow completion continuation
    /// - Parameters:
    ///   - workflowId: The workflow ID to wait for
    ///   - continuation: The continuation to notify when workflow completes
    public func register(workflowId: WorkflowID, continuation: AsyncStream<Result<ByteBuffer, Error>>.Continuation) {
        continuations.withLock { continuations in
            continuations[workflowId.value] = continuation
        }
    }

    /// Complete a workflow with success result
    /// - Parameters:
    ///   - workflowId: The workflow ID that completed
    ///   - result: The workflow output as ByteBuffer
    public func completeWorkflow(workflowId: WorkflowID, result: ByteBuffer) {
        continuations.withLock { continuations in
            if let continuation = continuations.removeValue(forKey: workflowId.value) {
                continuation.yield(.success(result))
                continuation.finish()
            }
        }
    }

    /// Fail a workflow with error
    /// - Parameters:
    ///   - workflowId: The workflow ID that failed
    ///   - error: The error that occurred
    public func failWorkflow(workflowId: WorkflowID, error: Error) {
        continuations.withLock { continuations in
            if let continuation = continuations.removeValue(forKey: workflowId.value) {
                continuation.yield(.failure(error))
                continuation.finish()
            }
        }
    }

    /// Cancel a workflow waiting operation
    /// - Parameter workflowId: The workflow ID to cancel waiting for
    public func cancelWorkflow(workflowId: WorkflowID) {
        continuations.withLock { continuations in
            if let continuation = continuations.removeValue(forKey: workflowId.value) {
                continuation.finish()
            }
        }
    }

    /// Create a completion stream for a workflow
    /// - Parameter workflowId: The workflow ID to create stream for
    /// - Returns: Tuple of stream and continuation
    public static func makeStream(
        workflowId: String
    ) -> (AsyncStream<Result<ByteBuffer, Error>>, AsyncStream<Result<ByteBuffer, Error>>.Continuation) {
        AsyncStream<Result<ByteBuffer, Error>>.makeStream()
    }
}

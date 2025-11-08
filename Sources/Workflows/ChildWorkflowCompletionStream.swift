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

import Foundation
import Logging
import NIOCore
import Synchronization

/// Child workflow completion stream using AsyncStream for event-driven completion
///
/// Provides an AsyncSequence interface for waiting on child workflow completion,
/// following the same pattern as ActivityCompletionStorage but using AsyncStream.
/// This enables clean, blocking child workflow execution APIs while using efficient
/// event-driven infrastructure underneath.
///
/// ## Pattern
/// Based on the ActivityCompletionStorage pattern:
/// 1. Check if child workflow already completed (cache hit)
/// 2. Create AsyncStream if cache miss
/// 3. Register stream continuation for completion
/// 4. Child workflow completion feeds stream via continuation
/// 5. Return first result from stream
///
/// ## Usage
/// ```swift
/// let (stream, continuation) = ChildWorkflowCompletionStream.makeStream(childWorkflowId: "child-123")
///
/// // User API - blocks until completion
/// for try await result in stream {
///     return try JSONUtils.decode(WorkflowOutput.self, from: result)
/// }
/// ```
///
/// ## Thread Safety
/// - Stream creation: Thread-safe via Mutex
/// - Result yielding: Thread-safe via AsyncStream.Continuation
/// - Cleanup: Automatic via AsyncStream lifecycle
public struct ChildWorkflowCompletionStream {

    /// The element type this sequence produces
    public typealias Element = Result<ByteBuffer, Error>

    /// The child workflow ID this stream is waiting for
    public let childWorkflowId: String

    /// The underlying AsyncStream
    private let stream: AsyncStream<Element>

    /// Private initializer - use makeStream() instead
    private init(stream: AsyncStream<Element>, childWorkflowId: String) {
        self.stream = stream
        self.childWorkflowId = childWorkflowId
    }
}

// MARK: - AsyncSequence Conformance

extension ChildWorkflowCompletionStream: AsyncSequence {

    public func makeAsyncIterator() -> AsyncStream<Element>.AsyncIterator {
        stream.makeAsyncIterator()
    }
}

// MARK: - Stream Creation

extension ChildWorkflowCompletionStream {

    /// Create a child workflow completion stream and its continuation
    ///
    /// - Parameter childWorkflowId: The child workflow ID to wait for
    /// - Returns: Tuple of (stream, continuation) where continuation is used to complete the stream
    public static func makeStream(childWorkflowId: String) -> (ChildWorkflowCompletionStream, AsyncStream<Element>.Continuation) {
        let (stream, continuation) = AsyncStream<Element>.makeStream()

        let completionStream = ChildWorkflowCompletionStream(
            stream: stream,
            childWorkflowId: childWorkflowId
        )

        return (completionStream, continuation)
    }
}

/// Registry for managing child workflow completion streams
///
/// Thread-safe registry that maps child workflow IDs to their completion continuations.
/// Used by the ChildWorkflowCallbackMiddleware to notify waiting parent workflows
/// when child workflows complete.
public final class ChildWorkflowRegistry: Sendable {

    /// Shared singleton instance
    public static let shared = ChildWorkflowRegistry()

    /// Thread-safe storage for workflow continuations
    private let continuations: Mutex<[String: AsyncStream<ChildWorkflowCompletionStream.Element>.Continuation]>

    /// Logger for debugging
    private let logger: Logger

    /// Private initializer - use shared instance
    private init() {
        self.continuations = Mutex([:])
        self.logger = Logger(label: "ChildWorkflowRegistry")
    }

    /// Register a continuation for a child workflow
    ///
    /// - Parameters:
    ///   - childWorkflowId: The child workflow ID
    ///   - continuation: The continuation to complete when workflow finishes
    public func register(childWorkflowId: WorkflowID, continuation: AsyncStream<ChildWorkflowCompletionStream.Element>.Continuation) {
        continuations.withLock { continuations in
            continuations[childWorkflowId.value] = continuation

            logger.trace(
                "Registered child workflow completion continuation",
                metadata: [
                    "childWorkflowId": .string(childWorkflowId.value),
                    "totalRegistered": .stringConvertible(continuations.count),
                ]
            )
        }
    }

    /// Complete a child workflow with success result
    ///
    /// - Parameters:
    ///   - childWorkflowId: The child workflow ID that completed
    ///   - result: The workflow output as ByteBuffer
    public func completeWorkflow(childWorkflowId: WorkflowID, result: ByteBuffer) {
        continuations.withLock { continuations in
            guard let continuation = continuations.removeValue(forKey: childWorkflowId.value) else {
                logger.trace(
                    "No continuation found for completed child workflow",
                    metadata: ["childWorkflowId": .string(childWorkflowId.value)]
                )
                return
            }

            logger.debug(
                "Completing child workflow with success",
                metadata: [
                    "childWorkflowId": .string(childWorkflowId.value),
                    "resultSize": .stringConvertible(result.readableBytes),
                ]
            )

            continuation.yield(.success(result))
            continuation.finish()
        }
    }

    /// Complete a child workflow with error
    ///
    /// - Parameters:
    ///   - childWorkflowId: The child workflow ID that failed
    ///   - error: The error that caused the failure
    public func failWorkflow(childWorkflowId: WorkflowID, error: Error) {
        continuations.withLock { continuations in
            guard let continuation = continuations.removeValue(forKey: childWorkflowId.value) else {
                logger.trace(
                    "No continuation found for failed child workflow",
                    metadata: ["childWorkflowId": .string(childWorkflowId.value)]
                )
                return
            }

            logger.debug(
                "Completing child workflow with error",
                metadata: [
                    "childWorkflowId": .string(childWorkflowId.value),
                    "error": .string(error.localizedDescription),
                ]
            )

            continuation.yield(.failure(error))
            continuation.finish()
        }
    }

    /// Cancel a child workflow stream
    ///
    /// - Parameter childWorkflowId: The child workflow ID to cancel
    public func cancelWorkflow(childWorkflowId: WorkflowID) {
        continuations.withLock { continuations in
            guard let continuation = continuations.removeValue(forKey: childWorkflowId.value) else {
                return
            }

            logger.debug(
                "Cancelling child workflow stream",
                metadata: ["childWorkflowId": .string(childWorkflowId.value)]
            )

            continuation.finish()
        }
    }

    /// Check if a child workflow has a registered continuation
    ///
    /// - Parameter childWorkflowId: The child workflow ID to check
    /// - Returns: True if the workflow has a registered continuation
    public func hasRegistration(for childWorkflowId: WorkflowID) -> Bool {
        continuations.withLock { continuations in
            continuations.keys.contains(childWorkflowId.value)
        }
    }

    /// Get the count of registered child workflows
    ///
    /// - Returns: Number of currently registered child workflows
    public var registeredCount: Int {
        continuations.withLock { $0.count }
    }
}

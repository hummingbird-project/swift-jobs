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
import Synchronization

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Example of an Activity registration:
///
/// ```swift
/// // Define activity parameters with type safety
/// struct SendEmailActivity: ActivityParameters {
///     static let activityName = "sendEmail"
///     typealias Input = EmailRequest
///     typealias Output = EmailResponse
/// }
///
/// // Create activity container with dependencies
/// struct EmailActivityContainer: ActivityContainer {
///     private let emailService: EmailService
///
///     init(emailService: EmailService) {
///         self.emailService = emailService
///     }
///
///     func registerActivities(with registry: ActivityRegistry) {
///         registry.registerActivity(SendEmailActivity.self) { input in
///             return try await self.emailService.send(
///                 to: input.recipient,
///                 subject: input.subject,
///                 body: input.body
///             )
///         }
///     }
/// }
///
/// // Usage in workflow engine
/// let activities = EmailActivityContainer(emailService: emailService)
/// let workflowEngine = WorkflowEngine(activities: [activities])
/// ```

/// Empty output type for activities that don't return a value
/// Used as the default Output type for ActivityParameters
public struct EmptyOutput: Codable, Sendable {
    public init() {}
}

/// How an activity should handle cancellation
public struct ActivityCancellationType: Sendable, Equatable {
    public let rawValue: String

    public init(_ rawValue: String) {
        self.rawValue = rawValue
    }

    /// Try to cancel the activity but don't wait for completion
    public static let TRY_CANCEL = ActivityCancellationType("TRY_CANCEL")
    /// Wait for the activity to handle cancellation gracefully
    public static let WAIT_CANCELLATION_COMPLETED = ActivityCancellationType("WAIT_CANCELLATION_COMPLETED")
    /// Abandon the activity immediately without notification
    public static let ABANDON = ActivityCancellationType("ABANDON")
}

/// Options for activity execution
public struct ActivityOptions: Sendable {
    /// Maximum time the activity can run before timing out
    public let startToCloseTimeout: Duration?
    /// Retry policy for failed activities
    public let retryPolicy: StepRetryPolicy
    /// How to handle cancellation of this activity
    public let cancellationType: ActivityCancellationType
    /// Heartbeat timeout for cancellation detection
    public let heartbeatTimeout: Duration?

    public init(
        startToCloseTimeout: Duration? = nil,
        retryPolicy: StepRetryPolicy = .exponentialJitter(maxAttempts: 4),
        cancellationType: ActivityCancellationType = .TRY_CANCEL,
        heartbeatTimeout: Duration? = nil
    ) {
        self.startToCloseTimeout = startToCloseTimeout
        self.retryPolicy = retryPolicy
        self.cancellationType = cancellationType
        self.heartbeatTimeout = heartbeatTimeout
    }
}

/// Protocol for activity containers that hold activity implementations
public protocol ActivityContainer: Sendable {
    /// Register all activities from this container with the given registry
    func registerActivities(with registry: ActivityRegistry)
}

/// Protocol for activity parameters (for workflows to reference activities)
public protocol ActivityParameters: Sendable {
    associatedtype Input: Codable & Sendable
    associatedtype Output: Codable & Sendable = EmptyOutput

    /// Name of the activity
    /// Defaults to the type name (e.g., "ProcessOrderActivity")
    static var activityName: String { get }
}

/// Default implementation using type name
extension ActivityParameters {
    public static var activityName: String {
        String(describing: self)
    }
}

/// Simple delay job for sleep functionality
internal struct DelayJob: JobParameters {
    public static let jobName = "DelayJob"
}

/// Protocol for type-erased activity execution
internal protocol ActivityExecutor: Sendable {
    func execute(input: any Codable & Sendable, context: ActivityExecutionContext) async throws -> any Codable & Sendable
}

/// Type-erased activity executor implementation
internal struct TypedActivityExecutor<Input: Codable & Sendable, Output: Codable & Sendable>: ActivityExecutor {
    private let executor: @Sendable (Input, ActivityExecutionContext) async throws -> Output

    init(executor: @escaping @Sendable (Input, ActivityExecutionContext) async throws -> Output) {
        self.executor = executor
    }

    func execute(input: any Codable & Sendable, context: ActivityExecutionContext) async throws -> any Codable & Sendable {
        guard let typedInput = input as? Input else {
            throw WorkflowError.invalidActivityInput
        }
        return try await executor(typedInput, context)
    }
}

/// Registry for activity functions
public final class ActivityRegistry: Sendable {
    private let activityBuilders: Mutex<[String: @Sendable (ByteBuffer, ActivityExecutionContext) async throws -> ByteBuffer]> = .init([:])

    internal let allocator: ByteBufferAllocator

    internal init(allocator: ByteBufferAllocator = ByteBufferAllocator()) {
        self.allocator = allocator
    }

    /// Register activities from containers
    internal func registerActivities(from containers: [any ActivityContainer]) {
        for container in containers {
            container.registerActivities(with: self)
        }
    }

    /// Register an activity that returns a value
    ///
    /// Example:
    /// ```swift
    /// registry.registerActivity(ProcessPaymentActivity.self) { input, context in
    ///     context.logger.info("Processing payment", metadata: ["customerId": .string(input.customerId)])
    ///     return try await paymentService.process(input)
    /// }
    /// ```
    public func registerActivity<A: ActivityParameters, R>(
        _ activityType: A.Type,
        returning returnType: R.Type = R.self,
        executor: @escaping @Sendable (A.Input, ActivityExecutionContext) async throws -> R
    ) where R == A.Output {
        let builder: @Sendable (ByteBuffer, ActivityExecutionContext) async throws -> ByteBuffer = { inputBuffer, context in
            // Decode input from ByteBuffer
            let input = try JSONDecoder().decode(A.Input.self, from: inputBuffer)

            // Execute the activity
            let result = try await executor(input, context)

            // Encode result back to ByteBuffer
            return try JSONEncoder().encodeAsByteBuffer(result, allocator: self.allocator)
        }

        activityBuilders.withLock { builders in
            builders[A.activityName] = builder
        }
    }

    /// Register a void activity (no return value)
    ///
    /// Example:
    /// ```swift
    /// registry.registerActivity(SendEmailActivity.self, returning: Void.self) { input, context in
    ///     context.logger.info("Sending email", metadata: ["recipient": .string(input.recipient)])
    ///     try await emailService.send(input)
    ///     // No return statement needed!
    /// }
    /// ```
    public func registerActivity<A: ActivityParameters>(
        _ activityType: A.Type,
        returning returnType: Void.Type = Void.self,
        executor: @escaping @Sendable (A.Input, ActivityExecutionContext) async throws -> Void
    ) where A.Output == EmptyOutput {
        let builder: @Sendable (ByteBuffer, ActivityExecutionContext) async throws -> ByteBuffer = { inputBuffer, context in
            // Decode input from ByteBuffer
            let input = try JSONDecoder().decode(A.Input.self, from: inputBuffer)

            // Execute the activity
            try await executor(input, context)

            // Return EmptyOutput encoded as ByteBuffer
            let result = EmptyOutput()
            return try JSONEncoder().encodeAsByteBuffer(result, allocator: self.allocator)
        }

        activityBuilders.withLock { builders in
            builders[A.activityName] = builder
        }
    }

    /// Execute an activity with ByteBuffer input and output (type-erased for workflow engine)
    internal func executeActivity(
        name: String,
        inputBuffer: ByteBuffer,
        context: ActivityExecutionContext
    ) async throws -> ByteBuffer {
        let activityBuilder = try activityBuilders.withLock { builders in
            guard let builder = builders[name] else {
                throw WorkflowError.unknownActivity(name)
            }
            return builder
        }

        return try await context.withCurrent {
            try await activityBuilder(inputBuffer, context)
        }
    }
}

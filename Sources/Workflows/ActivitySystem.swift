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
public struct EmptyOutput: Codable, Sendable {
    public init() {}
}

/// Options for activity execution
public struct ActivityOptions: Sendable {
    /// Maximum time the activity can run before timing out
    public let startToCloseTimeout: Duration?
    /// Retry policy for failed activities
    public let retryPolicy: (any JobRetryStrategy)?

    public init(
        startToCloseTimeout: Duration? = nil,
        retryPolicy: (any JobRetryStrategy)? = nil
    ) {
        self.startToCloseTimeout = startToCloseTimeout
        self.retryPolicy = retryPolicy
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
    associatedtype Output: Codable & Sendable

    /// Name of the activity
    static var activityName: String { get }
}

/// Simple delay job for sleep functionality
internal struct DelayJob: JobParameters {
    public static let jobName = "DelayJob"
}

/// Registry for activity functions
public final class ActivityRegistry: Sendable {
    private let activities: Mutex<[String: @Sendable (ByteBuffer) async throws -> AnyActivityResult]> = .init([:])

    /// Register activities from containers
    internal func registerActivities(from containers: [any ActivityContainer]) {
        for container in containers {
            container.registerActivities(with: self)
        }
    }

    /// Register an activity using ActivityParameters
    ///
    /// Example:
    /// ```swift
    /// registry.registerActivity(SendEmailActivity.self) { input in
    ///     return try await emailService.send(input)
    /// }
    /// ```
    public func registerActivity<A: ActivityParameters>(
        _ activityType: A.Type,
        executor: @escaping @Sendable (A.Input) async throws -> A.Output
    ) {
        let wrappedExecutor: @Sendable (ByteBuffer) async throws -> AnyActivityResult = { parameters in
            do {
                let input = try JSONDecoder().decode(A.Input.self, from: parameters)
                let output = try await executor(input)
                return try AnyActivityResult(success: output)
            } catch {
                return try AnyActivityResult(failure: error.localizedDescription)
            }
        }

        activities.withLock { activities in
            precondition(activities[A.activityName] == nil, "There is an activity already registered under name \"\(A.activityName)\"")
            activities[A.activityName] = wrappedExecutor
        }
    }

    /// Execute an activity by name
    internal func executeActivity(name: String, parameters: ByteBuffer) async throws -> AnyActivityResult {
        let executor = try activities.withLock { activities in
            guard let executor = activities[name] else {
                throw WorkflowError.unknownActivity(name)
            }
            return executor
        }

        return try await executor(parameters)
    }
}

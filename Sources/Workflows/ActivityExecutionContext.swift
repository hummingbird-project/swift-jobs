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

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Execution context information and utilities available during activity execution.
///
/// The activity execution context provides access to activity metadata, logging functionality,
/// retry information, and cancellation support. Activities can access the current context using
/// the static ``current`` property.
///
/// ## Accessing the Context
///
/// ```swift
/// registry.registerActivity(SendEmailActivity.self, returning: Void.self) { (input: EmailInput, context: ActivityExecutionContext) async throws in
///     context.logger.info("Sending email", metadata: [
///         "recipient": .string(input.recipient),
///         "attempt": .stringConvertible(context.attempt)
///     ])
///
///     if context.attempt > 1 {
///         context.logger.warning("Retry attempt \(context.attempt)")
///     }
///
///     try await emailService.send(input)
///     // No return statement needed for void activities!
/// }
/// ```
public struct ActivityExecutionContext: Sendable {
    /// A task-local storage container providing access to the current activity execution context.
    @TaskLocal
    static var taskLocal: ActivityExecutionContext?

    /// The current activity execution context if one is available.
    ///
    /// Returns `nil` if called outside of an activity execution context. Activity implementations should
    /// check for `nil` before using the context, or use the context parameter provided in the registration.
    ///
    /// - Note: Internally this is stored as a task local and will propagate down the structured task tree.
    public static var current: ActivityExecutionContext? {
        self.taskLocal
    }

    /// Detailed information about the currently executing activity.
    public struct Info: Sendable {
        /// The unique identifier for this activity instance.
        public let activityId: String

        /// The activity name as registered with the worker.
        public let activityName: String

        /// The workflow ID that scheduled this activity.
        public let workflowId: WorkflowID

        /// The workflow type name that scheduled this activity.
        public let workflowType: String

        /// The current step index in the workflow execution.
        public let stepIndex: Int

        /// The timeout configured for this activity execution.
        public let timeout: Duration?

        /// The job ID assigned to this activity execution.
        public let jobId: String

        internal init(
            activityId: String,
            activityName: String,
            workflowId: WorkflowID,
            workflowType: String,
            stepIndex: Int,
            timeout: Duration?,
            jobId: String
        ) {
            self.activityId = activityId
            self.activityName = activityName
            self.workflowId = workflowId
            self.workflowType = workflowType
            self.stepIndex = stepIndex
            self.timeout = timeout
            self.jobId = jobId
        }
    }

    /// Metadata and configuration information about the currently executing activity.
    public let info: Info

    /// Logger instance with activity-specific metadata for structured logging.
    ///
    /// The logger automatically includes activity ID, workflow ID, and other contextual information
    /// to help with debugging and monitoring.
    public let logger: Logger

    /// The current retry attempt number (starting from 1).
    ///
    /// This information can be used to implement exponential backoff, different behavior on retries,
    /// or to log retry attempts for monitoring purposes.
    public let attempt: Int

    /// Indicates whether this activity execution was triggered by a retry.
    ///
    /// This is a convenience property equivalent to `attempt > 1`.
    public var isRetry: Bool {
        attempt > 1
    }

    /// The underlying job execution context from the job system.
    ///
    /// This provides access to lower-level job execution details if needed for advanced use cases.
    public let jobContext: JobExecutionContext

    internal init(
        info: Info,
        logger: Logger,
        attempt: Int,
        jobContext: JobExecutionContext
    ) {
        self.info = info
        self.logger = logger
        self.attempt = attempt
        self.jobContext = jobContext
    }

    /// Creates an activity execution context from job execution parameters.
    internal init(
        activityJob: ActivityExecutionJob,
        jobContext: JobExecutionContext,
        jobId: String
    ) {
        let info = Info(
            activityId: activityJob.activityId.value,
            activityName: activityJob.activityName,
            workflowId: activityJob.workflowId,
            workflowType: activityJob.workflowType,
            stepIndex: activityJob.stepIndex,
            timeout: activityJob.timeout,
            jobId: jobId
        )

        // Create activity-specific logger with contextual metadata
        var logger = jobContext.logger
        logger[metadataKey: "activityId"] = .string(activityJob.activityId.value)
        logger[metadataKey: "activityName"] = .string(activityJob.activityName)
        logger[metadataKey: "workflowId"] = .string(activityJob.workflowId.value)
        logger[metadataKey: "workflowType"] = .string(activityJob.workflowType)

        // Use attempt number directly from job context
        self.init(
            info: info,
            logger: jobContext.logger,
            attempt: jobContext.attempt,
            jobContext: jobContext
        )
    }

}

/// Extension to provide task-local context execution
extension ActivityExecutionContext {
    /// Executes the given closure with this context set as the current task-local context.
    ///
    /// This allows activities to access the context using `ActivityExecutionContext.current`
    /// without requiring explicit context parameter passing.
    ///
    /// - Parameter operation: The operation to execute with this context active.
    /// - Returns: The result of the operation.
    internal func withCurrent<T>(
        operation: () async throws -> T
    ) async rethrows -> T {
        try await Self.$taskLocal.withValue(self) {
            try await operation()
        }
    }

}

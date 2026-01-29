//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

import Logging
import NIOCore
import NIOFoundationCompat
import ServiceLifecycle

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Protocol for Job queue. Allows us to pass job queues around as existentials
public protocol JobQueueProtocol {
    associatedtype Queue: JobQueueDriver

    var logger: Logger { get }

    ///  Push Job onto queue
    /// - Parameters:
    ///   - parameters: Job parameters
    ///   - parameters: parameters for the job
    /// - Returns: Identifier of queued job
    @discardableResult func push<Parameters: Sendable>(
        jobRequest: JobRequest<Parameters>,
        options: Queue.JobOptions
    ) async throws -> Queue.JobID

    ///  Register job type
    /// - Parameters:
    ///   - job: Job definition
    func registerJob(_ job: JobDefinition<some Sendable & Codable>)

    ///  Create JobQueue processor that will process jobs pushed to the queue
    /// - Parameter options: Processor options
    func processor(options: JobQueueProcessorOptions) -> any Service

    /// Job queue options
    var options: JobQueueOptions { get }
}

extension JobQueueProtocol {
    ///  Push Job onto queue
    /// - Parameters:
    ///   - parameters: parameters for the job
    /// - Returns: Identifier of queued job
    @inlinable
    @discardableResult public func push<Parameters: Sendable & Codable>(
        _ jobName: JobName<Parameters>,
        parameters: Parameters
    ) async throws -> Queue.JobID {
        let request = JobRequest(name: jobName.name, parameters: parameters, queuedAt: .now, attempt: 1)
        return try await self.push(jobRequest: request, options: .init(delayUntil: .now))
    }

    ///  Push Job onto queue
    /// - Parameters:
    ///   - parameters: Job parameters
    /// - Returns: Identifier of queued job
    @inlinable
    @discardableResult public func push<Parameters: JobParameters>(
        _ parameters: Parameters
    ) async throws -> Queue.JobID {
        try await self.push(.init(Parameters.jobName), parameters: parameters, options: .init(delayUntil: .now))
    }

    ///  Push Job onto queue
    /// - Parameters:
    ///   - parameters: parameters for the job
    /// - Returns: Identifier of queued job
    @inlinable
    @discardableResult public func push<Parameters: Sendable & Codable>(
        _ jobName: JobName<Parameters>,
        parameters: Parameters,
        options: Queue.JobOptions
    ) async throws -> Queue.JobID {
        let request = JobRequest(name: jobName.name, parameters: parameters, queuedAt: .now, attempt: 1)
        return try await self.push(jobRequest: request, options: options)
    }

    ///  Push Job onto queue
    /// - Parameters:
    ///   - parameters: Job parameters
    /// - Returns: Identifier of queued job
    @inlinable
    @discardableResult public func push<Parameters: JobParameters>(
        _ parameters: Parameters,
        options: Queue.JobOptions
    ) async throws -> Queue.JobID {
        try await self.push(.init(Parameters.jobName), parameters: parameters, options: options)
    }

    ///  Initialize JobDefinition
    /// - Parameters:
    ///   - parameters: Job parameter type
    ///   - retryStrategy: Retry strategy for failed jobs
    ///   - timeout: Timeout for long running jobs
    ///   - execute: Closure that executes job
    public func registerJob<Parameters: JobParameters>(
        parameters: Parameters.Type = Parameters.self,
        retryStrategy: (any JobRetryStrategy)? = nil,
        timeout: Duration? = nil,
        execute: @escaping @Sendable (Parameters, JobExecutionContext) async throws -> Void
    ) where Parameters: JobParameters {
        self.logger.info("Registered Job", metadata: ["JobName": .string(Parameters.jobName)])
        let job = JobDefinition<Parameters>(
            retryStrategy: retryStrategy ?? self.options.retryStrategy,
            timeout: timeout,
            execute: execute
        )
        self.registerJob(job)
    }

    ///  Initialize JobDefinition
    /// - Parameters:
    ///   - name: Job name
    ///   - parameters: Job parameter type
    ///   - retryStrategy: Retry strategy for failed jobs
    ///   - timeout: Timeout for long running jobs
    ///   - execute: Closure that executes job
    public func registerJob<Parameters: Sendable & Codable>(
        name: JobName<Parameters>,
        parameters: Parameters.Type = Parameters.self,
        retryStrategy: (any JobRetryStrategy)? = nil,
        timeout: Duration? = nil,
        execute: @escaping @Sendable (Parameters, JobExecutionContext) async throws -> Void
    ) {
        self.logger.info("Registered Job", metadata: ["JobName": .string(name.name)])
        let job = JobDefinition(
            name: name,
            parameters: parameters,
            retryStrategy: retryStrategy ?? self.options.retryStrategy,
            timeout: timeout,
            execute: execute
        )
        self.registerJob(job)
    }

    ///  Register job type
    /// - Parameters:
    ///   - parameters: Job Parameters
    ///   - maxRetryCount: Maximum number of times job is retried before being flagged as failed
    ///   - execute: Job code
    @available(*, deprecated, renamed: "init(parameters:retryStrategy:timeout:execute:)")
    public func registerJob<Parameters: JobParameters>(
        parameters: Parameters.Type = Parameters.self,
        maxRetryCount: Int,
        execute: @escaping @Sendable (Parameters, JobExecutionContext) async throws -> Void
    ) {
        self.registerJob(parameters: parameters, retryStrategy: .exponentialJitter(maxAttempts: maxRetryCount), execute: execute)
    }
}

/// Job queue
///
/// Wrapper type to bring together a job queue implementation and a job queue
/// handler. Before you can push jobs onto a queue you should register it
/// with the queue via either ``registerJob(parameters:maxRetryCount:execute:)`` or
/// ``registerJob(_:)``.
public struct JobQueue<Queue: JobQueueDriver>: JobQueueProtocol, Sendable {
    /// underlying driver for queue
    public let queue: Queue
    @usableFromInline
    let middleware: any JobMiddleware
    public let logger: Logger
    public let options: JobQueueOptions

    public init(
        _ queue: Queue,
        logger: Logger,
        options: JobQueueOptions = .init(),
        @JobMiddlewareBuilder middleware: () -> some JobMiddleware = { NullJobMiddleware() }
    ) {
        self.queue = queue
        self.middleware = middleware()
        self.logger = logger
        self.options = options
    }

    public init(
        _ queue: Queue,
        logger: Logger,
        options: JobQueueOptions = .init(),
        @JobMiddlewareBuilder middleware: (Queue) -> some JobMiddleware
    ) {
        self.queue = queue
        self.middleware = middleware(queue)
        self.logger = logger
        self.options = options
    }

    ///  Create JobQueue handler that will process jobs pushed to the queue
    /// - Parameter options: Processor options
    public func processor(
        options: JobQueueProcessorOptions = .init()
    ) -> any Service {
        JobQueueProcessor(queue: queue, logger: logger, options: options, middleware: self.middleware)
    }

    ///  Push Job onto queue
    /// - Parameters:
    ///   - parameters: parameters for the job
    /// - Returns: Identifier of queued job
    @discardableResult
    @inlinable
    public func push<Parameters: Sendable>(
        jobRequest: JobRequest<Parameters>,
        options: Queue.JobOptions
    ) async throws -> Queue.JobID {
        let instanceID = try await self.queue.push(jobRequest, options: options)
        await self.middleware.onPushJob(
            name: jobRequest.name,
            parameters: jobRequest.data.parameters,
            context: .init(jobID: instanceID.description, attempt: jobRequest.data.attempt)
        )
        self.logger.debug(
            "Pushed Job",
            metadata: ["JobID": .stringConvertible(instanceID), "JobName": .string(jobRequest.name)]
        )
        return instanceID
    }

    ///  Register job type
    /// - Parameters:
    ///   - job: Job definition
    public func registerJob(_ job: JobDefinition<some Sendable & Codable>) {
        self.queue.registerJob(job)
    }

    /// Attempt to cancel a job
    /// - Parameters:
    ///   - jobID: an existing job id
    @inlinable
    public func cancelJob(
        jobID: Queue.JobID
    ) async throws where Queue: CancellableJobQueue {
        try await self.queue.cancel(jobID: jobID)
    }

    /// Attempt to pause a job
    /// - Parameters:
    ///   - jobID: an existing job id
    @inlinable
    public func pauseJob(
        jobID: Queue.JobID
    ) async throws where Queue: ResumableJobQueue {
        try await self.queue.pause(jobID: jobID)
    }

    /// Resumes a job that was paused
    /// - Parameters:
    ///   - jobID: an existing job id
    @inlinable
    public func resumeJob(
        jobID: Queue.JobID
    ) async throws where Queue: ResumableJobQueue {
        try await self.queue.resume(jobID: jobID)
    }
}

extension JobQueue: CustomStringConvertible {
    public var description: String { "JobQueue<\(String(describing: Queue.self))>" }
}

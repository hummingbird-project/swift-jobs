//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2021-2024 the Hummingbird authors
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
import NIOFoundationCompat
import ServiceLifecycle

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Protocol for Job queue. Allows us to pass job queues around as existentials
public protocol JobQueueProtocol: Service {
    associatedtype Queue: JobQueueDriver

    var logger: Logger { get }

    ///  Push Job onto queue
    /// - Parameters:
    ///   - parameters: Job parameters
    ///   - parameters: parameters for the job
    /// - Returns: Identifier of queued job
    @discardableResult func push<Parameters: JobParameters>(
        _ parameters: Parameters,
        options: Queue.JobOptions
    ) async throws -> Queue.JobID

    ///  Register job type
    /// - Parameters:
    ///   - job: Job definition
    func registerJob(_ job: JobDefinition<some JobParameters>)

    /// Job queue options
    var options: JobQueueOptions { get }
}

extension JobQueueProtocol {
    ///  Push Job onto queue
    /// - Parameters:
    ///   - parameters: Job parameters
    /// - Returns: Identifier of queued job
    @discardableResult public func push<Parameters: JobParameters>(
        _ parameters: Parameters
    ) async throws -> Queue.JobID {
        try await self.push(parameters, options: .init())
    }

    ///  Initialize JobDefinition
    /// - Parameters:
    ///   - parameters: Job parameter type
    ///   - retryStrategy: Retry strategy for failed jobs
    ///   - execute: Closure that executes job
    public func registerJob<Parameters: JobParameters>(
        parameters: Parameters.Type = Parameters.self,
        retryStrategy: (any JobRetryStrategy)? = nil,
        execute: @escaping @Sendable (Parameters, JobExecutionContext) async throws -> Void
    ) where Parameters: JobParameters {
        self.logger.info("Registered Job", metadata: ["JobName": .string(Parameters.jobName)])
        let job = JobDefinition<Parameters>(retryStrategy: retryStrategy ?? self.options.retryStrategy, execute: execute)
        self.registerJob(job)
    }

    ///  Register job type
    /// - Parameters:
    ///   - parameters: Job Parameters
    ///   - maxRetryCount: Maximum number of times job is retried before being flagged as failed
    ///   - execute: Job code
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
public struct JobQueue<Queue: JobQueueDriver>: JobQueueProtocol {
    /// underlying driver for queue
    public let queue: Queue
    let handler: JobQueueHandler<Queue>
    let initializationComplete: Trigger

    public init(
        _ queue: Queue,
        numWorkers: Int = 1,
        logger: Logger,
        options: JobQueueOptions = .init(),
        @JobMiddlewareBuilder middleware: () -> some JobMiddleware = { NullJobMiddleware() }
    ) {
        self.queue = queue
        self.handler = .init(queue: queue, numWorkers: numWorkers, logger: logger, options: options, middleware: middleware())
        self.initializationComplete = .init()
    }

    ///  Push Job onto queue
    /// - Parameters:
    ///   - parameters: parameters for the job
    /// - Returns: Identifier of queued job
    @discardableResult public func push<Parameters: JobParameters>(
        _ parameters: Parameters,
        options: Queue.JobOptions = .init()
    ) async throws -> Queue.JobID {
        let request = JobRequest(parameters: parameters, queuedAt: .now, attempts: 0)
        let instanceID = try await self.queue.push(request, options: options)
        await self.handler.middleware.onPushJob(
            parameters: parameters,
            context: .init(jobID: instanceID.description)
        )
        self.logger.debug(
            "Pushed Job",
            metadata: ["JobID": .stringConvertible(instanceID), "JobName": .string(Parameters.jobName)]
        )
        return instanceID
    }

    ///  Push Job onto queue
    /// - Parameters:
    ///   - parameters: parameters for the job
    ///   - currentSchedule: current job schedule
    ///   - nextScheduledAt: next schedule for the job
    ///   - options: job options
    /// - Returns: Identifier of queued job
    @discardableResult internal func schedule<Parameters: JobParameters>(
        _ parameters: Parameters,
        currentSchedule: Date,
        nextScheduledAt: Date?,
        options: Queue.JobOptions
    ) async throws -> Queue.JobID {
        let request = JobRequest(
            parameters: parameters,
            queuedAt: currentSchedule,
            attempts: 0,
            nextScheduledAt: nextScheduledAt
        )
        let instanceID = try await self.queue.push(request, options: options)
        await self.handler.middleware.onPushJob(
            parameters: parameters,
            context: .init(jobID: instanceID.description)
        )
        self.logger.debug(
            "Pushed Job",
            metadata: ["JobID": .stringConvertible(instanceID), "JobName": .string(Parameters.jobName)]
        )
        return instanceID
    }

    ///  Register job type
    /// - Parameters:
    ///   - job: Job definition
    public func registerJob(_ job: JobDefinition<some JobParameters>) {
        self.handler.queue.registerJob(job)
    }

    /// Attempt to cancel a job
    /// - Parameters:
    ///   - jobID: an existing job id
    public func cancelJob(
        jobID: Queue.JobID
    ) async throws where Queue: CancellableJobQueueProtocol {
        try await self.queue.cancel(jobID: jobID)
    }

    /// Attempt to pause a job
    /// - Parameters:
    ///   - jobID: an existing job id
    public func pauseJob(
        jobID: Queue.JobID
    ) async throws where Queue: ResumeableJobQueueProtocol {
        try await self.queue.pause(jobID: jobID)
    }

    /// Resumes a job that was paused
    /// - Parameters:
    ///   - jobID: an existing job id
    public func resumeJob(
        jobID: Queue.JobID
    ) async throws where Queue: ResumeableJobQueueProtocol {
        try await self.queue.resume(jobID: jobID)
    }

    ///  Run queue handler
    public func run() async throws {
        do {
            try await self.queue.onInit()
            self.initializationComplete.trigger()
        } catch {
            self.initializationComplete.failed(error)
        }
        try await self.handler.run()
    }

    /// Logger used by Job queue and its handler
    public var logger: Logger { self.handler.logger }
    /// Job queue options
    public var options: JobQueueOptions { self.handler.options }
}

extension JobQueue {
    /// Get JobQueue metadata
    func getMetadata<Value: Codable>(_ key: JobMetadataKey<Value>) async throws -> Value? {
        guard let buffer = try await self.queue.getMetadata(key.name) else { return nil }
        return try JSONDecoder().decode(Value.self, from: buffer)
    }

    /// Set JobQueue metadata
    func setMetadata<Value: Codable>(key: JobMetadataKey<Value>, value: Value) async throws {
        let buffer = try JSONEncoder().encodeAsByteBuffer(value, allocator: ByteBufferAllocator())
        try await self.queue.setMetadata(key: key.name, value: buffer)
    }
}

extension JobQueue: CustomStringConvertible {
    public var description: String { "JobQueue<\(String(describing: Queue.self))>" }
}

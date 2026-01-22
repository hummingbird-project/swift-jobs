//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2021-2025 the Hummingbird authors
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

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Job queue protocol.
///
/// Defines how to push and pop job data off a queue
public protocol JobQueueDriver: AsyncSequence, Sendable where Element == JobQueueResult<JobID> {
    associatedtype JobID: CustomStringConvertible & Sendable
    associatedtype JobOptions: JobOptionsProtocol

    /// Wait until job queue is ready
    func waitUntilReady() async throws
    /// Register job definition with driver
    func registerJob<Parameters: Sendable & Codable>(_ job: JobDefinition<Parameters>)
    /// Push Job onto queue
    /// - Parameters
    ///   - jobRequest: Job Request
    ///   - options: JobOptions
    /// - Returns: Identifier of queued jobs
    @discardableResult func push<Parameters>(_ jobRequest: JobRequest<Parameters>, options: JobOptions) async throws -> JobID
    /// Retry an existing Job
    /// - Parameters
    ///   - id: Job instance ID
    ///   - jobRequest: Job Request
    ///   - options: JobOptions
    func retry<Parameters>(_ id: JobID, jobRequest: JobRequest<Parameters>, options: JobRetryOptions) async throws
    /// This is called to say job has finished processing and it can be deleted
    func finished(jobID: JobID) async throws
    /// This is called to say job has failed to run and should be put aside
    func failed(jobID: JobID, error: any Error) async throws
    /// stop serving jobs
    func stop() async
    /// shutdown queue
    func shutdownGracefully() async
    /// worker context
    var workerContext: JobWorkerContext { get }
}

extension JobQueueDriver {
    /// default version of waitUntilReady doing nothing
    public func waitUntilReady() async throws {}
    /// default version of worker ID is to return the string version of a UUID
    public var workerContext: JobWorkerContext { .init(id: UUID().uuidString, metadata: [:]) }
}

extension JobQueueDriver {
    func retry(_ jobID: JobID, job: some JobInstanceProtocol, attempt: Int, options: JobRetryOptions) async throws {
        let jobRequest = JobRequest(name: job.name, parameters: job.parameters, queuedAt: job.queuedAt, attempt: attempt)
        return try await self.retry(jobID, jobRequest: jobRequest, options: options)
    }
}

public struct JobWorkerContext: Sendable {
    /// Job worker id
    public let id: String
    /// Job worker metadata
    public let metadata: [String: String]
}

/// Type returned from iterating a JobQueueDriver
///
/// The `JobQueueResult`` can hold either a job instance or the error created when iterating
/// the job queue. If the error is returned in `JobQueueResult` then the job queue handler will
/// handle the error. If the job queue throws an error when it is iterated then the job queue
/// will throw that error.
public struct JobQueueResult<JobID: Sendable>: Sendable {
    public let id: JobID
    public let result: Result<any JobInstanceProtocol, JobQueueError>

    ///  Initialize JobQueueResult
    /// - Parameters:
    ///   - id: id of job
    ///   - result: Result (job instance or error)
    public init(id: JobID, result: Result<any JobInstanceProtocol, JobQueueError>) {
        self.id = id
        self.result = result
    }
}

/// Protocol for JobOptions
public protocol JobOptionsProtocol: Sendable {
    /// When to execute the job
    var delayUntil: Date { get }
    /// Initialize JobOptionsProtocol
    /// - Parameters:
    ///   - delayUntil: When to execute the job
    init(delayUntil: Date)
}

/// Options for retrying a job
public struct JobRetryOptions {
    /// When to execute the job
    public let delayUntil: Date
    /// Default init JobOptionsProtocol
    /// - Parameters:
    ///   - delayUntil: When to execute the job
    init(delayUntil: Date) {
        self.delayUntil = delayUntil
    }
}

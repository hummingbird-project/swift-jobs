//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

import Logging
import NIOCore

/// Job queue protocol.
///
/// Defines how to push and pop job data off a queue
public protocol JobQueueDriverV2: AsyncSequence, Sendable where Element == JobQueueResult<JobID> {
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
    func finished(jobID: JobID, retain: Bool) async throws
    /// This is called to say job has failed to run and should be put aside
    func failed(jobID: JobID, error: any Error, retain: Bool) async throws
    /// stop serving jobs
    func stop() async
    /// shutdown queue
    func shutdownGracefully() async
    /// job queue context
    var context: JobQueueContext { get }
}

extension JobQueueDriverV2 {
    /// default version of waitUntilReady doing nothing
    public func waitUntilReady() async throws {}

    func retry(_ jobID: JobID, job: some JobInstanceProtocol, attempt: Int, options: JobRetryOptions) async throws {
        let jobRequest = JobRequest(name: job.name, parameters: job.parameters, queuedAt: job.queuedAt, attempt: attempt)
        return try await self.retry(jobID, jobRequest: jobRequest, options: options)
    }
}

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

import Foundation
import Logging
import NIOCore

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

/// Job queue protocol.
///
/// Defines how to push and pop job data off a queue
public protocol JobQueueDriver: AsyncSequence, Sendable where Element == JobQueueResult<JobID> {
    associatedtype JobID: CustomStringConvertible & Sendable
    /// Called when JobQueueHandler is initialised with this queue
    func onInit() async throws
    /// Register job definition with driver
    func registerJob<Parameters: JobParameters>(_ job: JobDefinition<Parameters>)
    /// Push Job onto queue
    /// - Parameters
    ///   - jobRequest: Job Request
    ///   - options: JobOptions
    /// - Returns: Identifier of queued jobs
    @discardableResult func push<Parameters: JobParameters>(_ jobRequest: JobRequest<Parameters>, options: JobOptions) async throws -> JobID
    /// Retry an existing Job
    /// - Parameters
    ///   - id: JobID
    ///   - jobRequest: Job Request
    ///   - options: JobOptions
    /// - Returns: Bool
    func retry<Parameters: JobParameters>(_ id: JobID, jobRequest: JobRequest<Parameters>, options: JobOptions) async throws
    /// This is called to say job has finished processing and it can be deleted
    func finished(jobID: JobID) async throws
    /// This is called to say job has failed to run and should be put aside
    func failed(jobID: JobID, error: any Error) async throws
    /// stop serving jobs
    func stop() async
    /// shutdown queue
    func shutdownGracefully() async
    /// get job queue metadata
    func getMetadata(_ key: String) async throws -> ByteBuffer?
    /// set job queue metadata
    func setMetadata(key: String, value: ByteBuffer) async throws
}

extension JobQueueDriver {
    // default version of onInit doing nothing
    public func onInit() async throws {}
}

extension JobQueueDriver {
    func retry(_ jobID: JobID, job: some JobInstanceProtocol, attempts: Int, options: JobOptions) async throws {
        let jobRequest = JobRequest(parameters: job.parameters, queuedAt: job.queuedAt, attempts: attempts)
        return try await self.retry(jobID, jobRequest: jobRequest, options: options)
    }
}

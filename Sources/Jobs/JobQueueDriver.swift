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

import Foundation
import Logging
import NIOCore

/// Job queue protocol.
///
/// Defines how to push and pop job data off a queue
public protocol JobQueueDriver: AsyncSequence, Sendable where Element == QueuedJob<JobID> {
    associatedtype JobID: CustomStringConvertible & Sendable

    /// Called when JobQueueHandler is initialised with this queue
    func onInit() async throws
    /// Push Job onto queue
    /// - Parameters
    ///   - buffer: ByteBuffer
    ///   - options: JobOptions
    /// - Returns: Identifier of queued jobs
    func push(_ buffer: ByteBuffer, options: JobOptions) async throws -> JobID
    /// This is called to say job has finished processing and it can be deleted
    func finished(jobId: JobID) async throws
    /// This is called to say job has failed to run and should be put aside
    func failed(jobId: JobID, error: any Error) async throws
    /// stop serving jobs
    func stop() async
    /// shutdown queue
    func shutdownGracefully() async
    /// get job queue metadata
    func getMetadata(_ key: String) async throws -> ByteBuffer?
    /// set job queue metadata
    func setMetadata(key: String, value: ByteBuffer) async throws
    /// Register job

    /// job registry
    var jobRegistry: JobRegistry { get }
}

extension JobQueueDriver {
    // default version of onInit doing nothing
    public func onInit() async throws {}

    ///  Register job
    /// - Parameters:
    ///   - id: Job Identifier
    ///   - maxRetryCount: Maximum number of times job is retried before being flagged as failed
    ///   - execute: Job code
    func registerJob(_ job: JobDefinition<some Codable & Sendable>) {
        self.jobRegistry.registerJob(job: job)
    }

    func push<Parameters: Sendable & Codable>(
        id: JobIdentifier<Parameters>,
        parameters: Parameters,
        options: JobOptions = .init()
    ) async throws -> JobID {
        let buffer = try self.jobRegistry.encode(id: id, parameters: parameters)
        let id = try await self.push(buffer, options: options)
        return id
    }

    func push(
        job: some JobInstanceProtocol,
        attempts: Int,
        options: JobOptions = .init()
    ) async throws -> JobID {
        let buffer = try self.jobRegistry.encode(job, attempts: attempts)
        let id = try await self.push(buffer, options: options)
        return id
    }

    func decode(queuedJob: QueuedJob<JobID>, logger: Logger) async throws -> any JobInstanceProtocol {
        do {
            return try self.jobRegistry.decode(queuedJob.jobBuffer)
        } catch let error as JobQueueError where error == .unrecognisedJobId {
            logger.debug("Failed to find Job with ID while decoding")
            try await self.failed(jobId: queuedJob.id, error: error)
            throw error
        } catch {
            logger.debug("Job failed to decode")
            try await self.failed(jobId: queuedJob.id, error: JobQueueError.decodeJobFailed)
            throw JobQueueError.decodeJobFailed
        }
    }
}

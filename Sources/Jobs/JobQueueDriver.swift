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
    /// Retry a job that's been in a queue
    ///  - Parameter jobId: JobID
    func retry(jobId: JobID, buffer: ByteBuffer, options: JobOptions) async throws
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
}

extension JobQueueDriver {
    // default version of onInit doing nothing
    public func onInit() async throws {}

    func encode(_ job: some JobInstanceProtocol, attempts: Int) throws -> ByteBuffer {
        let data = EncodableJob(
            id: job.id,
            parameters: job.parameters,
            queuedAt: job.queuedAt,
            attempts: attempts
        )
        return try JSONEncoder().encodeAsByteBuffer(data, allocator: ByteBufferAllocator())
    }

    func encode<Parameters: Codable & Sendable>(
        id: JobIdentifier<Parameters>,
        parameters: Parameters
    ) throws -> ByteBuffer {
        let jobRequest = EncodableJob(id: id, parameters: parameters, queuedAt: .now, attempts: 0)
        return try JSONEncoder().encodeAsByteBuffer(jobRequest, allocator: ByteBufferAllocator())
    }
}

//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

import Logging
import NIOCore

#if canImport(FoundationEssentials)
public import FoundationEssentials
#else
public import Foundation
#endif

/// Job queue protocol.
///
/// Defines how to push and pop job data off a queue
public protocol JobQueueDriver: JobQueueDriverV2, AsyncSequence, Sendable where Element == JobQueueResult<JobID> {
    /// This is called to say job has finished processing and it can be deleted
    func finished(jobID: JobID) async throws
    /// This is called to say job has failed to run and should be put aside
    func failed(jobID: JobID, error: any Error) async throws
}

extension JobQueueDriver {
    /// default version of worker ID is to return the string version of a UUID
    public var context: JobQueueContext { .init(workerID: UUID().uuidString, metadata: [:]) }
}

extension JobQueueDriver {
    /// This is called to say job has finished processing and it can be deleted
    public func finished(jobID: JobID, retain: Bool) async throws {
        try await self.finished(jobID: jobID)
    }
    /// This is called to say job has failed to run and should be put aside
    public func failed(jobID: JobID, error: any Error, retain: Bool) async throws {
        try await self.failed(jobID: jobID, error: error)
    }
}

/// Various data associated with a job queue
public struct JobQueueContext: Sendable {
    public enum MetadataValue: Sendable {
        case string(String)
        case integer(Int)
        case double(Double)
    }
    /// Job worker id. Unique to this node
    ///
    /// This is used to associate a job being processed with the node processing it. If a
    /// node stops responding then jobs associated with that node can be re-queued to be
    /// processed by another node.
    public let workerID: String
    /// Job queue name
    public let queueName: String
    /// Job worker metadata
    public let metadata: [String: MetadataValue]

    // initialize JobWorkerContext
    public init(workerID: String, queueName: String = "default", metadata: [String: MetadataValue]) {
        self.workerID = workerID
        self.queueName = queueName
        self.metadata = metadata
    }
}

/// Type returned from iterating a JobQueueDriver
///
/// The `JobQueueResult` can hold either a job instance or the error created when iterating
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

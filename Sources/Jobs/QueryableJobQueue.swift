//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

/// A protocol that extends `JobQueueDriver` by providing the ability to
/// query and iterate over jobs in the queue matching specific criteria.
///
/// Conformers must implement a method to perform an async operation for
/// each job that matches a given `JobQueueQuery`.
public protocol QueryableJobQueueDriver: JobQueueDriver {
    /// Iterates over each job of the given type matching the provided query,
    /// performing the specified operation for each.
    ///
    /// - Parameters:
    ///   - type: The type of the parameters for the job. This is used for type safety and decoding.
    ///   - query: A `JobQueueQuery` that specifies which jobs to include.
    ///   - perform: A closure that is called with the job's ID and data for each matched job.
    func withEachJob<Parameters: Sendable>(
        ofType type: Parameters.Type,
        matching query: JobQueueQuery<Parameters>,
        perform: @Sendable @escaping (JobID, JobInstanceData<Parameters>) async throws -> Void
    ) async throws
}

/// Default implementation for `withEachJob` that includes all jobs of the given type,
/// using the default query options.
extension QueryableJobQueueDriver {
    /// Iterates over each job of the specified type using the default `JobQueueQuery` options,
    /// performing the operation for each.
    ///
    /// - Parameters:
    ///   - type: The type of the parameters for the job.
    ///   - perform: A closure called for each job.
    public func withEachJob<Parameters: Sendable>(
        ofType type: Parameters.Type,
        perform: @Sendable @escaping (JobID, JobInstanceData<Parameters>) async throws -> Void
    ) async throws {
        try await withEachJob(
            ofType: Parameters.self,
            matching: JobQueueQuery<Parameters>()
        ) { id, job in
            try await perform(id, job)
        }
    }
}

/// Represents a set of options for filtering and querying jobs in the job queue.
public struct JobQueueQuery<Parameters: Sendable>: Sendable {
    /// Whether to include cancelled jobs in the query result. Defaults to `true`.
    public var includeCancelledJobs = true
    /// Whether to include failed jobs in the query result. Defaults to `true`.
    public var includeFailedJobs = true
    /// Whether to include completed jobs in the query result. Defaults to `true`.
    public var includeCompletedJobs = true
    /// Whether to include delayed jobs in the query result. Defaults to `true`.
    public var includeDelayedJobs = true

    /// Creates a new `JobQueueQuery` with the default options, including all jobs.
    public init() {}
}

//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

#if compiler(>=6.2.3)

public import NIOCore

#if canImport(FoundationEssentials)
public import FoundationEssentials
#else
public import Foundation
#endif

@_spi(JobsAPI) public struct JobAPIMetadata: Sendable {
    @nonexhaustive
    public enum Status: String, Sendable {
        case pending
        case processing
        case completed
        case failed
        case cancelled
        case paused
    }
    public let id: UUID
    public let name: String
    public let createdAt: Date
    public let completedAt: Date?
    public let status: Status

    public init(id: UUID, name: String, createdAt: Date, completedAt: Date? = nil, status: JobAPIMetadata.Status) {
        self.id = id
        self.name = name
        self.createdAt = createdAt
        self.completedAt = completedAt
        self.status = status
    }
}

@_spi(JobsAPI) public struct GetJobsResponse: Sendable {
    public let paginationToken: String?
    public let jobs: [JobAPIMetadata]

    public init(paginationToken: String? = nil, jobs: [JobAPIMetadata]) {
        self.paginationToken = paginationToken
        self.jobs = jobs
    }
}

@_spi(JobsAPI) public struct GetJobResponse: Sendable {
    public let jobMetadata: JobAPIMetadata
    public let jobParameters: ByteBuffer

    public init(jobMetadata: JobAPIMetadata, jobParameters: ByteBuffer) {
        self.jobMetadata = jobMetadata
        self.jobParameters = jobParameters
    }
}

@_spi(JobsAPI) public protocol JobsAPI {
    /// Get list of job descriptions from job queue
    /// - Parameters:
    ///   - maxNumber: Maximum number of jobs to return
    ///   - paginationToken: Optional pagination token
    /// - Returns: Array of job descriptions and pagination token
    func getJobs(maxNumber: Int, paginationToken: String?) async throws -> GetJobsResponse
    /// Get job parameters
    /// - Parameters:
    ///   - id: Job ID
    /// - Returns: Job parameters
    func getJob(id: UUID) async throws -> GetJobResponse?
}

#endif

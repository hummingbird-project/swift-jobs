//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2024 the Hummingbird authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See hummingbird/CONTRIBUTORS.txt for the list of Hummingbird authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Foundation

/// Protocol for a Job
protocol JobInstanceProtocol: Sendable {
    /// Parameters job requries
    associatedtype Parameters: Codable & Sendable
    /// Job Type identifier
    var id: JobIdentifier<Parameters> { get }
    /// Maximum number of times a job will be retried before being classed as failed
    var maxRetryCount: Int { get }
    /// Time job was queued
    var queuedAt: Date { get }
    /// Number of attempts so far
    var attempts: Int { get }
    /// Job parameters
    var parameters: Parameters { get }
    /// Function to execute the job
    func execute(context: JobContext) async throws
}

extension JobInstanceProtocol {
    /// Name of job type
    public var name: String {
        id.name
    }

    /// Number of remaining attempts
    public var remainingAttempts: Int {
        maxRetryCount - attempts
    }

    /// If job failed after n number of attempts
    public var didFail: Bool {
        attempts >= maxRetryCount
    }
}

/// Job decoded from Queue
///
/// Includes everything needed to run the job plus any other data that was encoded
/// with the job
struct JobInstance<Parameters: Codable & Sendable>: JobInstanceProtocol {
    /// job definition
    let job: JobDefinition<Parameters>
    /// job parameters
    let data: JobInstanceData<Parameters>
    /// job identifier
    var id: JobIdentifier<Parameters> { self.job.id }
    /// max retry count for a job
    var maxRetryCount: Int { self.job.maxRetryCount }
    /// Time job was queued
    var queuedAt: Date { self.data.queuedAt }
    /// Number of attempts so far
    var attempts: Int { self.data.attempts }
    /// Job parameters
    var parameters: Parameters { self.data.parameters }

    func execute(context: JobContext) async throws {
        try await self.job.execute(self.data.parameters, context: context)
    }

    init(job: JobDefinition<Parameters>, data: JobInstanceData<Parameters>) throws {
        self.job = job
        self.data = data
    }
}

/// Data attach to a job
struct JobInstanceData<Parameters: Codable & Sendable>: Codable {
    /// Job parameters
    let parameters: Parameters
    /// Date job was queued
    let queuedAt: Date
    /// Number of attempts so far
    let attempts: Int

    // keep JSON strings small to improve decode speed
    private enum CodingKeys: String, CodingKey {
        case parameters = "p"
        case queuedAt = "q"
        case attempts = "a"
    }
}

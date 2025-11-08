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

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Request to run job, pushed to job queue
public struct JobRequest<Parameters: Sendable & Codable>: Encodable {
    /// Job name
    public let name: String
    /// Job details
    public let data: JobInstanceData<Parameters>

    @usableFromInline
    package init(
        name: String,
        parameters: Parameters,
        queuedAt: Date,
        attempt: Int,
        nextScheduledAt: Date? = nil
    ) {
        self.name = name
        self.data = .init(
            parameters: parameters,
            queuedAt: queuedAt,
            attempt: attempt,
            nextScheduledAt: nextScheduledAt
        )
    }

    init(jobInstance: JobInstance<Parameters>, attempt: Int) {
        self.name = .init(jobInstance.name)
        self.data = .init(
            parameters: jobInstance.parameters,
            queuedAt: jobInstance.queuedAt,
            attempt: attempt
        )
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: _JobCodingKey.self)
        let childEncoder = container.superEncoder(
            forKey: .init(stringValue: name, intValue: nil)
        )
        try self.data.encode(to: childEncoder)
    }
}

extension JobRequest: SchedulableJobRequest {
    /// Added so it's possible for the scheduler to add date partitions
    package func push<Queue: JobQueueDriver>(
        to jobQueue: JobQueue<Queue>,
        options: Queue.JobOptions = .init(delayUntil: .now)
    ) async throws -> Queue.JobID {
        try await jobQueue.push(jobRequest: self, options: options)
    }
}

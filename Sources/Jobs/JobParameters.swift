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

/// Defines job parameters and identifier
public protocol JobParameters: Codable, Sendable {
    /// Job type name
    static var jobName: String { get }
}

extension JobParameters {
    // Job name identifier
    public static var jobNameIdentifier: JobName<Self> {
        .init(jobName)
    }
}

extension JobParameters {
    /// Added so it's possible for the scheduler to add date partitions
    internal func push<Queue: JobQueueDriver>(
        to jobQueue: JobQueue<Queue>,
        currentSchedule: Date,
        nextScheduledAt: Date?,
        options: Queue.JobOptions = .init()
    ) async throws -> Queue.JobID {
        try await jobQueue.schedule(
            self,
            currentSchedule: currentSchedule,
            nextScheduledAt: nextScheduledAt,
            options: options
        )
    }
}

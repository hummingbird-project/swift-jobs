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

import struct Foundation.Date

/// Defines job parameters and identifier
public protocol JobParameters: Codable, Sendable {
    /// Job type name
    static var jobName: String { get }
}

extension JobParameters {
    /// Added so it is possible to push JobParameters referenced as Existentials to a Job queue
    @discardableResult public func push<Queue: JobQueueDriver>(
        to jobQueue: JobQueue<Queue>,
        options: JobOptions = .init()
    ) async throws -> Queue.JobID {
        try await jobQueue.push(self, options: options)
    }
}

extension JobParameters {
    /// Added so it's possible for the scheduler to add date partitions
    internal func push<Queue: JobQueueDriver>(
        to jobQueue: JobQueue<Queue>,
        currentSchedule: Date,
        nextScheduledAt: Date?,
        options: JobOptions = .init()
    ) async throws -> Queue.JobID {
        try await jobQueue.schedule(
            self,
            currentSchedule: currentSchedule,
            nextScheduledAt: nextScheduledAt,
            options: options
        )
    }
}

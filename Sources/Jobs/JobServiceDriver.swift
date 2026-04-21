//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

/// Protocol for JobQueue clean up Options
public protocol JobQueueCleanupOptionsProtocol: Sendable {
    static var `default`: Self { get }
}

/// A queue that can be used with the JobService type
public protocol JobServiceDriver: JobQueueDriver, JobMetadataDriver {
    associatedtype CleanupOptions: JobQueueCleanupOptionsProtocol

    /// Schedule regular job queue cleanup
    func scheduleQueueCleanup(_ schedule: inout JobSchedule, options: CleanupOptions)
}

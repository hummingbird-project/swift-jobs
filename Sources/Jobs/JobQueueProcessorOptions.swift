//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// JobQueueHandler Options
public struct JobQueueProcessorOptions: Sendable {
    /// Defines how often if at all a lock should be acquired
    public struct ExclusiveLock: Sendable {
        enum Value: Sendable {
            case inactive
            case acquire(every: TimeInterval, for: TimeInterval)
        }
        let value: Value
        /// Don't try to acquire worker lock
        public static var inactive: Self { .init(value: .inactive) }
        /// Acquire lock
        /// - Parameters:
        ///   - every: Frequency of acquiring the lock
        ///   - for: How long the lock should be acquired for
        public static func acquire(every: Duration, for: Duration) -> Self {
            precondition(`for` > every, "The time between acquiring each lock shoud be less then the time you acquire the lock for.")
            return .init(value: .acquire(every: .init(duration: every), for: .init(duration: `for`)))
        }
    }

    /// Number of concurrent jobs being processed at one time
    public var numWorkers: Int
    /// Timeout after graceful shutdown has been triggered, before jobs are cancelled
    public var gracefulShutdownTimeout: Duration
    /// worker active lock configuration
    public var workerActiveLock: ExclusiveLock

    /// Initialize a JobQueueProcessorOptions
    ///
    /// - Parameters:
    ///   - numWorkers: Number of concurrent jobs to be processed at one time.
    ///   - gracefulShutdownTimeout: Timeout after graceful shutdown has been triggered, before jobs are cancelled.
    ///   - workerActiveLock: How frequently to update the worker active lock and how long to hold it. This is used
    ///     to indicate if a worker is running. If another process can acquire the lock then the worker must have
    ///     stopped.
    public init(
        numWorkers: Int = 16,
        gracefulShutdownTimeout: Duration = .seconds(30),
        workerActiveLock: ExclusiveLock = .acquire(every: .milliseconds(300000), for: .milliseconds(450000))
    ) {
        self.numWorkers = numWorkers
        self.gracefulShutdownTimeout = gracefulShutdownTimeout
        self.workerActiveLock = workerActiveLock
    }
}

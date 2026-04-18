//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//
#if canImport(FoundationEssentials)
public import FoundationEssentials
#else
public import Foundation
#endif

/// Error used to implement custom retry implementations
///
/// When a job throws this error the JobQueueProcessor recognises it and
/// will delay retrying of the job until the date specified. It will allows
/// control over the attempt count is updated.
///
/// This can be used in situations where a resource is not ready for a job
/// and we want to delay until that resource is available
public struct JobSleep: Error {
    public struct AttemptAction: Sendable {
        enum Action {
            case doNothing
            case reset
            case increment
        }
        let action: Action

        /// Don't increment job attempts
        public static var doNothing: Self { .init(action: .doNothing) }
        /// Reset job attempts to the first attempt
        public static var reset: Self { .init(action: .reset) }
        /// Increment job attemtps
        public static var increment: Self { .init(action: .increment) }

        func updateAttempt(_ attempt: Int) -> Int {
            switch self.action {
            case .doNothing: attempt
            case .increment: attempt + 1
            case .reset: 1
            }
        }
    }
    let delayUntil: Date
    let attemptAction: AttemptAction

    ///  Initialize JobSleep
    /// - Parameters:
    ///   - delayUntil: How long we should wait until before queuing job again
    ///   - attempts: How we should update the attempt number
    public init(until: Date, attempts: AttemptAction = .doNothing) {
        self.delayUntil = until
        self.attemptAction = attempts
    }

    ///  Initialize JobSleep
    /// - Parameters:
    ///   - duration: How long we should wait before queuing job again
    ///   - attempts: How we should update the attempt number
    public init(for duration: Duration, attempts: AttemptAction = .doNothing) {
        self.delayUntil = Date.now._advanced(by: duration)
        self.attemptAction = attempts
    }
}

//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2025 the Hummingbird authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See hummingbird/CONTRIBUTORS.txt for the list of Hummingbird authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Foundation

/// Define whether a job should be retried or not and how long before retrying
public enum JobRetryResult {
    case retryAfter(TimeInterval)
    case dontRetry
}

/// Strategy deciding whether we should retry a failed job
public protocol JobRetryStrategy: Sendable {
    ///  Calculate whether we should retry a failed job and how long we wait before retrying
    /// - Parameters:
    ///   - attempt: Attempt number of job
    ///   - error: Error that was thrown by failed job
    /// - Returns: Whether we should retry a failed job
    func shouldRetry(attempt: Int, error: Error) -> Bool

    ///  Calculate backoff amount
    /// - Parameter attempt: Attempt number
    /// - Returns: Retry backoff
    func calculateBackoff(attempt: Int) -> TimeInterval
}

/// Never retry failed jobs
public struct NoRetryJobRetryStrategy: JobRetryStrategy {
    public func shouldRetry(attempt: Int, error: Error) -> Bool { false }
    public func calculateBackoff(attempt: Int) -> TimeInterval { 0 }

}

extension JobRetryStrategy where Self == NoRetryJobRetryStrategy {
    /// Dont retry job strategy
    public static var dontRetry: Self { .init() }
}

/// Retry failed jobs with an exponentially increasing delay
///
/// See https://en.wikipedia.org/wiki/Exponential_backoff
public struct ExponentialJitterJobRetryStrategy: JobRetryStrategy {
    /// Maximum attempts - default is 4
    public var maxAttempts: Int
    /// Maximum Delay - default is 120.0 seconds
    public var maxBackoff: Double
    /// Minimum jitter - default is 0 seconds
    public var minJitter: TimeInterval
    /// Maximum jitter - default is 10 seconds
    public var maxJitter: TimeInterval

    public init(maxAttempts: Int = 4, maxBackoff: TimeInterval = 120, minJitter: TimeInterval = 0, maxJitter: TimeInterval = 10) {
        self.maxAttempts = maxAttempts
        self.maxBackoff = maxBackoff
        self.maxJitter = maxJitter
        self.minJitter = minJitter
    }

    public func shouldRetry(attempt: Int, error: any Error) -> Bool {
        attempt < maxAttempts
    }

    public func calculateBackoff(attempt: Int) -> TimeInterval {
        let exp = exp2(Double(attempt))
        let delay = min(exp, self.maxBackoff)
        return TimeInterval(Double.random(in: self.minJitter..<self.maxJitter) + delay)
    }
}

extension JobRetryStrategy where Self == ExponentialJitterJobRetryStrategy {
    ///  Retry job with exponential backoff
    /// - Parameters:
    ///   - maxAttempts: Maximum retry attempts
    ///   - maxBackoff: Maximum backoff returned
    ///   - minJitter: Minimum jitter
    ///   - maxJitter: Maximum jitter
    public static func exponentialJitter(
        maxAttempts: Int = 4,
        maxBackoff: TimeInterval = 120,
        minJitter: TimeInterval = 0,
        maxJitter: TimeInterval = 10
    ) -> Self { .init(maxAttempts: maxAttempts, maxBackoff: maxBackoff, minJitter: minJitter, maxJitter: maxJitter) }
}

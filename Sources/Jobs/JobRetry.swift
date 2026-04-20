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

#if canImport(Glibc)
import Glibc
#elseif canImport(Musl)
import Musl
#elseif canImport(Darwin)
import Darwin.C
#elseif canImport(Android)
import Android
#else
#error("Unsupported platform")
#endif

/// Strategy deciding whether we should retry a failed job and backoff before retrying job
public protocol JobRetryStrategy: Sendable {
    /// Should we retry a failed job
    /// - Parameters:
    ///   - attempt: Attempt number of job
    ///   - error: Error that was thrown by failed job
    /// - Returns: Whether we should retry a failed job
    func shouldRetry(attempt: Int, error: any Error) -> Bool

    ///  Calculate backoff amount
    /// - Parameter attempt: Attempt number
    /// - Returns: Retry backoff
    func calculateBackoff(attempt: Int) -> Duration
}

/// Never retry failed jobs
public struct NoRetryJobRetryStrategy: JobRetryStrategy {
    public func shouldRetry(attempt: Int, error: any Error) -> Bool { false }
    public func calculateBackoff(attempt: Int) -> Duration { .seconds(0) }
}

extension JobRetryStrategy where Self == NoRetryJobRetryStrategy {
    /// Dont retry job strategy
    public static var dontRetry: Self { .init() }
}

/// Retry failed jobs with an exponentially increasing backoff
///
/// See https://en.wikipedia.org/wiki/Exponential_backoff
public struct ExponentialJitterJobRetryStrategy: JobRetryStrategy {
    /// Maximum attempts - default is 4
    public var maxAttempts: Int
    /// Maximum Delay - default is 120.0 seconds
    public var maxBackoff: Duration
    /// Minimum jitter as a ratio of calculated backoff - default is -0.5
    public var minJitter: Double
    /// Maximum jitter as a ratio of calculated backoff - default is 0.5
    public var maxJitter: Double

    public init(maxAttempts: Int = 4, maxBackoff: Duration = .seconds(120), minJitter: Double = -0.5, maxJitter: Double = 0.5) {
        self.maxAttempts = maxAttempts
        self.maxBackoff = maxBackoff
        self.maxJitter = maxJitter
        self.minJitter = minJitter
    }

    public func shouldRetry(attempt: Int, error: any Error) -> Bool {
        attempt < maxAttempts
    }

    public func calculateBackoff(attempt: Int) -> Duration {
        let exp = Duration.seconds(exp2(Double(attempt)))
        let delay = min(exp, self.maxBackoff)
        let jitter = Double.random(in: minJitter..<maxJitter)
        return delay * (1 + jitter)
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
        maxBackoff: Duration = .seconds(120),
        minJitter: Double = -0.5,
        maxJitter: Double = 0.5
    ) -> Self { .init(maxAttempts: maxAttempts, maxBackoff: maxBackoff, minJitter: minJitter, maxJitter: maxJitter) }
}

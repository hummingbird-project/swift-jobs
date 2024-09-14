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

import struct Foundation.TimeInterval

/// JobQueueOptions
public struct JobQueueOptions: Sendable {
    /// Maximum Backoff - default is 120.0 seconds
    public var maximumBackoff: TimeInterval
    /// Maximum jitter - default is 15 seconds
    public var maxJitter: TimeInterval
    /// Minimum jitter - default is 5 seconds
    public var minJitter: TimeInterval
    /// Computes a random jitter between minJitter and maxJitter
    internal var jitter: Double {
        Double.random(in: self.minJitter..<self.maxJitter)
    }

    /// Initialize a JobQueueOptions
    /// The current backoff computation uses jitters
    /// see Wikipedia if  not familiar  with the topic https://en.wikipedia.org/wiki/Exponential_backoff
    /// - Parameters:
    ///   - maximumBackoff: maximum backoff allowed, default 120 seconds
    ///   - maxJitter: maximum jitter
    ///   - minJitter: minimum jitter
    public init(
        maximumBackoff: TimeInterval = 120.0,
        maxJitter: TimeInterval = 15.0,
        minJitter: TimeInterval = 5.0
    ) {
        self.maximumBackoff = maximumBackoff
        self.maxJitter = maxJitter
        self.minJitter = minJitter
    }
}

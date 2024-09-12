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
    /// Backoff factor default 2, but isn't used
    /// since we ar esuing exp2 instead of pow
    public var backoffFactor: Double
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

    public init(
        backoffFactor: Double = 2,
        maximumBackoff: TimeInterval = 120.0,
        maxJitter: TimeInterval = 15.0,
        minJitter: TimeInterval = 5.0
    ) {
        self.backoffFactor = backoffFactor
        self.maximumBackoff = maximumBackoff
        self.maxJitter = maxJitter
        self.minJitter = minJitter
    }
}

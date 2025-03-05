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
    let jobRetryStrategy: JobRetryStrategy
    let driverRetryStrategy: JobRetryStrategy

    /// Initialize a JobQueueOptions
    public init(
        jobRetryStrategy: any JobRetryStrategy = .exponentialJitter(),
        driverRetryStrategy: any JobRetryStrategy = .exponentialJitter(maxAttempts: .max, maxBackoff: 120)
    ) {
        self.jobRetryStrategy = jobRetryStrategy
        self.driverRetryStrategy = driverRetryStrategy
    }
}

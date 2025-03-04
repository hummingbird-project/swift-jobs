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
    let retryStrategy: JobRetryStrategy

    /// Initialize a JobQueueOptions
    public init(defaultRetryStrategy: any JobRetryStrategy = .exponentialJitter()) {
        self.retryStrategy = defaultRetryStrategy
    }
}

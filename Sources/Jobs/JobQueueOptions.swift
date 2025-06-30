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

/// Options associated with a JobQueue
public struct JobQueueOptions: Sendable {
    /// Default job retry strategy for the job queue
    let retryStrategy: JobRetryStrategy

    /// Initialize a JobQueueOptions
    ///
    /// - Parameters:
    ///   - defaultRetryStrategy: Default job retry strategy for the job queue
    public init(
        defaultRetryStrategy: any JobRetryStrategy = .exponentialJitter()
    ) {
        self.retryStrategy = defaultRetryStrategy
    }
}

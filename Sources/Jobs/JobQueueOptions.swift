//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

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

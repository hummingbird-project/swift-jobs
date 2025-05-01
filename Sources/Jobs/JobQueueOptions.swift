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

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// JobQueueOptions
public struct JobQueueOptions: Sendable {
    /// Default job retry strategy for the job queue
    let retryStrategy: JobRetryStrategy
    /// Timeout after graceful shutdown has been triggered, before jobs are cancelled
    let gracefulShutdownTimeout: Duration

    /// Initialize a JobQueueOptions
    ///
    /// - Parameters:
    ///   - defaultRetryStrategy: Default job retry strategy for the job queue
    ///   - gracefulShutdownTimeout: Timeout after graceful shutdown has been triggered, before jobs are cancelled
    public init(
        defaultRetryStrategy: any JobRetryStrategy = .exponentialJitter(),
        gracefulShutdownTimeout: Duration = .seconds(30)
    ) {
        self.retryStrategy = defaultRetryStrategy
        self.gracefulShutdownTimeout = gracefulShutdownTimeout
    }
}

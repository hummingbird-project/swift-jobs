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

/// JobQueueHandler Options
public struct JobQueueProcessorOptions: Sendable {
    /// Number of concurrent jobs being processed at one time
    let numWorkers: Int
    /// Timeout after graceful shutdown has been triggered, before jobs are cancelled
    let gracefulShutdownTimeout: Duration

    /// Initialize a JobQueueHandlerOptions
    ///
    /// - Parameters:
    ///   - numWorkers: Number of concurrent jobs being processed at one time
    ///   - gracefulShutdownTimeout: Timeout after graceful shutdown has been triggered, before jobs are cancelled
    public init(
        numWorkers: Int = 1,
        gracefulShutdownTimeout: Duration = .seconds(30)
    ) {
        self.numWorkers = numWorkers
        self.gracefulShutdownTimeout = gracefulShutdownTimeout
    }
}

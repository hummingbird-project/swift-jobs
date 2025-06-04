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
public struct JobQueueHandlerOptions: Sendable {
    /// Timeout after graceful shutdown has been triggered, before jobs are cancelled
    let gracefulShutdownTimeout: Duration

    /// Initialize a JobQueueHandlerOptions
    ///
    /// - Parameters:
    ///   - gracefulShutdownTimeout: Timeout after graceful shutdown has been triggered, before jobs are cancelled
    public init(
        gracefulShutdownTimeout: Duration = .seconds(30)
    ) {
        self.gracefulShutdownTimeout = gracefulShutdownTimeout
    }
}

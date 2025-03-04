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

/// Job definition type
public struct JobDefinition<Parameters: JobParameters>: Sendable {
    let retryStrategy: any JobRetryStrategy
    let _execute: @Sendable (Parameters, JobContext) async throws -> Void

    ///  Initialize JobDefinition
    /// - Parameters:
    ///   - parameters: Job parameter type
    ///   - retryStrategy: Retry strategy for failed jobs
    ///   - execute: Closure that executes job
    public init(
        parameters: Parameters.Type = Parameters.self,
        retryStrategy: any JobRetryStrategy = .dontRetry,
        execute: @escaping @Sendable (Parameters, JobContext) async throws -> Void
    ) where Parameters: JobParameters {
        self.retryStrategy = retryStrategy
        self._execute = execute
    }

    ///  Initialize JobDefinition
    /// - Parameters:
    ///   - parameters: Job parameter type
    ///   - maxRetryCount: Maxiumum times this job will be retried if it fails
    ///   - execute: Closure that executes job
    public init(
        parameters: Parameters.Type = Parameters.self,
        maxRetryCount: Int,
        execute: @escaping @Sendable (Parameters, JobContext) async throws -> Void
    ) where Parameters: JobParameters {
        self.retryStrategy = ExponentialJitterJobRetryStrategy(maxAttempts: maxRetryCount)
        self._execute = execute
    }

    func execute(_ parameters: Parameters, context: JobContext) async throws {
        try await self._execute(parameters, context)
    }
}

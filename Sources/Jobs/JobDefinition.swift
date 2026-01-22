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
public struct JobDefinition<Parameters: Codable & Sendable>: Sendable {
    let name: String
    let retryStrategy: any JobRetryStrategy
    let timeout: Duration?
    let leaseDuration: Duration?
    let _execute: @Sendable (Parameters, JobExecutionContext) async throws -> Void

    ///  Initialize JobDefinition
    /// - Parameters:
    ///   - parameters: Job parameter type
    ///   - retryStrategy: Retry strategy for failed jobs
    ///   - timeout: Timeout for long running jobs
    ///   - execute: Closure that executes job
    public init(
        parameters: Parameters.Type = Parameters.self,
        retryStrategy: any JobRetryStrategy = .dontRetry,
        timeout: Duration? = nil,
        execute: @escaping @Sendable (Parameters, JobExecutionContext) async throws -> Void
    ) where Parameters: JobParameters {
        self.name = parameters.jobName
        self.retryStrategy = retryStrategy
        self._execute = execute
        self.timeout = timeout
        self.leaseDuration = nil
    }

    ///  Initialize JobDefinition
    /// - Parameters:
    ///   - name: Job name
    ///   - parameters: Job parameter type
    ///   - retryStrategy: Retry strategy for failed jobs
    ///   - timeout: Timeout for long running jobs
    ///   - execute: Closure that executes job
    public init(
        name: JobName<Parameters>,
        parameters: Parameters.Type = Parameters.self,
        retryStrategy: any JobRetryStrategy = .dontRetry,
        timeout: Duration? = nil,
        execute: @escaping @Sendable (Parameters, JobExecutionContext) async throws -> Void
    ) {
        self.name = name.name
        self.retryStrategy = retryStrategy
        self._execute = execute
        self.timeout = timeout
        self.leaseDuration = nil
    }

    ///  Initialize JobDefinition
    /// - Parameters:
    ///   - name: Job name
    ///   - parameters: Job parameter type
    ///   - retryStrategy: Retry strategy for failed jobs
    ///   - timeout: Timeout for long running jobs
    ///   - leaseDuration: Duration how long should the last heartbeat from now before a job is considered orphaned
    ///   - execute: Closure that executes job
    public init(
        name: JobName<Parameters>,
        parameters: Parameters.Type = Parameters.self,
        retryStrategy: any JobRetryStrategy = .dontRetry,
        timeout: Duration? = nil,
        leaseDuration: Duration,
        execute: @escaping @Sendable (Parameters, JobExecutionContext) async throws -> Void
    ) {
        self.name = name.name
        self.retryStrategy = retryStrategy
        self._execute = execute
        self.timeout = timeout
        self.leaseDuration = leaseDuration
    }

    ///  Initialize JobDefinition
    /// - Parameters:
    ///   - parameters: Job parameter type
    ///   - maxRetryCount: Maxiumum times this job will be retried if it fails
    ///   - execute: Closure that executes job
    @available(*, deprecated, renamed: "init(parameters:retryStrategy:timeout:execute:)")
    public init(
        parameters: Parameters.Type = Parameters.self,
        maxRetryCount: Int,
        execute: @escaping @Sendable (Parameters, JobExecutionContext) async throws -> Void
    ) where Parameters: JobParameters {
        self.name = parameters.jobName
        self.retryStrategy = ExponentialJitterJobRetryStrategy(maxAttempts: maxRetryCount)
        self._execute = execute
        self.timeout = nil
        self.leaseDuration = nil
    }

    func execute(_ parameters: Parameters, context: JobExecutionContext) async throws {
        try await self._execute(parameters, context)
    }
}

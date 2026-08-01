//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

public struct JobDefinitionOptions: OptionSet, Sendable {
    public var rawValue: Int

    public init(rawValue: Int) {
        self.rawValue = rawValue
    }
    /// When job has completed or failed we should not record its state even if the
    /// job queue driver has indicated it will store the state.
    public static var doNotRetain: Self { self.init(rawValue: 1 << 0) }
}

/// Job definition type
public struct JobDefinition<Parameters: Codable & Sendable>: Sendable {
    let name: String
    public var retryStrategy: any JobRetryStrategy
    public var timeout: Duration?
    public var options: JobDefinitionOptions
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
        self.options = []
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
        self.options = []
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
        self.options = []
    }

    func execute(_ parameters: Parameters, context: JobExecutionContext) async throws {
        try await self._execute(parameters, context)
    }
}

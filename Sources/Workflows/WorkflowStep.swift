//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

import Jobs

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

public struct EmptyOutput: Codable, Sendable {}

/// Job definition type
public struct WorkflowStep<Input: Codable & Sendable, Output: Codable & Sendable>: Sendable {
    let name: String
    let retryStrategy: any JobRetryStrategy
    let timeout: Duration?
    let _execute: @Sendable (Input, WorkflowExecutionContext) async throws -> Output

    ///  Initialize JobDefinition
    /// - Parameters:
    ///   - parameters: Job parameter type
    ///   - retryStrategy: Retry strategy for failed jobs
    ///   - timeout: Timeout for long running jobs
    ///   - execute: Closure that executes job
    public init(
        name: String,
        parameters: Input.Type = Input.self,
        retryStrategy: any JobRetryStrategy = .dontRetry,
        timeout: Duration? = nil,
        execute: @escaping @Sendable (Input, WorkflowExecutionContext) async throws -> Output
    ) {
        self.name = name
        self.retryStrategy = retryStrategy
        self._execute = execute
        self.timeout = timeout
    }

    ///  Initialize JobDefinition
    /// - Parameters:
    ///   - parameters: Job parameter type
    ///   - retryStrategy: Retry strategy for failed jobs
    ///   - timeout: Timeout for long running jobs
    ///   - execute: Closure that executes job
    public init(
        name: String,
        parameters: Input.Type = Input.self,
        retryStrategy: any JobRetryStrategy = .dontRetry,
        timeout: Duration? = nil,
        execute: @escaping @Sendable (Input, WorkflowExecutionContext) async throws -> Void
    ) where Output == EmptyOutput {
        self.name = name
        self.retryStrategy = retryStrategy
        self._execute = {
            try await execute($0, $1)
            return EmptyOutput()
        }
        self.timeout = timeout
    }

    ///  Initialize JobDefinition
    /// - Parameters:
    ///   - parameters: Job parameter type
    ///   - retryStrategy: Retry strategy for failed jobs
    ///   - timeout: Timeout for long running jobs
    ///   - execute: Closure that executes job
    public init(
        name: String,
        retryStrategy: any JobRetryStrategy = .dontRetry,
        timeout: Duration? = nil,
        execute: @escaping @Sendable (WorkflowExecutionContext) async throws -> Void
    ) where Output == EmptyOutput {
        self.name = name
        self.retryStrategy = retryStrategy
        self._execute = { _, context in
            try await execute(context)
            return EmptyOutput()
        }
        self.timeout = timeout
    }

    ///  Initialize JobDefinition
    /// - Parameters:
    ///   - retryStrategy: Retry strategy for failed jobs
    ///   - timeout: Timeout for long running jobs
    ///   - execute: Closure that executes job
    public init(
        name: String,
        retryStrategy: any JobRetryStrategy = .dontRetry,
        timeout: Duration? = nil,
        execute: @escaping @Sendable (WorkflowExecutionContext) async throws -> Output
    ) where Input == EmptyOutput {
        self.name = name
        self.retryStrategy = retryStrategy
        self._execute = { _, context in
            try await execute(context)
        }
        self.timeout = timeout
    }

    ///  Initialize JobDefinition
    /// - Parameters:
    ///   - retryStrategy: Retry strategy for failed jobs
    ///   - timeout: Timeout for long running jobs
    ///   - execute: Closure that executes job
    public init(
        name: String,
        retryStrategy: any JobRetryStrategy = .dontRetry,
        timeout: Duration? = nil,
        execute: @escaping @Sendable (WorkflowExecutionContext) async throws -> Void
    ) where Input == EmptyOutput, Output == EmptyOutput {
        self.name = name
        self.retryStrategy = retryStrategy
        self._execute = { _, context in
            try await execute(context)
            return EmptyOutput()
        }
        self.timeout = timeout
    }
}

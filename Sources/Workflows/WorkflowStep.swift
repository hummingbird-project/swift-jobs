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

/// Job definition type
public struct WorkflowStep<Input: Codable & Sendable, Output>: Sendable {
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
}

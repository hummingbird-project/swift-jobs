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
    let maxRetryCount: Int
    let _execute: @Sendable (Parameters, JobContext) async throws -> Void

    ///  Initialize JobDefinition
    /// - Parameters:
    ///   - id: Job identifier
    ///   - maxRetryCount: Maxiumum times this job will be retried if it fails
    ///   - execute: Closure that executes job
    public init(
        maxRetryCount: Int = 0,
        execute: @escaping @Sendable (Parameters, JobContext) async throws -> Void
    ) {
        self.maxRetryCount = maxRetryCount
        self._execute = execute
    }

    ///  Initialize JobDefinition
    /// - Parameters:
    ///   - parameters: Job parameter type
    ///   - maxRetryCount: Maxiumum times this job will be retried if it fails
    ///   - execute: Closure that executes job
    public init(
        parameters: Parameters.Type = Parameters.self,
        maxRetryCount: Int = 0,
        execute: @escaping @Sendable (Parameters, JobContext) async throws -> Void
    ) where Parameters: JobParameters {
        self.init(maxRetryCount: maxRetryCount, execute: execute)
    }

    func execute(_ parameters: Parameters, context: JobContext) async throws {
        try await self._execute(parameters, context)
    }
}

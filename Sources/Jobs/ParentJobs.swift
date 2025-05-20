//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2021-2025 the Hummingbird authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See hummingbird/CONTRIBUTORS.txt for the list of Hummingbird authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

extension JobQueue {
    ///  Initialize JobDefinition
    /// - Parameters:
    ///   - name: Job name
    ///   - parameters: Job parameter type
    ///   - retryStrategy: Retry strategy for failed jobs
    ///   - timeout: Timeout for long running jobs
    ///   - execute: Closure that executes job
    public func registerParentJob<Parameters: Sendable & Codable, Output: Sendable & Codable>(
        name: JobName<Parameters>,
        parameters: Parameters.Type = Parameters.self,
        children: [JobName<Output>],
        retryStrategy: (any JobRetryStrategy)? = nil,
        timeout: Duration? = nil,
        execute: @escaping @Sendable (Parameters, JobExecutionContext) async throws -> Output
    ) {
        self.logger.info("Registered Parent Job", metadata: ["JobName": .string(name.name), "Children": .array(children.map { .string($0.name) })])
        let job = JobDefinition(
            name: name,
            parameters: parameters,
            retryStrategy: retryStrategy ?? self.options.retryStrategy,
            timeout: timeout
        ) { parameters, context in
            let result = try await execute(parameters, context)
            self.logger.debug("Triggered Jobs", metadata: ["JobName": .string(name.name), "Children": .array(children.map { .string($0.name) })])
            for child in children {
                try await self.push(child, parameters: result)
            }
        }
        self.registerJob(job)
    }

    ///  Initialize JobDefinition
    /// - Parameters:
    ///   - name: Job name
    ///   - parameters: Job parameter type
    ///   - retryStrategy: Retry strategy for failed jobs
    ///   - timeout: Timeout for long running jobs
    ///   - execute: Closure that executes job
    public func registerParentJob<Parameters: JobParameters, Output: Sendable & Codable>(
        parameters: Parameters.Type = Parameters.self,
        children: [JobName<Output>],
        retryStrategy: (any JobRetryStrategy)? = nil,
        timeout: Duration? = nil,
        execute: @escaping @Sendable (Parameters, JobExecutionContext) async throws -> Output
    ) {
        self.logger.info(
            "Registered Parent Job",
            metadata: ["JobName": .string(Parameters.jobName), "Children": .array(children.map { .string($0.name) })]
        )
        let job = JobDefinition(
            parameters: parameters,
            retryStrategy: retryStrategy ?? self.options.retryStrategy,
            timeout: timeout
        ) { parameters, context in
            let result = try await execute(parameters, context)
            self.logger.debug(
                "Triggered jobs",
                metadata: ["JobName": .string(Parameters.jobName), "Children": .array(children.map { .string($0.name) })]
            )
            for child in children {
                try await self.push(child, parameters: result)
            }
        }
        self.registerJob(job)
    }
}

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

import Tracing

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Protocol for a Job
public protocol JobInstanceProtocol: Sendable {
    /// Parameters job requries
    associatedtype Parameters: Sendable & Codable
    /// Retry strategy
    var retryStrategy: any JobRetryStrategy { get }
    /// Time job was queued
    var queuedAt: Date { get }
    /// Current attempt
    var attempt: Int { get }
    /// Job parameters
    var parameters: Parameters { get }
    /// Job name
    var name: String { get }
    /// Trace context
    var traceContext: [String: String]? { get }
    /// Next time job is scheduled to run
    var nextScheduledAt: Date? { get }
    /// Timeout
    var timeout: Duration? { get }
    /// Function to execute the job
    func execute(context: JobExecutionContext) async throws
}

extension JobInstanceProtocol {
    /// Should we retry this job
    public func shouldRetry(error: Error) -> Bool {
        self.retryStrategy.shouldRetry(attempt: self.attempt, error: error)
    }

    /// Extract trace context from job instance data
    public func serviceContext() -> ServiceContext? {
        if let traceContext {
            var serviceContext = ServiceContext.topLevel
            InstrumentationSystem.tracer.extract(traceContext, into: &serviceContext, using: DictionaryExtractor())
            return serviceContext
        }
        return nil
    }
}

extension JobInstanceProtocol where Parameters: JobParameters {
    var name: String { Parameters.jobName }
}

/// Job decoded from Queue
///
/// Includes everything needed to run the job plus any other data that was encoded
/// with the job
struct JobInstance<Parameters: Sendable & Codable>: JobInstanceProtocol {
    /// job definition
    let job: JobDefinition<Parameters>
    /// job parameters
    let data: JobInstanceData<Parameters>
    /// Retry strategy
    var retryStrategy: any JobRetryStrategy { job.retryStrategy }
    /// Time job was queued
    var queuedAt: Date { self.data.queuedAt }
    /// Current attempt
    var attempt: Int { self.data.attempt }
    /// Trace context
    var traceContext: [String: String]? { self.data.traceContext }
    /// Job name
    var name: String { self.job.name }
    /// Job parameters
    var parameters: Parameters { self.data.parameters }
    /// Next time job is scheduled to run
    var nextScheduledAt: Date? { self.data.nextScheduledAt }
    /// Timeout for long running jobs
    var timeout: Duration? { self.job.timeout }

    func execute(context: JobExecutionContext) async throws {
        try await self.job.execute(self.data.parameters, context: context)
    }

    init(job: JobDefinition<Parameters>, data: JobInstanceData<Parameters>) throws {
        self.job = job
        self.data = data
    }
}

/// Data attach to a job
public struct JobInstanceData<Parameters: Sendable & Codable>: Codable, Sendable {
    /// Job parameters
    @usableFromInline
    let parameters: Parameters
    /// Time job was queued
    let queuedAt: Date
    /// Current attempt
    let attempt: Int
    /// trace context
    let traceContext: [String: String]?
    /// Next time job is scheduled to run
    let nextScheduledAt: Date?

    init(
        parameters: Parameters,
        queuedAt: Date,
        attempt: Int,
        nextScheduledAt: Date? = nil
    ) {
        self.parameters = parameters
        self.queuedAt = queuedAt
        self.attempt = attempt
        self.nextScheduledAt = nextScheduledAt

        var traceContext: [String: String]? = nil
        if let serviceContext = ServiceContext.current {
            var tempTraceContext = [String: String]()
            InstrumentationSystem.tracer.inject(serviceContext, into: &tempTraceContext, using: DictionaryInjector())
            if tempTraceContext.count > 0 {
                traceContext = tempTraceContext
            }
        }
        self.traceContext = traceContext
    }

    // keep JSON strings small to improve decode speed
    private enum CodingKeys: String, CodingKey {
        case parameters = "p"
        case queuedAt = "q"
        case attempt = "a"
        case traceContext = "t"
        case nextScheduledAt = "n"
    }
}

private struct DictionaryInjector: Injector {
    func inject(_ value: String, forKey key: String, into carrier: inout [String: String]) {
        carrier[key] = value
    }
}

private struct DictionaryExtractor: Extractor {
    func extract(key: String, from carrier: [String: String]) -> String? {
        carrier[key]
    }
}

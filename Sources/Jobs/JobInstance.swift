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

import Foundation
import Tracing

/// Protocol for a Job
public protocol JobInstanceProtocol: Sendable {
    /// Parameters job requries
    associatedtype Parameters: JobParameters
    /// Maximum number of times a job will be retried before being classed as failed
    var maxRetryCount: Int { get }
    /// Time job was queued
    var queuedAt: Date { get }
    /// Number of attempts so far
    var attempts: Int? { get }
    /// Job parameters
    var parameters: Parameters { get }
    /// Trace context
    var traceContext: [String: String]? { get }
    /// Next time job is scheduled to run
    var nextScheduledAt: Date? { get }
    /// Function to execute the job
    func execute(context: JobContext) async throws
}

extension JobInstanceProtocol {
    /// Name of job type
    public var name: String {
        Parameters.jobName
    }

    /// Number of remaining attempts
    public var remainingAttempts: Int {
        maxRetryCount - (attempts ?? 0)
    }

    /// If job failed after n number of attempts
    public var didFail: Bool {
        (attempts ?? 0) >= maxRetryCount
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

extension JobInstanceProtocol {
    /// Default conformance
    public var nextScheduledAt: Date? {
        get {
            nil
        }
    }
}

/// Job decoded from Queue
///
/// Includes everything needed to run the job plus any other data that was encoded
/// with the job
struct JobInstance<Parameters: JobParameters>: JobInstanceProtocol {
    /// job definition
    let job: JobDefinition<Parameters>
    /// job parameters
    let data: JobInstanceData<Parameters>
    /// max retry count for a job
    var maxRetryCount: Int { self.job.maxRetryCount }
    /// Time job was queued
    var queuedAt: Date { self.data.queuedAt }
    /// Number of attempts so far
    var attempts: Int? { self.data.attempts ?? 0 }
    /// Trace context
    var traceContext: [String: String]? { self.data.traceContext }
    /// Job parameters
    var parameters: Parameters { self.data.parameters }
    /// Next time job is scheduled to run
    var nextScheduledAt: Date? { self.data.nextScheduledAt }

    func execute(context: JobContext) async throws {
        try await self.job.execute(self.data.parameters, context: context)
    }

    init(job: JobDefinition<Parameters>, data: JobInstanceData<Parameters>) throws {
        self.job = job
        self.data = data
    }
}

/// Data attach to a job
public struct JobInstanceData<Parameters: JobParameters>: Codable, Sendable {
    /// Job parameters
    let parameters: Parameters
    /// Time job was queued
    let queuedAt: Date
    /// Number of attempts so far
    let attempts: Int?
    /// trace context
    let traceContext: [String: String]?
    /// Next time job is scheduled to run
    let nextScheduledAt: Date?

    init(
        parameters: Parameters,
        queuedAt: Date,
        attempts: Int?,
        nextScheduledAt: Date? = nil
    ) {
        self.parameters = parameters
        self.queuedAt = queuedAt
        self.attempts = attempts
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
        case attempts = "a"
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

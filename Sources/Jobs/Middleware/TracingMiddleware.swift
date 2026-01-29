//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

import Tracing

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Add distributed trace spans to each job instance a job queue runs
public struct TracingJobMiddleware: JobMiddleware {
    @usableFromInline
    let queueName: String

    public init() {
        self.queueName = JobMiddlewareBuilder.jobQueueName ?? "default"
    }

    public init(queueName: String) {
        self.queueName = queueName
    }

    /// Setup tracing span and pass it onto next handler, recording success or erro into the span
    ///
    /// - Parameters:
    ///   - job: Job instance
    ///   - context: Job execution context
    ///   - next: Next handler
    /// - Throws:
    @inlinable
    public func handleJob(
        job: any JobInstanceProtocol,
        context: JobExecutionContext,
        next: (any JobInstanceProtocol, JobExecutionContext) async throws -> Void
    ) async throws {
        let linkContext = job.serviceContext()
        return try await withSpan(job.name, ofKind: .server) { span in
            if let linkContext {
                span.addLink(.init(context: linkContext, attributes: .init()))
            }
            span.updateAttributes { attributes in
                attributes["job.id"] = context.jobID
                attributes["job.attempt"] = job.attempt
                attributes["job.queue"] = self.queueName
            }
            do {
                try await next(job, context)
            } catch {
                span.recordError(error)
                if !job.shouldRetry(error: error) || error is CancellationError {
                    span.attributes["job.failed"] = true
                }
                throw error
            }
        }
    }
}

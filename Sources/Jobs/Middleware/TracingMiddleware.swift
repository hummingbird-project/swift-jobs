//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2025 the Hummingbird authors
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

/// Add distributed trace spans to each job instance a job queue runs
public struct TracingJobMiddleware: JobMiddleware {
    @usableFromInline
    let queueName: String

    public init(queueName: String = "default") {
        self.queueName = queueName
    }

    @inlinable
    public func onPushJob<Parameters: JobParameters>(parameters: Parameters, context: JobQueueContext) async {}
    @inlinable
    public func onPopJob(result: Result<any JobInstanceProtocol, JobQueueError>, context: JobQueueContext) async {}

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
                attributes["job.attempt"] = job.attempts
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

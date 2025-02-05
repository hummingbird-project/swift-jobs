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

import Foundation
import Tracing

public struct TracingJobMiddleware: JobMiddleware {
    @inlinable
    public func pushJob<Parameters: Codable & Sendable>(jobID: JobIdentifier<Parameters>, parameters: Parameters, jobInstanceID: String) async {}
    @inlinable
    public func popJob(result: Result<any JobInstanceProtocol, Error>, jobInstanceID: String) async {}

    @inlinable
    public func handleJob(
        job: any JobInstanceProtocol,
        context: JobContext,
        next: (any JobInstanceProtocol, JobContext) async throws -> Void
    ) async throws {
        let linkContext = job.serviceContext()
        return try await withSpan(job.name, ofKind: .server) { span in
            if let linkContext {
                span.addLink(.init(context: linkContext, attributes: .init()))
            }
            span.updateAttributes { attributes in
                attributes["job.id"] = context.jobInstanceID
                attributes["job.attempt"] = (job.attempts ?? 0) + 1
            }
            do {
                try await next(job, context)
            } catch {
                span.recordError(error)
                if job.didFail || error is CancellationError {
                    span.attributes["job.failed"] = true
                }
                throw error
            }
        }
    }
}

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

public protocol JobMiddleware: Sendable {
    /// Job has been pushed onto the queue
    ///
    /// - Parameters:
    ///   - jobID: Job type identifier
    ///   - parameters: Job parameters
    ///   - jobInstanceID: Job instance identifier
    func onPushJob<Parameters: Codable & Sendable>(jobID: JobIdentifier<Parameters>, parameters: Parameters, jobInstanceID: String) async
    /// Job has been popped off the queue and decoded (with decode errors reported)
    ///
    /// - Parameters:
    ///   - result: Result of popping the job from the queue (Either job instance or error)
    ///   - jobInstanceID: Job instance identifer
    func onPopJob(result: Result<any JobInstanceProtocol, JobQueueError>, jobInstanceID: String) async
    /// Handle job and pass it onto next handler
    ///
    /// - Parameters:
    ///   - job: Job instance
    ///   - context: Job context
    ///   - next: Next handler
    /// - Throws:
    func handleJob(job: any JobInstanceProtocol, context: JobContext, next: (any JobInstanceProtocol, JobContext) async throws -> Void) async throws
}

public struct NullJobMiddleware: JobMiddleware {
    public init() {}

    /// Job has been pushed onto the queue
    @inlinable
    public func onPushJob<Parameters: Codable & Sendable>(jobID: JobIdentifier<Parameters>, parameters: Parameters, jobInstanceID: String) async {}
    /// Job has been popped off the queue and decoded (with decode errors reported)
    @inlinable
    public func onPopJob(result: Result<any JobInstanceProtocol, JobQueueError>, jobInstanceID: String) async {}
    /// Handle job and pass it onto next handler (works like middleware in Hummingbird)
    @inlinable
    public func handleJob(
        job: any JobInstanceProtocol,
        context: JobContext,
        next: (any JobInstanceProtocol, JobContext) async throws -> Void
    ) async throws {
        try await next(job, context)
    }
}

struct OptionalJobMiddleware<Middleware: JobMiddleware>: JobMiddleware {
    @usableFromInline
    let middleware: Middleware?

    @inlinable
    func onPushJob<Parameters: Codable & Sendable>(jobID: JobIdentifier<Parameters>, parameters: Parameters, jobInstanceID: String) async {
        if let middleware {
            await middleware.onPushJob(jobID: jobID, parameters: parameters, jobInstanceID: jobInstanceID)
        }
    }
    /// Job has been popped off the queue and decoded (with decode errors reported)
    @inlinable
    func onPopJob(result: Result<any JobInstanceProtocol, JobQueueError>, jobInstanceID: String) async {
        if let middleware {
            await middleware.onPopJob(result: result, jobInstanceID: jobInstanceID)
        }
    }
    /// Process job and pass it onto next handler (works like middleware in Hummingbird)
    @inlinable
    func handleJob(
        job: any JobInstanceProtocol,
        context: JobContext,
        next: (any JobInstanceProtocol, JobContext) async throws -> Void
    ) async throws {
        guard let middleware else {
            return try await next(job, context)
        }
        return try await middleware.handleJob(job: job, context: context, next: next)
    }
}

struct TwoJobMiddlewares<Middleware1: JobMiddleware, Middleware2: JobMiddleware>: JobMiddleware {
    @usableFromInline
    let middleware1: Middleware1
    @usableFromInline
    let middleware2: Middleware2

    @inlinable
    func onPushJob<Parameters: Codable & Sendable>(jobID: JobIdentifier<Parameters>, parameters: Parameters, jobInstanceID: String) async {
        await self.middleware1.onPushJob(jobID: jobID, parameters: parameters, jobInstanceID: jobInstanceID)
        await self.middleware2.onPushJob(jobID: jobID, parameters: parameters, jobInstanceID: jobInstanceID)
    }
    /// Job has been popped off the queue and decoded (with decode errors reported)
    @inlinable
    func onPopJob(result: Result<any JobInstanceProtocol, JobQueueError>, jobInstanceID: String) async {
        await self.middleware1.onPopJob(result: result, jobInstanceID: jobInstanceID)
        await self.middleware2.onPopJob(result: result, jobInstanceID: jobInstanceID)
    }
    /// Process job and pass it onto next handler (works like middleware in Hummingbird)
    @inlinable
    func handleJob(
        job: any JobInstanceProtocol,
        context: JobContext,
        next: (any JobInstanceProtocol, JobContext) async throws -> Void
    ) async throws {
        try await self.middleware1.handleJob(job: job, context: context) { job, context in
            try await self.middleware2.handleJob(job: job, context: context, next: next)
        }
    }
}

@resultBuilder
public enum JobMiddlewareBuilder {
    public static func buildBlock<Middleware: JobMiddleware>(_ middleware: Middleware) -> Middleware {
        middleware
    }

    public static func buildPartialBlock<Middleware: JobMiddleware>(first: Middleware) -> Middleware {
        first
    }

    public static func buildPartialBlock<Middleware1: JobMiddleware, Middleware2: JobMiddleware>(
        accumulated middleware1: Middleware1,
        next middleware2: Middleware2
    ) -> some JobMiddleware {
        TwoJobMiddlewares(middleware1: middleware1, middleware2: middleware2)
    }

    public static func buildOptional<Middleware: JobMiddleware>(_ middleware: Middleware?) -> some JobMiddleware {
        OptionalJobMiddleware(middleware: middleware)
    }

    public static func buildEither<Middleware: JobMiddleware>(
        first content: Middleware
    ) -> Middleware {
        content
    }

    public static func buildEither<Middleware: JobMiddleware>(
        second content: Middleware
    ) -> Middleware {
        content
    }
}

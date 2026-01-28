//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

import Foundation
import Logging
import NIOCore
import Testing

@testable import Jobs

struct JobMiddlewareTests {
    func buildJobMiddleware(@JobMiddlewareBuilder build: () -> some JobMiddleware) -> some JobMiddleware {
        build()
    }

    final class TestJobMiddleware: JobMiddleware, @unchecked Sendable {
        var pushed: Bool
        var popped: Bool
        var handled: Bool
        var completed: Bool
        init() {
            self.pushed = false
            self.popped = false
            self.handled = false
            self.completed = false
        }

        func onPushJob<Parameters>(name: String, parameters: Parameters, context: JobPushQueueContext) async {
            self.pushed = true
        }

        func onPopJob(result: Result<any JobInstanceProtocol, JobQueueError>, context: JobPopQueueContext) async {
            self.popped = true
        }

        func handleJob(
            job: any JobInstanceProtocol,
            context: JobExecutionContext,
            next: (any JobInstanceProtocol, JobExecutionContext) async throws -> Void
        ) async throws {
            self.handled = true
            try await next(job, context)
        }

        func onCompletedJob(job: any Jobs.JobInstanceProtocol, result: Result<Void, any Error>, context: JobCompletedQueueContext) async {
            self.completed = true
        }
    }

    @Test func testResultBuilderWithTwoMiddleware() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testResultBuilderWithTwoMiddleware"
            let value: String
        }
        struct FakeJobInstance: JobInstanceProtocol {
            let parameters = TestParameters(value: "test")
            let retryStrategy: JobRetryStrategy = .exponentialJitter(maxAttempts: 1)
            let queuedAt = Date.now
            let attempt: Int = 0
            let nextScheduledAt: Date? = nil
            let traceContext: [String: String]? = nil
            let timeout: Duration? = nil

            func execute(context: JobExecutionContext) async throws {}
        }
        let observer1 = TestJobMiddleware()
        let observer2 = TestJobMiddleware()
        let observers = buildJobMiddleware {
            observer1
            TestJobMiddleware()
            observer2
        }
        await observers.onPushJob(name: "testResultBuilderWithTwoMiddleware", parameters: "test", context: .init(jobID: "0", attempt: 1))
        #expect(observer1.pushed == true)
        #expect(observer2.pushed == true)
        await observers.onPopJob(result: .success(FakeJobInstance()), context: .init(jobID: "0"))
        #expect(observer1.popped == true)
        #expect(observer2.popped == true)
        let job = FakeJobInstance()
        try await observers.handleJob(
            job: job,
            context: .init(
                jobID: "0",
                logger: .init(label: "Test"),
                queuedAt: job.queuedAt,
                nextScheduledAt: job.nextScheduledAt,
                attempt: job.attempt
            )
        ) { _, _ in }
        #expect(observer1.handled == true)
        #expect(observer2.handled == true)
        await observers.onCompletedJob(job: FakeJobInstance(), result: .success(()), context: .init(jobID: "0"))
        #expect(observer1.completed == true)
        #expect(observer2.completed == true)
    }

    @Test func testResultBuildeOptionalMiddleware() async throws {
        func testIf(
            _ first: Bool
        ) async throws {
            struct TestParameters: JobParameters {
                static let jobName = "testResultBuildeOptionalMiddleware"
                let value: String
            }
            struct FakeJobInstance: JobInstanceProtocol {
                let parameters = TestParameters(value: "test")
                let retryStrategy: JobRetryStrategy = .exponentialJitter(maxAttempts: 1)
                let queuedAt = Date.now
                let attempt: Int = 0
                let traceContext: [String: String]? = nil
                var nextScheduledAt: Date? = nil
                let timeout: Duration? = nil

                func execute(context: JobExecutionContext) async throws {}
            }
            let middleware1 = TestJobMiddleware()
            let middlewareChain = buildJobMiddleware {
                if first {
                    middleware1
                }
            }
            await middlewareChain.onPushJob(
                name: "testResultBuildeOptionalMiddleware",
                parameters: TestParameters(value: "test"),
                context: .init(jobID: "0", attempt: 1)
            )
            #expect(middleware1.pushed == first)
            await middlewareChain.onPopJob(result: .success(FakeJobInstance()), context: .init(jobID: "0"))
            #expect(middleware1.popped == first)
            let job = FakeJobInstance()
            try await middlewareChain.handleJob(
                job: job,
                context: .init(
                    jobID: "0",
                    logger: .init(label: "Test"),
                    queuedAt: job.queuedAt,
                    nextScheduledAt: job.nextScheduledAt,
                    attempt: job.attempt
                )
            ) { _, _ in }
            #expect(middleware1.handled == first)
            await middlewareChain.onCompletedJob(job: FakeJobInstance(), result: .success(()), context: .init(jobID: "0"))
            #expect(middleware1.completed == first)
        }
        try await testIf(true)
        try await testIf(false)
    }

    @Test func testResultBuilderIfElseMiddleware() async throws {
        func testEitherOr(
            first: Bool
        ) async throws {
            struct TestParameters: JobParameters {
                static let jobName = "testResultBuilderIfElseMiddleware"
                let value: String
            }
            struct FakeJobInstance: JobInstanceProtocol {
                let parameters = TestParameters(value: "test")
                let retryStrategy: JobRetryStrategy = .exponentialJitter(maxAttempts: 1)
                let queuedAt = Date.now
                let attempt: Int = 0
                let traceContext: [String: String]? = nil
                let nextScheduledAt: Date? = Date.now
                let timeout: Duration? = nil

                func execute(context: JobExecutionContext) async throws {}
            }
            let middleware1 = TestJobMiddleware()
            let middleware2 = TestJobMiddleware()
            let middlewareChain = buildJobMiddleware {
                if first {
                    middleware1
                } else {
                    middleware2
                }
            }
            await middlewareChain.onPushJob(
                name: "testResultBuilderIfElseMiddleware",
                parameters: TestParameters(value: "test"),
                context: .init(jobID: "0", attempt: 1)
            )
            #expect(middleware1.pushed == first)
            #expect(middleware2.pushed != first)
            await middlewareChain.onPopJob(result: .success(FakeJobInstance()), context: .init(jobID: "0"))
            #expect(middleware1.popped == first)
            #expect(middleware2.popped != first)
            let job = FakeJobInstance()
            try await middlewareChain.handleJob(
                job: job,
                context: .init(
                    jobID: "0",
                    logger: .init(label: "Test"),
                    queuedAt: job.queuedAt,
                    nextScheduledAt: job.nextScheduledAt,
                    attempt: job.attempt
                )
            ) { _, _ in }
            #expect(middleware1.handled == first)
            #expect(middleware2.handled != first)
            await middlewareChain.onCompletedJob(job: FakeJobInstance(), result: .success(()), context: .init(jobID: "0"))
            #expect(middleware1.completed == first)
            #expect(middleware2.completed != first)
        }
        try await testEitherOr(first: true)
        try await testEitherOr(first: false)
    }

    @Test func testMiddlewareBeingCalled() async throws {
        struct TestMiddleware<Queue: JobMetadataDriver>: JobMiddleware {
            let continuation: AsyncStream<String>.Continuation
            let queue: Queue

            init(continuation: AsyncStream<String>.Continuation, queue: Queue) {
                self.continuation = continuation
                self.queue = queue
            }
            func onPushJob<Parameters>(name: String, parameters: Parameters, context: JobPushQueueContext) async {
                continuation.yield("push")
            }
            func onPopJob(result: Result<any JobInstanceProtocol, JobQueueError>, context: JobPopQueueContext) async {
                continuation.yield("pop")
            }
            func handleJob(
                job: any JobInstanceProtocol,
                context: JobExecutionContext,
                next: (any JobInstanceProtocol, JobExecutionContext) async throws -> Void
            ) async throws {
                continuation.yield("handle")
                return try await next(job, context)
            }
            func onCompletedJob(job: any JobInstanceProtocol, result: Result<Void, any Error>, context: JobCompletedQueueContext) async {
                continuation.yield("complete")
                try? await queue.setMetadata(key: .init("\(context.jobID)"), value: ByteBuffer(string: "Done"))
            }

        }
        struct TestParameters: JobParameters {
            static let jobName = "testBasic"
            let value: Int
        }
        let (stream, cont) = AsyncStream.makeStream(of: String.self)
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let jobQueue = JobQueue(
            .memory,
            logger: logger
        ) { queue in
            TestMiddleware(continuation: cont, queue: queue)
        }

        jobQueue.registerJob(name: .init("MiddlewareTest")) { (parameters: Bool, context) in
        }
        try await testJobQueue(jobQueue.processor()) {
            let jobID = try await jobQueue.push("MiddlewareTest", parameters: true)
            var iterator = stream.makeAsyncIterator()
            #expect(await iterator.next() == "push")
            #expect(await iterator.next() == "pop")
            #expect(await iterator.next() == "handle")
            #expect(await iterator.next() == "complete")
            let response = try await jobQueue.queue.getMetadata(.init("\(jobID)"))
            #expect(response == ByteBuffer(string: "Done"))
        }
    }

}

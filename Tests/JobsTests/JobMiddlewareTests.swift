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
import Logging
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
            let leaseDuration: Duration? = nil
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
                attempt: job.attempt,
                workerID: .init()
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
                let leaseDuration: Duration? = nil

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
                    attempt: job.attempt,
                    workerID: .init()
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
                let leaseDuration: Duration? = nil

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
                    attempt: job.attempt,
                    workerID: .init()
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
}

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

import Logging
import XCTest

@testable import Jobs

final class JobMiddlewareTests: XCTestCase {
    func buildJobMiddleware(@JobMiddlewareBuilder build: () -> some JobMiddleware) -> some JobMiddleware {
        build()
    }

    final class TestJobMiddleware: JobMiddleware, @unchecked Sendable {
        var pushed: Bool
        var popped: Bool
        var handled: Bool
        init() {
            self.pushed = false
            self.popped = false
            self.handled = false
        }

        func onPushJob<Parameters: JobParameters>(parameters: Parameters, jobInstanceID: String) async {
            self.pushed = true
        }

        func onPopJob(result: Result<any JobInstanceProtocol, JobQueueError>, jobInstanceID: String) async {
            self.popped = true
        }

        func handleJob(
            job: any JobInstanceProtocol,
            context: JobContext,
            next: (any JobInstanceProtocol, JobContext) async throws -> Void
        ) async throws {
            self.handled = true
            try await next(job, context)
        }

    }

    func testResultBuilderWithTwoMiddleware() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testResultBuilderWithTwoMiddleware"
            let value: String
        }
        struct FakeJobInstance: JobInstanceProtocol {
            let parameters = TestParameters(value: "test")
            let maxRetryCount = 1
            let queuedAt = Date.now
            let attempts: Int? = 0
            let traceContext: [String: String]? = nil

            func execute(context: JobContext) async throws {}
        }
        let observer1 = TestJobMiddleware()
        let observer2 = TestJobMiddleware()
        let observers = buildJobMiddleware {
            observer1
            TestJobMiddleware()
            observer2
        }
        await observers.onPushJob(parameters: TestParameters(value: "test"), jobInstanceID: "0")
        XCTAssertEqual(observer1.pushed, true)
        XCTAssertEqual(observer2.pushed, true)
        await observers.onPopJob(result: .success(FakeJobInstance()), jobInstanceID: "0")
        XCTAssertEqual(observer1.popped, true)
        XCTAssertEqual(observer2.popped, true)
        try await observers.handleJob(job: FakeJobInstance(), context: .init(jobInstanceID: "0", logger: .init(label: "Test"))) { _, _ in }
        XCTAssertEqual(observer1.handled, true)
        XCTAssertEqual(observer2.handled, true)
    }

    func testResultBuildeOptionalMiddleware() async throws {
        func testIf(
            _ first: Bool
        ) async throws {
            struct TestParameters: JobParameters {
                static let jobName = "testResultBuildeOptionalMiddleware"
                let value: String
            }
            struct FakeJobInstance: JobInstanceProtocol {
                let parameters = TestParameters(value: "test")
                let maxRetryCount = 1
                let queuedAt = Date.now
                let attempts: Int? = 0
                let traceContext: [String: String]? = nil

                func execute(context: JobContext) async throws {}
            }
            let middleware1 = TestJobMiddleware()
            let middlewareChain = buildJobMiddleware {
                if first {
                    middleware1
                }
            }
            await middlewareChain.onPushJob(parameters: TestParameters(value: "test"), jobInstanceID: "0")
            XCTAssertEqual(middleware1.pushed, first == true)
            await middlewareChain.onPopJob(result: .success(FakeJobInstance()), jobInstanceID: "0")
            XCTAssertEqual(middleware1.popped, first == true)
            try await middlewareChain.handleJob(job: FakeJobInstance(), context: .init(jobInstanceID: "0", logger: .init(label: "Test"))) { _, _ in }
            XCTAssertEqual(middleware1.handled, first == true)
        }
        try await testIf(true)
        try await testIf(false)
    }

    func testResultBuilderIfElseMiddleware() async throws {
        func testEitherOr(
            first: Bool
        ) async throws {
            struct TestParameters: JobParameters {
                static let jobName = "testResultBuilderIfElseMiddleware"
                let value: String
            }
            struct FakeJobInstance: JobInstanceProtocol {
                let parameters = TestParameters(value: "test")
                let maxRetryCount = 1
                let queuedAt = Date.now
                let attempts: Int? = 0
                let traceContext: [String: String]? = nil

                func execute(context: JobContext) async throws {}
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
            await middlewareChain.onPushJob(parameters: TestParameters(value: "test"), jobInstanceID: "0")
            XCTAssertEqual(middleware1.pushed, first == true)
            XCTAssertEqual(middleware2.pushed, first != true)
            await middlewareChain.onPopJob(result: .success(FakeJobInstance()), jobInstanceID: "0")
            XCTAssertEqual(middleware1.popped, first == true)
            XCTAssertEqual(middleware2.popped, first != true)
            try await middlewareChain.handleJob(job: FakeJobInstance(), context: .init(jobInstanceID: "0", logger: .init(label: "Test"))) { _, _ in }
            XCTAssertEqual(middleware1.handled, first == true)
            XCTAssertEqual(middleware2.handled, first != true)
        }
        try await testEitherOr(first: true)
        try await testEitherOr(first: false)
    }
}

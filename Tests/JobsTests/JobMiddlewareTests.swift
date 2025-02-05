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

    func testResultBuilderWithTwoObservers() async throws {
        final class TestJobMiddleware: JobMiddleware, @unchecked Sendable {
            var pushed: Bool
            var popped: Bool
            var handled: Bool
            init() {
                self.pushed = false
                self.popped = false
                self.handled = false
            }

            func pushJob<Parameters: Codable & Sendable>(jobID: JobIdentifier<Parameters>, parameters: Parameters, jobInstanceID: String) async {
                self.pushed = true
            }

            func popJob(result: Result<any JobInstanceProtocol, any Error>, jobInstanceID: String) async {
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
        struct FakeJobInstance: JobInstanceProtocol {
            let id = JobIdentifier<String>("fake")
            let parameters = "Fake parameters"
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
        await observers.pushJob(jobID: JobIdentifier<String>("test"), parameters: "Test", jobInstanceID: "0")
        XCTAssertEqual(observer1.pushed, true)
        XCTAssertEqual(observer2.pushed, true)
        await observers.popJob(result: .success(FakeJobInstance()), jobInstanceID: "0")
        XCTAssertEqual(observer1.popped, true)
        XCTAssertEqual(observer2.popped, true)
        try await observers.handleJob(job: FakeJobInstance(), context: .init(jobInstanceID: "0", logger: .init(label: "Test"))) { _, _ in }
        XCTAssertEqual(observer1.handled, true)
        XCTAssertEqual(observer2.handled, true)
    }
}

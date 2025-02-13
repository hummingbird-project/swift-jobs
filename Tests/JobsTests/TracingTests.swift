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
import Jobs
import Logging
import NIOConcurrencyHelpers
import Tracing
import XCTest

@testable import Instrumentation

final class TracingTests: XCTestCase {
    func testTraceOutput() async throws {
        struct TestParameters: JobParameters {
            static var jobName: String = "TestTracing"
            let sleep: Double
        }
        let expectation = expectation(description: "Expected span to be ended.")
        let tracer = TestTracer()
        tracer.onEndSpan = { _ in expectation.fulfill() }
        InstrumentationSystem.bootstrapInternal(tracer)

        let jobQueue = JobQueue(.memory, numWorkers: 1, logger: Logger(label: "JobsTests"))
        jobQueue.registerJob(parameters: TestParameters.self) { parameters, context in
            context.logger.info("Parameters=\(parameters)")
            try await Task.sleep(for: .milliseconds(parameters.sleep))
        }
        let jobID = try await testJobQueue(jobQueue) {
            let jobID = try await jobQueue.push(TestParameters(sleep: 10))
            await fulfillment(of: [expectation], timeout: 1)
            return jobID
        }

        let span = try XCTUnwrap(tracer.spans.first)

        XCTAssertEqual(span.operationName, "TestTracing")
        XCTAssertEqual(span.kind, .server)
        XCTAssertNil(span.status)
        XCTAssertTrue(span.recordedErrors.isEmpty)

        XCTAssertSpanAttributesEqual(
            span.attributes,
            [
                "job.id": "\(jobID.uuidString)",
                "job.attempt": 1,
            ]
        )
    }

    func testTracingErrorAndRetry() async throws {
        struct TestParameters: JobParameters {
            static var jobName: String = "TestTracingErrors"
        }
        struct FailedError: Error {}
        let expectation = expectation(description: "Expected span to be ended.")
        expectation.expectedFulfillmentCount = 2
        let tracer = TestTracer()
        tracer.onEndSpan = { _ in expectation.fulfill() }
        InstrumentationSystem.bootstrapInternal(tracer)

        let failJob = NIOLockedValueBox(true)
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .debug
        let jobQueue = JobQueue(.memory, numWorkers: 1, logger: logger, options: .init(maxJitter: 0.25, minJitter: 0.01))
        jobQueue.registerJob(parameters: TestParameters.self, maxRetryCount: 4) { parameters, context in
            if failJob.withLockedValue({
                let value = $0
                $0 = false
                return value
            }) {
                throw FailedError()
            }
        }
        let jobID = try await testJobQueue(jobQueue) {
            let jobID = try await jobQueue.push(TestParameters())
            await fulfillment(of: [expectation], timeout: 5)
            return jobID
        }

        let span = try XCTUnwrap(tracer.spans.first)

        XCTAssertEqual(span.operationName, "TestTracingErrors")
        XCTAssertEqual(span.kind, .server)
        XCTAssertNil(span.status)
        let error = try XCTUnwrap(span.recordedErrors.first)

        XCTAssert(error.0 is FailedError)

        XCTAssertSpanAttributesEqual(
            span.attributes,
            [
                "job.id": "\(jobID.uuidString)",
                "job.attempt": 1,
            ]
        )
        let span2 = try XCTUnwrap(tracer.spans.last)

        XCTAssertEqual(span2.operationName, "TestTracingErrors")
        XCTAssertEqual(span2.kind, .server)
        XCTAssertNil(span2.status)
        XCTAssert(span2.recordedErrors.isEmpty)
        XCTAssertEqual(span2.attributes.get("job.attempt"), SpanAttribute.int64(2))
    }

    func testParentSpan() async throws {
        struct TestParameters: JobParameters {
            static var jobName: String = "TestParentSpan"
            let sleep: Double
        }
        struct JobTestKey: ServiceContextKey {
            typealias Value = String
            static var nameOverride: String? { "job-test-key" }
        }
        let jobExpectation = expectation(description: "Expecyt job to finish")
        let tracerExpectation = expectation(description: "Expected span to be ended.")
        tracerExpectation.expectedFulfillmentCount = 2
        let tracer = TestTracer()
        tracer.onEndSpan = { _ in tracerExpectation.fulfill() }
        InstrumentationSystem.bootstrapInternal(tracer)

        let jobQueue = JobQueue(.memory, numWorkers: 1, logger: Logger(label: "JobsTests"))
        jobQueue.registerJob(parameters: TestParameters.self) { parameters, context in
            context.logger.info("Parameters=\(parameters)")
            try await Task.sleep(for: .milliseconds(parameters.sleep))
            jobExpectation.fulfill()
        }

        var serviceContext = ServiceContext.current ?? ServiceContext.topLevel
        serviceContext.traceID = UUID().uuidString
        let jobID = try await withSpan("ParentSpan", context: serviceContext, ofKind: .server) { _ in
            let jobID = try await testJobQueue(jobQueue) {
                let jobID = try await jobQueue.push(TestParameters(sleep: 1))
                await fulfillment(of: [jobExpectation], timeout: 5)
                return jobID
            }
            return jobID
        }
        await fulfillment(of: [tracerExpectation], timeout: 5)

        let span = try XCTUnwrap(tracer.spans.first)

        XCTAssertEqual(span.operationName, "ParentSpan")
        XCTAssertEqual(span.kind, .server)
        XCTAssertNil(span.status)
        XCTAssertTrue(span.recordedErrors.isEmpty)

        let span2 = try XCTUnwrap(tracer.spans.last)

        XCTAssertEqual(span2.operationName, "TestParentSpan")
        XCTAssertEqual(span2.kind, .server)
        XCTAssertNil(span2.status)
        XCTAssertTrue(span2.recordedErrors.isEmpty)
        let link = try XCTUnwrap(span2.links.first)
        XCTAssertEqual(link.context.traceID, serviceContext.traceID)

        XCTAssertSpanAttributesEqual(
            span2.attributes,
            [
                "job.id": "\(jobID.uuidString)",
                "job.attempt": 1,
            ]
        )
    }
}

private func XCTAssertSpanAttributesEqual(
    _ lhs: @autoclosure () -> SpanAttributes,
    _ rhs: @autoclosure () -> [String: SpanAttribute],
    file: StaticString = #filePath,
    line: UInt = #line
) {
    var rhs = rhs()

    // swift-format-ignore: ReplaceForEachWithForLoop
    lhs().forEach { key, attribute in
        if let rhsValue = rhs.removeValue(forKey: key) {
            if rhsValue != attribute {
                XCTFail(
                    #""\#(key)" was expected to be "\#(rhsValue)" but is actually "\#(attribute)"."#,
                    file: file,
                    line: line
                )
            }
        } else {
            XCTFail(
                #"Did not specify expected value for "\#(key)", actual value is "\#(attribute)"."#,
                file: file,
                line: line
            )
        }
    }

    if !rhs.isEmpty {
        XCTFail(#"Expected attributes "\#(rhs.keys)" are not present in actual attributes."#, file: file, line: line)
    }
}

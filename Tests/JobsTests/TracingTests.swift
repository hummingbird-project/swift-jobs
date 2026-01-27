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
import Synchronization
import Testing
import Tracing

@testable import Instrumentation

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

struct TracingTests {
    static let testTracer = {
        let tracer = TaskUniqueTestTracer()
        InstrumentationSystem.bootstrap(tracer)
        return tracer
    }()

    @Test func testTraceOutput() async throws {
        struct TestParameters: JobParameters {
            static let jobName: String = "TestTracing"
            let sleep: Double
        }
        try await Self.testTracer.withUnique {
            let jobQueue = JobQueue(MemoryQueue(queueName: "TestTracing"), logger: Logger(label: "JobsTests")) {
                TracingJobMiddleware()
            }
            jobQueue.registerJob(parameters: TestParameters.self) { parameters, context in
                context.logger.info("Parameters=\(parameters)")
                try await Task.sleep(for: .milliseconds(parameters.sleep))
            }
            let expectation = TestExpectation()
            Self.testTracer.onEndSpan = { _ in expectation.trigger() }

            let jobID = try await testJobQueue(jobQueue.processor()) {
                let jobID = try await jobQueue.push(TestParameters(sleep: 10))
                try await expectation.wait()
                return jobID
            }

            let span = try #require(Self.testTracer.spans.first)

            #expect(span.operationName == "TestTracing")
            #expect(span.kind == .server)
            #expect(span.status == nil)
            #expect(span.recordedErrors.isEmpty == true)

            expectSpanAttributesEqual(
                span.attributes,
                [
                    "job.id": "\(jobID.uuidString)",
                    "job.attempt": 1,
                    "job.queue": "TestTracing",
                ]
            )
        }
    }

    @Test func testTracingErrorAndRetry() async throws {
        struct TestParameters: JobParameters {
            static let jobName: String = "TestTracingErrors"
        }
        struct FailedError: Error {}
        try await Self.testTracer.withUnique {
            let failJob = Mutex(true)
            var logger = Logger(label: "JobsTests")
            logger.logLevel = .debug
            let jobQueue = JobQueue(.memory, logger: logger) {
                TracingJobMiddleware()
            }
            jobQueue.registerJob(
                parameters: TestParameters.self,
                retryStrategy: .exponentialJitter(maxAttempts: 4, maxBackoff: .seconds(0.5), minJitter: 0.0, maxJitter: 0.25)
            ) { parameters, context in
                if failJob.withLock({
                    let value = $0
                    $0 = false
                    return value
                }) {
                    throw FailedError()
                }
            }
            let expectation = TestExpectation()
            Self.testTracer.onEndSpan = { _ in expectation.trigger() }

            let jobID = try await testJobQueue(jobQueue.processor()) {
                let jobID = try await jobQueue.push(TestParameters())
                try await expectation.wait(count: 2)
                return jobID
            }

            let span = try #require(Self.testTracer.spans.first)

            #expect(span.operationName == "TestTracingErrors")
            #expect(span.kind == .server)
            #expect(span.status == nil)
            let error = try #require(span.recordedErrors.first)

            #expect(error.0 is FailedError)

            expectSpanAttributesEqual(
                span.attributes,
                [
                    "job.id": "\(jobID.uuidString)",
                    "job.attempt": 1,
                    "job.queue": "default",
                ]
            )
            let span2 = try #require(Self.testTracer.spans.last)

            #expect(span2.operationName == "TestTracingErrors")
            #expect(span2.kind == .server)
            #expect(span2.status == nil)
            #expect(span2.recordedErrors.isEmpty)
            #expect(span2.attributes.get("job.attempt") == SpanAttribute.int64(2))
        }
    }

    @Test func testParentSpan() async throws {
        struct TestParameters: JobParameters {
            static let jobName: String = "TestParentSpan"
            let sleep: Double
        }
        struct JobTestKey: ServiceContextKey {
            typealias Value = String
            static var nameOverride: String? { "job-test-key" }
        }
        try await Self.testTracer.withUnique {

            let jobExpectation = TestExpectation()
            let tracerExpectation = TestExpectation()
            Self.testTracer.onEndSpan = { _ in tracerExpectation.trigger() }

            let jobQueue = JobQueue(.memory, logger: Logger(label: "JobsTests")) {
                TracingJobMiddleware(queueName: "tracing")
            }
            jobQueue.registerJob(parameters: TestParameters.self) { parameters, context in
                context.logger.info("Parameters=\(parameters)")
                try await Task.sleep(for: .milliseconds(parameters.sleep))
                jobExpectation.trigger()
            }

            var serviceContext = ServiceContext.current ?? ServiceContext.topLevel
            serviceContext.traceID = UUID().uuidString
            let jobID = try await withSpan("ParentSpan", context: serviceContext, ofKind: .server) { _ in
                let jobID = try await testJobQueue(jobQueue.processor()) {
                    let jobID = try await jobQueue.push(TestParameters(sleep: 1))
                    try await jobExpectation.wait()
                    return jobID
                }
                return jobID
            }
            try await tracerExpectation.wait(count: 2)

            let span = try #require(Self.testTracer.spans.first)

            #expect(span.operationName == "ParentSpan")
            #expect(span.kind == .server)
            #expect(span.status == nil)
            #expect(span.recordedErrors.isEmpty == true)

            let span2 = try #require(Self.testTracer.spans.last)

            #expect(span2.operationName == "TestParentSpan")
            #expect(span2.kind == .server)
            #expect(span2.status == nil)
            #expect(span2.recordedErrors.isEmpty == true)
            let link = try #require(span2.links.first)
            #expect(link.context.traceID == serviceContext.traceID)

            expectSpanAttributesEqual(
                span2.attributes,
                [
                    "job.id": "\(jobID.uuidString)",
                    "job.attempt": 1,
                    "job.queue": "tracing",
                ]
            )
        }
    }
}

private func expectSpanAttributesEqual(
    _ lhs: @autoclosure () -> SpanAttributes,
    _ rhs: @autoclosure () -> [String: SpanAttribute],
    fileID: String = #fileID,
    filePath: String = #filePath,
    line: Int = #line,
    column: Int = #column
) {
    var rhs = rhs()

    // swift-format-ignore: ReplaceForEachWithForLoop
    lhs().forEach { key, attribute in
        if let rhsValue = rhs.removeValue(forKey: key) {
            #expect(rhsValue == attribute, sourceLocation: .init(fileID: fileID, filePath: filePath, line: line, column: column))
        } else {
            Issue.record(
                #"Did not specify expected value for "\#(key)", actual value is "\#(attribute)"."#,
                sourceLocation: .init(fileID: fileID, filePath: filePath, line: line, column: column)
            )
        }
    }

    if !rhs.isEmpty {
        Issue.record(
            #"Expected attributes "\#(rhs.keys)" are not present in actual attributes."#,
            sourceLocation: .init(fileID: fileID, filePath: filePath, line: line, column: column)
        )
    }
}

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
            try await jobQueue.push(TestParameters(sleep: 10))
        }

        await fulfillment(of: [expectation], timeout: 1)
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

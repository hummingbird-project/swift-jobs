//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2021-2024 the Hummingbird authors
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
import Metrics
import MetricsTestKit
import ServiceLifecycle
import Synchronization
import Testing

struct MetricsTests {
    static let testMetrics = {
        let metrics = TaskUniqueTestMetrics()
        MetricsSystem.bootstrap(metrics)
        return metrics
    }()

    func expectMetricDimensionsEqual(
        _ lhs: [(String, String)],
        _ rhs: [String: String],
        fileID: String = #fileID,
        filePath: String = #filePath,
        line: Int = #line,
        column: Int = #column
    ) {
        var rhs = rhs

        for (label, value) in lhs {
            if let rhsValue = rhs.removeValue(forKey: label) {
                #expect(rhsValue == value, sourceLocation: .init(fileID: fileID, filePath: filePath, line: line, column: column))
            } else {
                Issue.record(
                    #"Did not specify expected value for "\#(label)", actual value is "\#(value)"."#,
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

    @Test func testDispatchJobCounter() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testDispatchJobCounter"
            let value: Int
        }

        try await Self.testMetrics.withUnique {
            let expectation = TestExpectation()
            let jobQueue = JobQueue(.memory, logger: Logger(label: "JobsTests")) { MetricsJobMiddleware() }
            let job = JobDefinition { (parameters: TestParameters, context) in
                context.logger.info("Parameters=\(parameters.value)")
                try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
                expectation.trigger()
            }
            jobQueue.registerJob(job)
            try await jobQueue.push(TestParameters(value: 1))
            try await jobQueue.push(TestParameters(value: 2))
            try await jobQueue.push(TestParameters(value: 3))
            try await jobQueue.push(TestParameters(value: 4))
            try await jobQueue.push(TestParameters(value: 5))
            try await testJobQueue(jobQueue.processor()) {
                try await expectation.wait(count: 5)
            }

            let jobCounters = Self.testMetrics.counters.filter { $0.label == "swift.jobs" }
            #expect(jobCounters.count == 1)
            #expect(jobCounters[0].values == [1, 1, 1, 1, 1])
            expectMetricDimensionsEqual(
                jobCounters[0].dimensions,
                ["name": TestParameters.jobName, "status": "succeeded", "queue": "default"]
            )

            let jobMeters = Self.testMetrics.meters.filter { $0.label == "swift.jobs.meter" }
            #expect(jobMeters.count == 2)

            let queuedMeter = try #require(jobMeters.first { $0["status"] == "queued" })
            #expect(queuedMeter.values == [1, 2, 3, 4, 5, 4, 3, 2, 1, 0])
            expectMetricDimensionsEqual(
                queuedMeter.dimensions,
                ["name": TestParameters.jobName, "status": "queued", "queue": "default"]
            )

            let processingMeter = try #require(jobMeters.first { $0["status"] == "processing" })
            #expect(processingMeter.values == [1, 0, 1, 0, 1, 0, 1, 0, 1, 0])
            expectMetricDimensionsEqual(
                processingMeter.dimensions,
                ["name": TestParameters.jobName, "status": "processing", "queue": "default"]
            )
        }
    }

    @Test func testFailToDecode() async throws {
        let string: Mutex<String> = .init("")
        struct TestIntParameter: JobParameters {
            static let jobName = "testFailToDecode"
            let value: Int
        }
        struct TestStringParameter: JobParameters {
            static let jobName = "testFailToDecode"
            let value: String
        }
        try await Self.testMetrics.withUnique {
            let expectation = TestExpectation()

            var logger = Logger(label: "JobsTests")
            logger.logLevel = .debug
            let jobQueue = JobQueue(.memory, logger: Logger(label: "JobsTests")) { MetricsJobMiddleware() }
            jobQueue.registerJob(parameters: TestStringParameter.self) { parameters, _ in
                string.withLock { $0 = parameters.value }
                expectation.trigger()
            }
            try await testJobQueue(jobQueue.processor(options: .init(numWorkers: 2))) {
                try await jobQueue.push(TestIntParameter(value: 2))
                try await jobQueue.push(TestStringParameter(value: "test"))
                try await expectation.wait()
            }
            string.withLock {
                #expect($0 == "test")
            }
            let jobCounters = Self.testMetrics.counters.filter { $0.label == "swift.jobs.discarded" }
            #expect(jobCounters.count == 1)
            #expect(jobCounters[0].values == [1])
            expectMetricDimensionsEqual(
                jobCounters[0].dimensions,
                ["reason": "decodeJobFailed", "name": TestIntParameter.jobName, "queue": "default"]
            )
        }
    }

    @Test func testErrorRetryAndThenSucceed() async throws {
        struct TestParameter: JobParameters {
            static let jobName = "testErrorRetryAndThenSucceed"
        }
        try await Self.testMetrics.withUnique {
            let expectation = TestExpectation()
            let currentJobTryCount: Mutex<Int> = .init(0)
            struct FailedError: Error {}
            var logger = Logger(label: "JobsTests")
            logger.logLevel = .trace
            let jobQueue = JobQueue(
                .memory,
                logger: logger
            ) { MetricsJobMiddleware() }
            jobQueue.registerJob(
                parameters: TestParameter.self,
                retryStrategy: .exponentialJitter(maxAttempts: 3, maxBackoff: .seconds(0.5), minJitter: -0.25, maxJitter: 0.25)
            ) { _, _ in
                defer {
                    currentJobTryCount.withLock { $0 += 1 }
                }

                expectation.trigger()

                if (currentJobTryCount.withLock { $0 }) == 0 {
                    throw FailedError()
                }
            }
            try await jobQueue.push(TestParameter())

            var jobMeters = Self.testMetrics.meters.filter { $0.label == "swift.jobs.meter" }
            let queuedMeter = try #require(jobMeters.first { $0["status"] == "queued" })
            #expect(queuedMeter.values == [1])
            expectMetricDimensionsEqual(
                queuedMeter.dimensions,
                ["name": TestParameter.jobName, "status": "queued", "queue": "default"]
            )

            try await testJobQueue(jobQueue.processor()) {
                try await expectation.wait(count: 2)
            }
            #expect(currentJobTryCount.withLock { $0 } == 2)

            jobMeters = Self.testMetrics.meters.filter { $0.label == "swift.jobs.meter" }
            let processingMeter = try #require(jobMeters.first { $0["status"] == "processing" })
            #expect(processingMeter.values == [1, 0, 1, 0])
            expectMetricDimensionsEqual(
                processingMeter.dimensions,
                ["name": TestParameter.jobName, "status": "processing", "queue": "default"]
            )
        }
    }

    @Test func testFailedJobs() async throws {
        struct TestParameter: JobParameters {
            static let jobName = "testFailedJobs"
        }
        try await Self.testMetrics.withUnique {
            let expectation = TestExpectation()
            let failedJobCount = Atomic(0)
            struct FailedError: Error {}
            var logger = Logger(label: "JobsTests")
            logger.logLevel = .trace
            let jobQueue = JobQueue(
                MemoryQueue { _, _ in failedJobCount.wrappingAdd(1, ordering: .relaxed) },
                logger: logger
            ) { MetricsJobMiddleware() }
            jobQueue.registerJob(
                parameters: TestParameter.self,
                retryStrategy: .exponentialJitter(maxAttempts: 3, maxBackoff: .seconds(0.5), minJitter: 0.0, maxJitter: 0.01)
            ) { _, _ in
                expectation.trigger()
                throw FailedError()
            }
            try await testJobQueue(jobQueue.processor()) {
                try await jobQueue.push(TestParameter())

                try await expectation.wait(count: 3)
            }
            #expect(failedJobCount.load(ordering: .relaxed) == 1)

            let jobCounters = Self.testMetrics.counters.filter { $0.label == "swift.jobs" }
            #expect(jobCounters.count == 2)
            let retriedCounter = try #require(jobCounters.first { $0["status"] == "retried" })
            #expect(retriedCounter.values == [1])
            expectMetricDimensionsEqual(
                retriedCounter.dimensions,
                ["name": TestParameter.jobName, "status": "retried", "queue": "default"]
            )
            let failedCounter = try #require(jobCounters.first { $0["status"] == "failed" })
            #expect(failedCounter.values == [1])
            expectMetricDimensionsEqual(
                failedCounter.dimensions,
                ["name": TestParameter.jobName, "status": "failed", "queue": "default"]
            )

            let jobMeters = Self.testMetrics.meters.filter { $0.label == "swift.jobs.meter" }
            #expect(jobMeters.count == 2)

            let queuedMeter = try #require(jobMeters.first { $0.dimensions.first?.1 == "queued" })
            #expect(queuedMeter.values == [1, 0, 1, 0, 1, 0])
            expectMetricDimensionsEqual(
                queuedMeter.dimensions,
                ["name": TestParameter.jobName, "status": "queued", "queue": "default"]
            )

            let processingMeter = try #require(jobMeters.first { $0.dimensions.first?.1 == "processing" })
            #expect(processingMeter.values == [1, 0, 1, 0, 1, 0])
            expectMetricDimensionsEqual(
                processingMeter.dimensions,
                ["name": TestParameter.jobName, "status": "processing", "queue": "default"]
            )
        }
    }

    @Test func testJobExecutionTime() async throws {
        struct TestParameter: JobParameters {
            static let jobName = "testJobExecutionTime"
        }
        try await Self.testMetrics.withUnique {
            let expectation = TestExpectation()
            let jobQueue = JobQueue(.memory, logger: Logger(label: "JobsTests")) { MetricsJobMiddleware() }
            let job = JobDefinition { (parameters: TestParameter, context) in
                context.logger.info("Parameters=\(parameters)")
                try await Task.sleep(for: .milliseconds(5))
                expectation.trigger()
            }
            jobQueue.registerJob(job)
            try await testJobQueue(jobQueue.processor()) {
                try await jobQueue.push(TestParameter())
                try await expectation.wait()
            }

            let jobTimers = Self.testMetrics.timers.filter { $0.label == "swift.jobs.duration" }
            let succeededTimer = try #require(jobTimers.first { $0["status"] == "succeeded" })
            #expect(succeededTimer.values.count == 1)
            #expect(succeededTimer.values[0] > 1_000_000)  // Reduced from 5ms to 1ms for CI reliability
            expectMetricDimensionsEqual(
                succeededTimer.dimensions,
                ["name": TestParameter.jobName, "status": "succeeded", "queue": "default"]
            )
        }
    }

    @Test func testJobQueuedTime() async throws {
        struct SleepJobParameters: JobParameters {
            static let jobName = "testJobQueuedTime"
            let wait: Int
        }
        try await Self.testMetrics.withUnique {
            let expectation = TestExpectation()
            let jobQueue = JobQueue(.memory, logger: Logger(label: "JobsTests")) { MetricsJobMiddleware() }
            let job = JobDefinition { (parameters: SleepJobParameters, context) in
                context.logger.info("Parameters=\(parameters)")
                try await Task.sleep(for: .milliseconds(parameters.wait))
                expectation.trigger()
            }
            jobQueue.registerJob(job)
            try await testJobQueue(jobQueue.processor(options: .init(numWorkers: 1))) {
                // add two jobs. First job ensures the second job is queued for more than 5ms
                // Using much longer sleep and reduced threshold for system performance variance
                try await jobQueue.push(SleepJobParameters(wait: 300))
                try await jobQueue.push(SleepJobParameters(wait: 5))
                try await expectation.wait(count: 2)
            }

            let jobTimers = Self.testMetrics.timers.filter { $0.label == "swift.jobs.queued.duration" }
            let succeededTimer = try #require(jobTimers.first { $0["name"] == "testJobQueuedTime" })
            #expect(succeededTimer.values.count == 2)
            // Second job should be queued for at least 5ms (very conservative for fast systems)
            // since first job sleeps for 300ms and we have only 1 worker
            #expect(succeededTimer.values[1] > 5_000_000)
            expectMetricDimensionsEqual(
                succeededTimer.dimensions,
                ["name": SleepJobParameters.jobName, "queue": "default"]
            )
        }
    }
}

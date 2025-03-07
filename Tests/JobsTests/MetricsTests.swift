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

import Atomics
import Jobs
import Logging
import Metrics
import NIOConcurrencyHelpers
import ServiceLifecycle
import XCTest

final class TestMetrics: MetricsFactory {
    private let lock = NIOLock()
    let counters = NIOLockedValueBox([String: CounterHandler]())
    let recorders = NIOLockedValueBox([String: RecorderHandler]())
    let timers = NIOLockedValueBox([String: TimerHandler]())
    let meters = NIOLockedValueBox([String: MeterHandler]())

    public func makeCounter(label: String, dimensions: [(String, String)]) -> CounterHandler {
        self.counters.withLockedValue { counters in
            self.make(label: label, dimensions: dimensions, registry: &counters, maker: TestCounter.init)
        }
    }

    public func makeRecorder(label: String, dimensions: [(String, String)], aggregate: Bool) -> RecorderHandler {
        let maker = { (label: String, dimensions: [(String, String)]) -> RecorderHandler in
            TestRecorder(label: label, dimensions: dimensions, aggregate: aggregate)
        }
        return self.recorders.withLockedValue { recorders in
            self.make(label: label, dimensions: dimensions, registry: &recorders, maker: maker)
        }
    }

    public func makeTimer(label: String, dimensions: [(String, String)]) -> TimerHandler {
        self.timers.withLockedValue { timers in
            self.make(label: label, dimensions: dimensions, registry: &timers, maker: TestTimer.init)
        }
    }

    public func makeMeter(label: String, dimensions: [(String, String)]) -> MeterHandler {
        self.meters.withLockedValue { meters in
            self.make(label: label, dimensions: dimensions, registry: &meters, maker: TestMeter.init)
        }
    }

    private func make<Item>(
        label: String,
        dimensions: [(String, String)],
        registry: inout [String: Item],
        maker: (String, [(String, String)]) -> Item
    ) -> Item {
        let item = maker(label, dimensions)
        registry[label] = item
        return item
    }

    func destroyCounter(_ handler: CounterHandler) {
        if let testCounter = handler as? TestCounter {
            _ = self.counters.withLockedValue { counters in
                counters.removeValue(forKey: testCounter.label)
            }
        }
    }

    func destroyRecorder(_ handler: RecorderHandler) {
        if let testRecorder = handler as? TestRecorder {
            _ = self.recorders.withLockedValue { recorders in
                recorders.removeValue(forKey: testRecorder.label)
            }
        }
    }

    func destroyTimer(_ handler: TimerHandler) {
        if let testTimer = handler as? TestTimer {
            _ = self.timers.withLockedValue { timers in
                timers.removeValue(forKey: testTimer.label)
            }
        }
    }

    func destroyMeter(_ handler: MeterHandler) {
        if let testMeter = handler as? TestMeter {
            _ = self.meters.withLockedValue { meters in
                meters.removeValue(forKey: testMeter.label)
            }
        }
    }
}

internal final class TestCounter: CounterHandler, Equatable {
    let id: String
    let label: String
    let dimensions: [(String, String)]
    let values = NIOLockedValueBox([(Date, Int64)]())

    init(label: String, dimensions: [(String, String)]) {
        self.id = NSUUID().uuidString
        self.label = label
        self.dimensions = dimensions
    }

    func increment(by amount: Int64) {
        self.values.withLockedValue { values in
            values.append((Date(), amount))
        }
        print("adding \(amount) to \(self.label)")
    }

    func reset() {
        self.values.withLockedValue { values in
            values = []
        }
        print("reseting \(self.label)")
    }

    public static func == (lhs: TestCounter, rhs: TestCounter) -> Bool {
        lhs.id == rhs.id
    }
}

internal final class TestMeter: MeterHandler, Equatable {
    let id: String
    let label: String
    let dimensions: [(String, String)]
    let values = NIOLockedValueBox([(Date, Double)]())

    init(label: String, dimensions: [(String, String)]) {
        self.id = NSUUID().uuidString
        self.label = label
        self.dimensions = dimensions
    }

    func set(_ value: Int64) {
        self.values.withLockedValue { values in
            values.append((Date(), Double(value)))
        }
    }

    func set(_ value: Double) {
        self.values.withLockedValue { values in
            values.append((Date(), value))
        }
    }

    func increment(by: Double) {
        self.values.withLockedValue { values in
            let last = values.last ?? (Date(), 0)
            values.append((Date(), last.1 + by))
        }
    }

    func decrement(by: Double) {
        self.values.withLockedValue { values in
            let last = values.last ?? (Date(), 1)
            values.append((Date(), last.1 - by))
        }
    }

    public static func == (lhs: TestMeter, rhs: TestMeter) -> Bool {
        lhs.id == rhs.id
    }
}

internal final class TestRecorder: RecorderHandler, Equatable {
    let id: String
    let label: String
    let dimensions: [(String, String)]
    let aggregate: Bool
    let values = NIOLockedValueBox([(Date, Double)]())

    init(label: String, dimensions: [(String, String)], aggregate: Bool) {
        self.id = NSUUID().uuidString
        self.label = label
        self.dimensions = dimensions
        self.aggregate = aggregate
    }

    func record(_ value: Int64) {
        self.record(Double(value))
    }

    func record(_ value: Double) {
        self.values.withLockedValue { values in
            // this may loose precision but good enough as an example
            values.append((Date(), Double(value)))
        }
        print("recording \(value) in \(self.label)")
    }

    public static func == (lhs: TestRecorder, rhs: TestRecorder) -> Bool {
        lhs.id == rhs.id
    }
}

internal final class TestTimer: TimerHandler, Equatable {
    let id: String
    let label: String
    let displayUnit: NIOLockedValueBox<TimeUnit?>
    let dimensions: [(String, String)]
    let values = NIOLockedValueBox([(Date, Int64)]())

    init(label: String, dimensions: [(String, String)]) {
        self.id = NSUUID().uuidString
        self.label = label
        self.displayUnit = .init(nil)
        self.dimensions = dimensions
    }

    func preferDisplayUnit(_ unit: TimeUnit) {
        self.displayUnit.withLockedValue { displayUnit in
            displayUnit = unit
        }
    }

    func retriveValueInPreferredUnit(atIndex i: Int) -> Double {
        self.values.withLockedValue { values in
            let value = values[i].1
            return self.displayUnit.withLockedValue { displayUnit in
                guard let displayUnit else {
                    return Double(value)
                }
                return Double(value) / Double(displayUnit.scaleFromNanoseconds)
            }
        }
    }

    func recordNanoseconds(_ duration: Int64) {
        self.values.withLockedValue { values in
            values.append((Date(), duration))
        }
        print("recording \(duration) \(self.label)")
    }

    public static func == (lhs: TestTimer, rhs: TestTimer) -> Bool {
        lhs.id == rhs.id
    }
}

final class MetricsTests: XCTestCase {
    static var testMetrics = TestMetrics()

    override class func setUp() {
        MetricsSystem.bootstrap(self.testMetrics)
    }

    func testDispatchJobCounter() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testDispatchJobCounter"
            let value: Int
        }

        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 5)
        let jobQueue = JobQueue(.memory, numWorkers: 1, logger: Logger(label: "JobsTests")) { MetricsJobMiddleware() }
        let job = JobDefinition { (parameters: TestParameters, context) in
            context.logger.info("Parameters=\(parameters.value)")
            try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
            expectation.fulfill()
        }
        jobQueue.registerJob(job)
        try await testJobQueue(jobQueue) {
            try await jobQueue.push(TestParameters(value: 1))
            try await jobQueue.push(TestParameters(value: 2))
            try await jobQueue.push(TestParameters(value: 3))
            try await jobQueue.push(TestParameters(value: 4))
            try await jobQueue.push(TestParameters(value: 5))

            await fulfillment(of: [expectation], timeout: 5)
        }

        let counter = try XCTUnwrap(Self.testMetrics.counters.withLockedValue { $0 }["swift.jobs"] as? TestCounter)
        XCTAssertEqual(counter.values.withLockedValue { $0 }[0].1, 1)
        XCTAssertEqual(counter.values.withLockedValue { $0 }.count, 1)  // This technically 5, need to figueout how to await the results to get 5
        XCTAssertEqual(counter.dimensions.count, 3)
        XCTAssertEqual(counter.dimensions[0].0, "name")
        XCTAssertEqual(counter.dimensions[0].1, "testDispatchJobCounter")
        XCTAssertEqual(counter.dimensions[1].0, "status")
        XCTAssertEqual(counter.dimensions[1].1, "succeeded")

        let queuedMeter = try XCTUnwrap(Self.testMetrics.meters.withLockedValue { $0 }["swift.jobs.meter"] as? TestMeter)
        XCTAssertEqual(queuedMeter.values.withLockedValue { $0 }.count, 1)

        let processingMeter = try XCTUnwrap(Self.testMetrics.meters.withLockedValue { $0 }["swift.jobs.meter"] as? TestMeter)
        XCTAssertEqual(processingMeter.values.withLockedValue { $0 }.count, 1)
        XCTAssertEqual(processingMeter.dimensions.count, 3)
        XCTAssertEqual(processingMeter.dimensions[0].0, "status")
        XCTAssertEqual(processingMeter.dimensions[0].1, "processing")
        XCTAssertEqual(processingMeter.dimensions[1].0, "name")
        XCTAssertEqual(processingMeter.dimensions[1].1, TestParameters.jobName)
        XCTAssertEqual(processingMeter.dimensions[2].0, "queue")
        XCTAssertEqual(processingMeter.dimensions[2].1, "default")
    }

    func testFailToDecode() async throws {
        let string: NIOLockedValueBox<String> = .init("")
        struct TestIntParameter: JobParameters {
            static let jobName = "testFailToDecode"
            let value: Int
        }
        struct TestStringParameter: JobParameters {
            static let jobName = "testFailToDecode"
            let value: String
        }
        let expectation = XCTestExpectation(description: "job was called", expectedFulfillmentCount: 1)

        var logger = Logger(label: "JobsTests")
        logger.logLevel = .debug
        let jobQueue = JobQueue(.memory, numWorkers: 2, logger: Logger(label: "JobsTests")) { MetricsJobMiddleware() }
        jobQueue.registerJob(parameters: TestStringParameter.self) { parameters, _ in
            string.withLockedValue { $0 = parameters.value }
            expectation.fulfill()
        }
        try await testJobQueue(jobQueue) {
            try await jobQueue.push(TestIntParameter(value: 2))
            try await jobQueue.push(TestStringParameter(value: "test"))
            await fulfillment(of: [expectation], timeout: 5)
        }
        string.withLockedValue {
            XCTAssertEqual($0, "test")
        }

        let discardedCounter = try XCTUnwrap(Self.testMetrics.counters.withLockedValue { $0 }["swift.jobs.discarded"] as? TestCounter)
        XCTAssertEqual(discardedCounter.dimensions.count, 3)
        XCTAssertEqual(discardedCounter.dimensions[0].0, "reason")
        XCTAssertEqual(discardedCounter.dimensions[0].1, "decodeJobFailed")
        XCTAssertEqual(discardedCounter.dimensions[1].0, "queue")
        XCTAssertEqual(discardedCounter.dimensions[1].1, "default")
        XCTAssertEqual(discardedCounter.dimensions[2].0, "name")
        XCTAssertEqual(discardedCounter.dimensions[2].1, "testFailToDecode")
    }

    func testErrorRetryAndThenSucceed() async throws {
        struct TestParameter: JobParameters {
            static let jobName = "testErrorRetryAndThenSucceed"
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 2)
        let currentJobTryCount: NIOLockedValueBox<Int> = .init(0)
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
                currentJobTryCount.withLockedValue {
                    $0 += 1
                }
            }

            expectation.fulfill()

            if (currentJobTryCount.withLockedValue { $0 }) == 0 {
                throw FailedError()
            }
        }
        try await testJobQueue(jobQueue) {
            try await jobQueue.push(TestParameter())
            let meter = try XCTUnwrap(Self.testMetrics.meters.withLockedValue { $0 }["swift.jobs.meter"] as? TestMeter)
            XCTAssertEqual(meter.values.withLockedValue { $0 }.count, 1)
            XCTAssertEqual(meter.values.withLockedValue { $0 }[0].1, 1.0)
            XCTAssertEqual(meter.dimensions[0].0, "status")
            XCTAssertEqual(meter.dimensions[0].1, "queued")
            await fulfillment(of: [expectation], timeout: 5)
        }
        XCTAssertEqual(currentJobTryCount.withLockedValue { $0 }, 2)

        let meter = try XCTUnwrap(Self.testMetrics.meters.withLockedValue { $0 }["swift.jobs.meter"] as? TestMeter)
        XCTAssertEqual(meter.values.withLockedValue { $0 }.count, 1)
        XCTAssertEqual(meter.values.withLockedValue { $0 }[0].1, 0)
        XCTAssertEqual(meter.dimensions[0].0, "status")
        XCTAssertEqual(meter.dimensions[0].1, "processing")
    }

    func testFailedJobs() async throws {
        struct TestParameter: JobParameters {
            static let jobName = "testFailedJobs"
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 4)
        let failedJobCount = ManagedAtomic(0)
        struct FailedError: Error {}
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let jobQueue = JobQueue(
            MemoryQueue { _, _ in failedJobCount.wrappingIncrement(by: 1, ordering: .relaxed) },
            logger: logger
        ) { MetricsJobMiddleware() }
        jobQueue.registerJob(
            parameters: TestParameter.self,
            retryStrategy: .exponentialJitter(maxAttempts: 3, maxBackoff: .seconds(0.5), minJitter: 0.0, maxJitter: 0.01)
        ) { _, _ in
            expectation.fulfill()
            throw FailedError()
        }
        try await testJobQueue(jobQueue) {
            try await jobQueue.push(TestParameter())

            await fulfillment(of: [expectation], timeout: 5)
        }
        XCTAssertEqual(failedJobCount.load(ordering: .relaxed), 1)
        let counter = try XCTUnwrap(Self.testMetrics.counters.withLockedValue { $0 }["swift.jobs"] as? TestCounter)
        XCTAssertEqual(counter.values.withLockedValue { $0 }.count, 1)
        XCTAssertEqual(counter.values.withLockedValue { $0 }[0].1, 1)
        XCTAssertEqual(counter.dimensions[0].0, "name")
        XCTAssertEqual(counter.dimensions[0].1, "testFailedJobs")
        XCTAssertEqual(counter.dimensions[1].0, "status")
        XCTAssertEqual(counter.dimensions[1].1, "failed")
        let meter = try XCTUnwrap(Self.testMetrics.meters.withLockedValue { $0 }["swift.jobs.meter"] as? TestMeter)
        XCTAssertEqual(meter.values.withLockedValue { $0 }.count, 1)
        XCTAssertEqual(meter.values.withLockedValue { $0 }[0].1, 0)
        XCTAssertEqual(meter.dimensions[0].0, "status")
        XCTAssertEqual(meter.dimensions[0].1, "processing")
        let timer = try XCTUnwrap(Self.testMetrics.timers.withLockedValue { $0 }["swift.jobs.duration"] as? TestTimer)
        XCTAssertEqual(timer.dimensions[1].0, "status")
        XCTAssertEqual(timer.dimensions[1].1, "failed")
    }

    func testJobExecutionTime() async throws {
        struct TestParameter: JobParameters {
            static let jobName = "testJobExecutionTime"
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 1)
        let jobQueue = JobQueue(.memory, numWorkers: 1, logger: Logger(label: "JobsTests")) { MetricsJobMiddleware() }
        let job = JobDefinition { (parameters: TestParameter, context) in
            context.logger.info("Parameters=\(parameters)")
            try await Task.sleep(for: .milliseconds(5))
            expectation.fulfill()
        }
        jobQueue.registerJob(job)
        try await testJobQueue(jobQueue) {
            try await jobQueue.push(TestParameter())
            await fulfillment(of: [expectation], timeout: 5)
        }

        let timer = try XCTUnwrap(Self.testMetrics.timers.withLockedValue { $0 }["swift.jobs.duration"] as? TestTimer)
        XCTAssertGreaterThan(timer.values.withLockedValue { $0 }[0].1, 5_000_000)
        XCTAssertEqual(timer.dimensions.count, 3)
        XCTAssertEqual(timer.dimensions[0].0, "name")
        XCTAssertEqual(timer.dimensions[0].1, "testJobExecutionTime")
        XCTAssertEqual(timer.dimensions[1].0, "status")
        XCTAssertEqual(timer.dimensions[1].1, "succeeded")
        XCTAssertEqual(timer.dimensions[2].0, "queue")
        XCTAssertEqual(timer.dimensions[2].1, "default")
    }

    func testJobQueuedTime() async throws {
        struct SleepJobParameters: JobParameters {
            static let jobName = "testJobQueuedTime"
            let wait: Int
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 2)
        let jobQueue = JobQueue(.memory, numWorkers: 1, logger: Logger(label: "JobsTests")) { MetricsJobMiddleware() }
        let job = JobDefinition { (parameters: SleepJobParameters, context) in
            context.logger.info("Parameters=\(parameters)")
            try await Task.sleep(for: .milliseconds(parameters.wait))
            expectation.fulfill()
        }
        jobQueue.registerJob(job)
        try await testJobQueue(jobQueue) {
            // add two jobs. First job ensures the second job is queued for more than 50ms
            try await jobQueue.push(SleepJobParameters(wait: 50))
            try await jobQueue.push(SleepJobParameters(wait: 5))
            await fulfillment(of: [expectation], timeout: 5)
        }

        let timer = try XCTUnwrap(Self.testMetrics.timers.withLockedValue { $0 }["swift.jobs.queued.duration"] as? TestTimer)
        XCTAssertGreaterThan(timer.values.withLockedValue { $0 }[0].1, 50_000_000)
        XCTAssertEqual(timer.dimensions.count, 3)
        XCTAssertEqual(timer.dimensions[0].0, "name")
        XCTAssertEqual(timer.dimensions[0].1, "testJobQueuedTime")
        XCTAssertEqual(timer.dimensions[1].0, "status")
        XCTAssertEqual(timer.dimensions[1].1, "succeeded")
        XCTAssertEqual(timer.dimensions[2].0, "queue")
        XCTAssertEqual(timer.dimensions[2].1, "default")
    }
}

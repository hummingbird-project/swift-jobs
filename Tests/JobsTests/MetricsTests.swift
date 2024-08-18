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
            return self.make(label: label, dimensions: dimensions, registry: &counters, maker: TestCounter.init)
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

    private func make<Item>(label: String, dimensions: [(String, String)], registry: inout [String: Item], maker: (String, [(String, String)]) -> Item) -> Item {
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
        return lhs.id == rhs.id
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
        return lhs.id == rhs.id
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
        return lhs.id == rhs.id
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
        return self.values.withLockedValue { values in
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
        return lhs.id == rhs.id
    }
}

final class MetricsTests: XCTestCase {
    static var testMetrics = TestMetrics()

    override class func setUp() {
        MetricsSystem.bootstrap(self.testMetrics)
    }

    func wait(for expectations: [XCTestExpectation], timeout: TimeInterval) async {
        #if (os(Linux) && swift(<5.9)) || swift(<5.8)
        super.wait(for: expectations, timeout: timeout)
        #else
        await fulfillment(of: expectations, timeout: timeout)
        #endif
    }

    /// Helper function for test a server
    ///
    /// Creates test client, runs test function abd ensures everything is
    /// shutdown correctly
    public func testJobQueue(
        _ jobQueue: Service,
        _ test: () async throws -> Void
    ) async throws {
        try await withThrowingTaskGroup(of: Void.self) { group in
            let serviceGroup = ServiceGroup(
                configuration: .init(
                    services: [jobQueue],
                    gracefulShutdownSignals: [.sigterm, .sigint],
                    logger: Logger(label: "JobQueueService")
                )
            )
            group.addTask {
                try await serviceGroup.run()
            }
            try await test()
            await serviceGroup.triggerGracefulShutdown()
        }
    }

    func testDispatchJobCounter() async throws {
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 5)
        let jobQueue = JobQueue(.memory, numWorkers: 1, logger: Logger(label: "JobsTests"))
        let job = JobDefinition(id: "testBasic") { (parameters: Int, context) in
            context.logger.info("Parameters=\(parameters)")
            try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
            expectation.fulfill()
        }
        jobQueue.registerJob(job)
        try await self.testJobQueue(jobQueue) {
            try await jobQueue.push(id: job.id, parameters: 1)
            try await jobQueue.push(id: job.id, parameters: 2)
            try await jobQueue.push(id: job.id, parameters: 3)
            try await jobQueue.push(id: job.id, parameters: 4)
            try await jobQueue.push(id: job.id, parameters: 5)

            await self.wait(for: [expectation], timeout: 5)
        }

        let counter = try XCTUnwrap(Self.testMetrics.counters.withLockedValue { $0 }["swift_jobs"] as? TestCounter)
        XCTAssertEqual(counter.values.withLockedValue { $0 }[0].1, 1)
        XCTAssertEqual(counter.values.withLockedValue { $0 }.count, 1) // This technically 5, need to figueout how to await the results to get 5
        XCTAssertEqual(counter.dimensions.count, 2)
        XCTAssertEqual(counter.dimensions[0].0, "name")
        XCTAssertEqual(counter.dimensions[0].1, "testBasic")
        XCTAssertEqual(counter.dimensions[1].0, "status")
        XCTAssertEqual(counter.dimensions[1].1, "succeeded")
        
        let queuedMeter = try XCTUnwrap(Self.testMetrics.meters.withLockedValue { $0 }["swift_jobs_meter"] as? TestMeter)
        XCTAssertEqual(queuedMeter.values.withLockedValue { $0 }.count, 1)
    }

    func testFailedJobs() async throws {
        let jobIdentifer = JobIdentifier<Int>(#function)
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 4)
        let failedJobCount = ManagedAtomic(0)
        struct FailedError: Error {}
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let jobQueue = JobQueue(
            MemoryQueue { _, _ in failedJobCount.wrappingIncrement(by: 1, ordering: .relaxed) },
            logger: logger
        )
        jobQueue.registerJob(id: jobIdentifer, maxRetryCount: 3) { _, _ in
            expectation.fulfill()
            throw FailedError()
        }
        try await self.testJobQueue(jobQueue) {
            try await jobQueue.push(id: jobIdentifer, parameters: 0)

            await self.wait(for: [expectation], timeout: 5)
        }
        XCTAssertEqual(failedJobCount.load(ordering: .relaxed), 1)
        let counter = try XCTUnwrap(Self.testMetrics.counters.withLockedValue { $0 }["swift_jobs"] as? TestCounter)
        XCTAssertEqual(counter.values.withLockedValue { $0 }.count, 1)
        XCTAssertEqual(counter.values.withLockedValue { $0 }[0].1, 1)
        XCTAssertEqual(counter.dimensions[0].0, "name")
        XCTAssertEqual(counter.dimensions[0].1, "testFailedJobs()")
        XCTAssertEqual(counter.dimensions[1].0, "status")
        XCTAssertEqual(counter.dimensions[1].1, "failed")
        let meter = try XCTUnwrap(Self.testMetrics.meters.withLockedValue { $0 }["swift_jobs_meter"] as? TestMeter)
        XCTAssertEqual(meter.values.withLockedValue { $0 }.count, 1)
        XCTAssertEqual(meter.values.withLockedValue { $0 }[0].1, 0)
        XCTAssertEqual(meter.dimensions[0].0, "status")
        XCTAssertEqual(meter.dimensions[0].1, "processing")
        let timer = try XCTUnwrap(Self.testMetrics.timers.withLockedValue { $0 }["swift_jobs_duration_seconds"] as? TestTimer)
        XCTAssertEqual(timer.dimensions[1].0, "status")
        XCTAssertEqual(timer.dimensions[1].1, "failed")
    }

    func testJobExecutionTime() async throws {
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 1)
        let jobQueue = JobQueue(.memory, numWorkers: 1, logger: Logger(label: "JobsTests"))
        let job = JobDefinition(id: "testBasic") { (parameters: Int, context) in
            context.logger.info("Parameters=\(parameters)")
            try await Task.sleep(for: .milliseconds(5))
            expectation.fulfill()
        }
        jobQueue.registerJob(job)
        try await self.testJobQueue(jobQueue) {
            try await jobQueue.push(id: job.id, parameters: 1)
            await self.wait(for: [expectation], timeout: 5)
        }

        let timer = try XCTUnwrap(Self.testMetrics.timers.withLockedValue { $0 }["swift_jobs_duration_seconds"] as? TestTimer)
        XCTAssertGreaterThan(timer.values.withLockedValue { $0 }[0].1, 5_000_000)
        XCTAssertEqual(timer.dimensions[0].0, "name")
        XCTAssertEqual(timer.dimensions[0].1, "testBasic")
        XCTAssertEqual(timer.dimensions[1].0, "status")
        XCTAssertEqual(timer.dimensions[1].1, "succeeded")
    }
}

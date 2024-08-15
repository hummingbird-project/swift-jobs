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

    var counters: [String: any CoreMetrics.CounterHandler] = [:]
    var recorders: [String: any CoreMetrics.RecorderHandler] = [:]
    var timers: [String: any CoreMetrics.TimerHandler] = [:]

    private func make<Item>(label: String, dimensions: [(String, String)], registry: inout [String: Item], maker: (String, [(String, String)]) -> Item) -> Item {
        return self.lock.withLock {
            let item = maker(label, dimensions)
            registry[label] = item
            return item
        }
    }

    func makeCounter(label: String, dimensions: [(String, String)]) -> any CoreMetrics.CounterHandler {
        return self.make(label: label, dimensions: dimensions, registry: &self.counters, maker: TestCounter.init)
    }

    func makeRecorder(label: String, dimensions: [(String, String)], aggregate: Bool) -> any CoreMetrics.RecorderHandler {
        let maker = { (label: String, dimensions: [(String, String)]) -> RecorderHandler in
            TestRecorder(label: label, dimensions: dimensions, aggregate: aggregate)
        }
        return self.make(label: label, dimensions: dimensions, registry: &self.recorders, maker: maker)
    }

    func makeTimer(label: String, dimensions: [(String, String)]) -> any CoreMetrics.TimerHandler {
        return self.make(label: label, dimensions: dimensions, registry: &self.timers, maker: TestTimer.init)
    }

    func destroyCounter(_ handler: any CoreMetrics.CounterHandler) {
        if let testCounter = handler as? TestCounter {
            self.counters.removeValue(forKey: testCounter.label)
        }
    }

    func destroyRecorder(_ handler: any CoreMetrics.RecorderHandler) {
        if let testRecorder = handler as? TestRecorder {
            self.recorders.removeValue(forKey: testRecorder.label)
        }
    }

    func destroyTimer(_ handler: any CoreMetrics.TimerHandler) {
        if let testTimer = handler as? TestTimer {
            self.timers.removeValue(forKey: testTimer.label)
        }
    }
}

internal class TestRecorder: RecorderHandler, Equatable {
    let id: String
    let label: String
    let dimensions: [(String, String)]
    let aggregate: Bool

    let lock = NIOLock()
    var values = [(Date, Double)]()

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
        self.lock.withLock {
            // this may loose precision but good enough as an example
            self.values.append((Date(), Double(value)))
        }
        print("recording \(value) in \(self.label)")
    }

    public static func == (lhs: TestRecorder, rhs: TestRecorder) -> Bool {
        return lhs.id == rhs.id
    }
}

internal class TestCounter: CounterHandler, Equatable {
    let id: String
    let label: String
    let dimensions: [(String, String)]

    let lock = NIOLock()
    var values = [(Date, Int64)]()

    init(label: String, dimensions: [(String, String)]) {
        self.id = NSUUID().uuidString
        self.label = label
        self.dimensions = dimensions
    }

    func increment(by amount: Int64) {
        self.lock.withLock {
            self.values.append((Date(), amount))
        }
        print("adding \(amount) to \(self.label)")
    }

    func reset() {
        self.lock.withLock {
            self.values = []
        }
        print("reseting \(self.label)")
    }

    public static func == (lhs: TestCounter, rhs: TestCounter) -> Bool {
        return lhs.id == rhs.id
    }
}

internal class TestTimer: TimerHandler, Equatable {
    let id: String
    let label: String
    var displayUnit: TimeUnit?
    let dimensions: [(String, String)]

    let lock = NIOLock()
    var values = [(Date, Int64)]()

    init(label: String, dimensions: [(String, String)]) {
        self.id = NSUUID().uuidString
        self.label = label
        self.displayUnit = nil
        self.dimensions = dimensions
    }

    func preferDisplayUnit(_ unit: TimeUnit) {
        self.lock.withLock {
            self.displayUnit = unit
        }
    }

    func retriveValueInPreferredUnit(atIndex i: Int) -> Double {
        return self.lock.withLock {
            let value = self.values[i].1
            guard let displayUnit = self.displayUnit else {
                return Double(value)
            }
            return Double(value) / Double(displayUnit.scaleFromNanoseconds)
        }
    }

    func recordNanoseconds(_ duration: Int64) {
        self.lock.withLock {
            self.values.append((Date(), duration))
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

        let counter = try XCTUnwrap(Self.testMetrics.counters["queued_jobs_counter"] as? TestCounter)

        XCTAssertEqual(counter.values[0].1, 1)
        XCTAssertEqual(counter.values.count, 1) // This technically 5, need to figueout how to await the results to get 5
        XCTAssertEqual(counter.dimensions.count, 1)
        XCTAssertEqual(counter.dimensions[0].0, "queue_name")
        XCTAssertEqual(counter.dimensions[0].1, "testBasic")
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
        let counter = try XCTUnwrap(Self.testMetrics.counters["failed_jobs_counter"] as? TestCounter)
        XCTAssertEqual(counter.values.count, 1)
        XCTAssertEqual(counter.values[0].1, 1)
        XCTAssertEqual(counter.dimensions[0].0, "queue_name")
        XCTAssertEqual(counter.dimensions[0].1, "testFailedJobs()")
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

        let timer = try XCTUnwrap(Self.testMetrics.timers["testBasic_job_duration"] as? TestTimer)
        XCTAssertGreaterThan(timer.values[0].1, 5_000_000)
    }
}

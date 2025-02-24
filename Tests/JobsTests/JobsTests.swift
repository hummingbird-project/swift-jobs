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
import NIOConcurrencyHelpers
import ServiceLifecycle
import XCTest

extension XCTestExpectation {
    convenience init(description: String, expectedFulfillmentCount: Int) {
        self.init(description: description)
        self.expectedFulfillmentCount = expectedFulfillmentCount
    }
}

final class JobsTests: XCTestCase {
    func testBasic() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testBasic"
            let value: Int
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 10)
        let jobQueue = JobQueue(.memory, numWorkers: 1, logger: Logger(label: "JobsTests"))
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
            try await jobQueue.push(TestParameters(value: 6))
            try await jobQueue.push(TestParameters(value: 7))
            try await jobQueue.push(TestParameters(value: 8))
            try await jobQueue.push(TestParameters(value: 9))
            try await jobQueue.push(TestParameters(value: 10))

            await fulfillment(of: [expectation], timeout: 5)
        }
    }

    func testMultipleWorkers() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testMultipleWorkers"
            let value: Int
        }
        let runningJobCounter = ManagedAtomic(0)
        let maxRunningJobCounter = ManagedAtomic(0)
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 10)

        let jobQueue = JobQueue(.memory, numWorkers: 4, logger: Logger(label: "JobsTests"))
        jobQueue.registerJob(parameters: TestParameters.self) { parameters, context in
            let runningJobs = runningJobCounter.wrappingIncrementThenLoad(by: 1, ordering: .relaxed)
            if runningJobs > maxRunningJobCounter.load(ordering: .relaxed) {
                maxRunningJobCounter.store(runningJobs, ordering: .relaxed)
            }
            try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
            context.logger.info("Parameters=\(parameters)")
            expectation.fulfill()
            runningJobCounter.wrappingDecrement(by: 1, ordering: .relaxed)
        }
        try await testJobQueue(jobQueue) {
            try await jobQueue.push(TestParameters(value: 1))
            try await jobQueue.push(TestParameters(value: 2))
            try await jobQueue.push(TestParameters(value: 3))
            try await jobQueue.push(TestParameters(value: 4))
            try await jobQueue.push(TestParameters(value: 5))
            try await jobQueue.push(TestParameters(value: 6))
            try await jobQueue.push(TestParameters(value: 7))
            try await jobQueue.push(TestParameters(value: 8))
            try await jobQueue.push(TestParameters(value: 9))
            try await jobQueue.push(TestParameters(value: 10))

            await fulfillment(of: [expectation], timeout: 5)

            XCTAssertGreaterThan(maxRunningJobCounter.load(ordering: .relaxed), 1)
            XCTAssertLessThanOrEqual(maxRunningJobCounter.load(ordering: .relaxed), 4)
        }
    }

    func testErrorRetryAndThenSucceed() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testErrorRetryAndThenSucceed"
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 2)
        let currentJobTryCount: NIOLockedValueBox<Int> = .init(0)
        struct FailedError: Error {}
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let jobQueue = JobQueue(
            .memory,
            logger: logger,
            options: .init(
                maxJitter: 0.25,
                minJitter: 0.01
            )
        )
        jobQueue.registerJob(parameters: TestParameters.self, maxRetryCount: 3) { _, _ in
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
            try await jobQueue.push(TestParameters())
            await fulfillment(of: [expectation], timeout: 5)
        }
        XCTAssertEqual(currentJobTryCount.withLockedValue { $0 }, 2)
    }

    func testErrorRetryCount() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testErrorRetryCount"
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 4)
        let failedJobCount = ManagedAtomic(0)
        struct FailedError: Error {}
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let jobQueue = JobQueue(
            MemoryQueue { _, _ in failedJobCount.wrappingIncrement(by: 1, ordering: .relaxed) },
            logger: logger,
            options: .init(
                maximumBackoff: 0.5,
                maxJitter: 0.01,
                minJitter: 0.0
            )
        )
        jobQueue.registerJob(parameters: TestParameters.self, maxRetryCount: 3) { _, _ in
            expectation.fulfill()
            throw FailedError()
        }
        try await testJobQueue(jobQueue) {
            try await jobQueue.push(TestParameters())

            await fulfillment(of: [expectation], timeout: 5)
        }
        XCTAssertEqual(failedJobCount.load(ordering: .relaxed), 1)
    }

    func testDelayedJob() async throws {
        struct TestParameters: JobParameters, Equatable {
            static let jobName = "testDelayedJob"
            let value: Int
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 2)
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .debug
        let jobQueue = JobQueue(.memory, numWorkers: 1, logger: logger)
        let delayedJob = ManagedAtomic(0)
        let delayedJobParameters = TestParameters(value: 23)
        let notDelayedJobParameters = TestParameters(value: 89)
        let jobExecutionSequence: NIOLockedValueBox<[TestParameters]> = .init([])
        jobQueue.registerJob(parameters: TestParameters.self) { parameters, context in
            context.logger.info("Parameters=\(parameters)")
            jobExecutionSequence.withLockedValue {
                $0.append(parameters)
            }
            expectation.fulfill()

            if parameters == delayedJobParameters {
                delayedJob.wrappingDecrement(by: 1, ordering: .relaxed)
            }
        }
        try await testJobQueue(jobQueue) {
            delayedJob.wrappingIncrement(by: 1, ordering: .relaxed)
            try await jobQueue.push(delayedJobParameters, options: .init(delayUntil: Date.now.addingTimeInterval(1)))
            delayedJob.wrappingIncrement(by: 1, ordering: .relaxed)
            try await jobQueue.push(notDelayedJobParameters)
            XCTAssertEqual(delayedJob.load(ordering: .relaxed), 2)
            await fulfillment(of: [expectation], timeout: 5)
            XCTAssertEqual(delayedJob.load(ordering: .relaxed), 1)
        }

        XCTAssertEqual(jobExecutionSequence.withLockedValue { $0 }, [notDelayedJobParameters, delayedJobParameters])
    }

    func testJobParameters() async throws {
        struct TestJobParameters: JobParameters {
            static let jobName: String = "TestJobParameters"
            let id: Int
            let message: String
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called")
        let jobQueue = JobQueue(.memory, numWorkers: 1, logger: Logger(label: "JobsTests"))
        jobQueue.registerJob(parameters: TestJobParameters.self) { parameters, _ in
            XCTAssertEqual(parameters.id, 23)
            XCTAssertEqual(parameters.message, "Hello!")
            expectation.fulfill()
        }
        try await testJobQueue(jobQueue) {
            try await jobQueue.push(TestJobParameters(id: 23, message: "Hello!"))

            await fulfillment(of: [expectation], timeout: 5)
        }
    }

    /// Verify test job is cancelled when service group is cancelled
    func testShutdownJob() async throws {
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 1)

        let cancelledJobCount = ManagedAtomic(0)
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let jobQueue = JobQueue(
            MemoryQueue { _, error in
                if error is CancellationError {
                    cancelledJobCount.wrappingIncrement(by: 1, ordering: .relaxed)
                }
            },
            numWorkers: 4,
            logger: logger
        )
        struct SleepJobParameters: JobParameters {
            static let jobName = "Sleep"
            let length: Duration
        }
        jobQueue.registerJob(parameters: SleepJobParameters.self) { parameters, _ in
            expectation.fulfill()
            try await Task.sleep(for: parameters.length)
        }
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
            try await jobQueue.push(SleepJobParameters(length: .milliseconds(1000)))
            await fulfillment(of: [expectation], timeout: 5)
            group.cancelAll()
        }

        XCTAssertEqual(cancelledJobCount.load(ordering: .relaxed), 1)
    }

    /// test job fails to decode but queue continues to process
    func testFailToDecode() async throws {
        struct TestIntParameter: JobParameters {
            static let jobName = "testFailToDecode"
            let value: Int
        }
        struct TestStringParameter: JobParameters {
            static let jobName = "testFailToDecode"
            let value: String
        }
        let string: NIOLockedValueBox<String> = .init("")
        let expectation = XCTestExpectation(description: "job was called", expectedFulfillmentCount: 1)

        var logger = Logger(label: "JobsTests")
        logger.logLevel = .debug
        let jobQueue = JobQueue(.memory, numWorkers: 1, logger: logger)
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
    }

    func testMultipleJobQueueHandlers() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testMultipleJobQueueHandlers"
            let value: Int
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 200)
        let job = JobDefinition(parameters: TestParameters.self) { parameters, context in
            context.logger.info("Parameters=\(parameters)")
            try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
            expectation.fulfill()
        }
        let logger = {
            var logger = Logger(label: "JobsTests")
            logger.logLevel = .debug
            return logger
        }()
        let jobQueue = JobQueue(.memory, numWorkers: 2, logger: Logger(label: "JobsTests"))
        jobQueue.registerJob(job)
        let jobQueue2 = JobQueue(.memory, numWorkers: 1, logger: Logger(label: "JobsTests"))
        jobQueue2.registerJob(job)

        try await withThrowingTaskGroup(of: Void.self) { group in
            let serviceGroup = ServiceGroup(
                configuration: .init(
                    services: [jobQueue, jobQueue2],
                    gracefulShutdownSignals: [.sigterm, .sigint],
                    logger: logger
                )
            )
            group.addTask {
                try await serviceGroup.run()
            }
            do {
                for i in 0..<200 {
                    try await jobQueue.push(TestParameters(value: i))
                }
                await fulfillment(of: [expectation], timeout: 5)
                await serviceGroup.triggerGracefulShutdown()
            } catch {
                XCTFail("\(String(reflecting: error))")
                await serviceGroup.triggerGracefulShutdown()
                throw error
            }
        }
    }
}

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
import Logging
import NIOConcurrencyHelpers
import ServiceLifecycle
import XCTest

@testable import Jobs

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
            logger: logger
        )
        jobQueue.registerJob(
            parameters: TestParameters.self,
            retryStrategy: .exponentialJitter(maxAttempts: 3, minJitter: 0.01, maxJitter: 0.25)
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
            try await jobQueue.push(TestParameters())
            await fulfillment(of: [expectation], timeout: 5)
        }
        XCTAssertEqual(currentJobTryCount.withLockedValue { $0 }, 2)
    }

    func testErrorRetryCount() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testErrorRetryCount"
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 3)
        let failedJobCount = ManagedAtomic(0)
        let attemptCounter: NIOLockedValueBox<[Int]> = .init([])
        struct FailedError: Error {}
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let jobQueue = JobQueue(
            MemoryQueue { _, _ in failedJobCount.wrappingIncrement(by: 1, ordering: .relaxed) },
            logger: logger
        )
        jobQueue.registerJob(
            parameters: TestParameters.self,
            retryStrategy: .exponentialJitter(maxAttempts: 3, maxBackoff: .seconds(0.5), minJitter: 0.0, maxJitter: 0.01)
        ) { _, context in
            expectation.fulfill()
            attemptCounter.withLockedValue {
                $0.append(context.attempt)
            }
            throw FailedError()
        }
        try await testJobQueue(jobQueue) {
            try await jobQueue.push(TestParameters())

            await fulfillment(of: [expectation], timeout: 5)
        }
        XCTAssertEqual(failedJobCount.load(ordering: .relaxed), 1)
        XCTAssertEqual(attemptCounter.withLockedValue { $0 }, [1, 2, 3])
    }

    /// Test retry policy that does different things based on the error passed to it
    func testRetryHandlerErrorChecking() async throws {
        struct TestError: Error {}
        // retry strategy that retires indefinitely unless error is TestError
        struct TestRetryStrategy: JobRetryStrategy {
            let jitterRetry: ExponentialJitterJobRetryStrategy
            init() {
                self.jitterRetry = .init(maxAttempts: .max)
            }
            func shouldRetry(attempt: Int, error: any Error) -> Bool {
                if error is TestError { return false }
                return jitterRetry.shouldRetry(attempt: attempt, error: error)
            }

            func calculateBackoff(attempt: Int) -> Duration {
                jitterRetry.calculateBackoff(attempt: attempt)
            }
        }
        struct TestParameters: JobParameters {
            static let jobName = "testErrorRetryCount"
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 1)
        let failedJobCount = ManagedAtomic(0)
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let jobQueue = JobQueue(
            MemoryQueue { _, _ in failedJobCount.wrappingIncrement(by: 1, ordering: .relaxed) },
            logger: logger
        )
        jobQueue.registerJob(
            parameters: TestParameters.self,
            retryStrategy: TestRetryStrategy()
        ) { _, _ in
            expectation.fulfill()
            throw TestError()
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
        let delayedJobQueuedAt: NIOLockedValueBox<Date> = .init(Date.now)
        jobQueue.registerJob(parameters: TestParameters.self) { parameters, context in
            context.logger.info("Parameters=\(parameters)")
            jobExecutionSequence.withLockedValue {
                $0.append(parameters)
            }
            expectation.fulfill()

            if parameters == delayedJobParameters {
                delayedJob.wrappingDecrement(by: 1, ordering: .relaxed)
                delayedJobQueuedAt.withLockedValue {
                    $0 = context.queuedAt
                }
            }
        }
        try await testJobQueue(jobQueue) {
            delayedJob.wrappingIncrement(by: 1, ordering: .relaxed)
            try await jobQueue.push(delayedJobParameters, options: .init(delayUntil: Date.now.addingTimeInterval(1)))
            try await jobQueue.push(notDelayedJobParameters)
            XCTAssertEqual(delayedJob.load(ordering: .relaxed), 1)
            await fulfillment(of: [expectation], timeout: 5)
            XCTAssertEqual(delayedJob.load(ordering: .relaxed), 0)
        }

        XCTAssertEqual(jobExecutionSequence.withLockedValue { $0 }, [notDelayedJobParameters, delayedJobParameters])
        XCTAssertGreaterThan(Date.now, delayedJobQueuedAt.withLockedValue { $0 })
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

    /// Test we can user `any JobQueueProtocol` to register and push jobs
    func testJobQueueProtocol() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testJobQueueProtocol"
            let value: Int
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 1)
        let jobQueue: any JobQueueProtocol = JobQueue(.memory, numWorkers: 1, logger: Logger(label: "JobsTests"))
        let job = JobDefinition { (parameters: TestParameters, context) in
            context.logger.info("Parameters=\(parameters.value)")
            try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
            expectation.fulfill()
        }
        jobQueue.registerJob(job)
        try await testJobQueue(jobQueue) {
            try await jobQueue.push(TestParameters(value: 1))

            await fulfillment(of: [expectation], timeout: 5)
        }
    }

    func testJobTimeout() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testJobTimeout"
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 1)
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let jobQueue = JobQueue(
            MemoryQueue { _, error in
                XCTAssertEqual((error as? JobQueueError)?.code, .jobTimedOut)
                expectation.fulfill()
            },
            logger: logger
        )
        jobQueue.registerJob(
            parameters: TestParameters.self,
            retryStrategy: .dontRetry,
            timeout: .milliseconds(50)
        ) { _, _ in
            try await Task.sleep(for: .seconds(1))
        }
        try await testJobQueue(jobQueue) {
            try await jobQueue.push(TestParameters())

            await fulfillment(of: [expectation], timeout: 5)
        }
    }

    func testJobWithTimeoutThatSucceeded() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testJobWithoutTimeout"
            let value: Int
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 1)
        let failedJobCount = ManagedAtomic(0)
        let jobQueue = JobQueue(
            MemoryQueue { _, _ in failedJobCount.wrappingIncrement(by: 1, ordering: .relaxed) },
            numWorkers: 1,
            logger: Logger(label: "JobsTests")
        )
        let job = JobDefinition(timeout: .seconds(1)) { (parameters: TestParameters, context) in
            context.logger.info("Parameters=\(parameters.value)")
            try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
            expectation.fulfill()
        }
        jobQueue.registerJob(job)
        try await testJobQueue(jobQueue) {
            try await jobQueue.push(TestParameters(value: 1))

            await fulfillment(of: [expectation], timeout: 5)
        }
        XCTAssertEqual(failedJobCount.load(ordering: .relaxed), 0)
    }

    // verify advance by gives us at least millisecond accuracy across 30000 years
    func testAdvancedBy() {
        for _ in 0..<100 {
            let date = Date(timeIntervalSinceReferenceDate: TimeInterval.random(in: 0..<1_000_000_000_000))
            let offset = Double.random(in: 0..<1_000_000_000)
            let duration = Duration.seconds(offset)
            let newDate = date._advanced(by: duration)
            XCTAssertEqual(date.advanced(by: offset).timeIntervalSinceReferenceDate, newDate.timeIntervalSinceReferenceDate, accuracy: 0.001)
        }
    }

    func testCancelledJob() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testCancelledJob"
            let value: Int
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 1)
        let jobProcessed: NIOLockedValueBox<[Int]> = .init([])
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let jobQueue = JobQueue(
            .memory,
            logger: logger
        )
        jobQueue.registerJob(
            parameters: TestParameters.self,
            retryStrategy: .exponentialJitter(maxAttempts: 3, minJitter: 0.01, maxJitter: 0.25)
        ) { parameters, _ in
            jobProcessed.withLockedValue {
                $0.append(parameters.value)
            }
            expectation.fulfill()
        }
        try await withThrowingTaskGroup(of: Void.self) { group in
            let serviceGroup = ServiceGroup(
                configuration: .init(
                    services: [jobQueue],
                    gracefulShutdownSignals: [.sigterm, .sigint],
                    logger: Logger(label: "JobQueueService")
                )
            )

            let cancellable = try await jobQueue.push(TestParameters(value: 30))

            try await jobQueue.push(TestParameters(value: 15))

            try await jobQueue.cancelJob(jobID: cancellable)

            group.addTask {
                try await serviceGroup.run()
            }

            await fulfillment(of: [expectation], timeout: 5)
            await serviceGroup.triggerGracefulShutdown()
        }
        XCTAssertEqual(jobProcessed.withLockedValue { $0 }, [15])
    }

    func testPausedThenResume() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testPausedAndThenResume"
            let value: Int
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 2)
        let jobRunSequence: NIOLockedValueBox<[Int]> = .init([])
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let jobQueue = JobQueue(
            .memory,
            logger: logger
        )
        jobQueue.registerJob(
            parameters: TestParameters.self,
            retryStrategy: .exponentialJitter(maxAttempts: 3, minJitter: 0.01, maxJitter: 0.25)
        ) { parameters, context in
            context.logger.info("Parameters=\(parameters)")
            jobRunSequence.withLockedValue {
                $0.append(parameters.value)
            }
            expectation.fulfill()
        }

        try await withThrowingTaskGroup(of: Void.self) { group in
            let serviceGroup = ServiceGroup(
                configuration: .init(
                    services: [jobQueue],
                    gracefulShutdownSignals: [.sigterm, .sigint],
                    logger: Logger(label: "JobQueueService")
                )
            )

            let pausableJob = try await jobQueue.push(TestParameters(value: 15))

            try await jobQueue.push(TestParameters(value: 30))

            try await jobQueue.pauseJob(jobID: pausableJob)

            group.addTask {
                try await serviceGroup.run()
            }

            try await jobQueue.resumeJob(jobID: pausableJob)
            await fulfillment(of: [expectation], timeout: 5)
            await serviceGroup.triggerGracefulShutdown()
        }
        XCTAssertEqual(jobRunSequence.withLockedValue { $0 }, [30, 15])
    }

    func testJobQueueGracefulShutdownWaitsForJob() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testJobTimeout"
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 1)
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let jobQueue = JobQueue(
            .memory,
            logger: logger,
            options: .init(gracefulShutdownTimeout: nil)
        )
        jobQueue.registerJob(
            parameters: TestParameters.self,
            retryStrategy: .dontRetry
        ) { _, _ in
            try await Task.sleep(for: .milliseconds(100))
            expectation.fulfill()
        }
        try await testJobQueue(jobQueue) {
            _ = try await jobQueue.push(TestParameters())
        }
        await fulfillment(of: [expectation], timeout: 5)
    }

    func testJobQueueGracefulShutdownTimeout() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testJobTimeout"
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 1)
        let failedJobCount = ManagedAtomic(0)
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let jobQueue = JobQueue(
            MemoryQueue { _, error in
                XCTAssert(error is CancellationError)
                failedJobCount.wrappingIncrement(by: 1, ordering: .relaxed)
            },
            logger: logger,
            options: .init(gracefulShutdownTimeout: .milliseconds(50))
        )
        jobQueue.registerJob(
            parameters: TestParameters.self,
            retryStrategy: .dontRetry
        ) { _, _ in
            expectation.fulfill()
            try await Task.sleep(for: .seconds(10))
        }
        try await testJobQueue(jobQueue) {
            try await jobQueue.push(TestParameters())
            await fulfillment(of: [expectation], timeout: 5)
        }
        XCTAssertEqual(failedJobCount.load(ordering: .relaxed), 1)
    }
}

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
import Foundation
import Logging
import NIOCore
import ServiceLifecycle
import Synchronization
import Testing

@testable import Jobs

struct JobsTests {
    @Test func testBasic() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testBasic"
            let value: Int
        }
        let logger = Logger(label: "JobsTests")
        let expectation = TestExpectation()
        let jobQueue = JobQueue(.memory, logger: logger)
        let job = JobDefinition { (parameters: TestParameters, context) in
            context.logger.info("Parameters=\(parameters.value)")
            try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
            expectation.trigger()
        }
        jobQueue.registerJob(job)
        try await testJobQueue(JobQueueProcessor(queue: jobQueue, logger: logger)) {
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

            try await expectation.wait(count: 10)
        }
    }

    @Test func testJobNameBasic() async throws {
        let expectation = TestExpectation()
        let jobQueue = JobQueue(.memory, logger: Logger(label: "JobsTests"))
        let jobName = JobName<Int>("testJobNameBasic")
        jobQueue.registerJob(name: jobName) { parameters, context in
            context.logger.info("Parameters=\(parameters)")
            try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
            expectation.trigger()
        }
        try await testJobQueue(jobQueue.processor()) {
            try await jobQueue.push(jobName, parameters: 1)
            try await jobQueue.push(jobName, parameters: 2)
            try await jobQueue.push(jobName, parameters: 3)
            try await jobQueue.push(jobName, parameters: 4)
            try await jobQueue.push(jobName, parameters: 5)
            try await jobQueue.push(jobName, parameters: 6)

            try await expectation.wait(count: 6)
        }
    }

    @Test func testMultipleWorkers() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testMultipleWorkers"
            let value: Int
        }
        let runningJobCounter = ManagedAtomic(0)
        let maxRunningJobCounter = ManagedAtomic(0)
        let expectation = TestExpectation()

        let jobQueue = JobQueue(.memory, logger: Logger(label: "JobsTests"))
        jobQueue.registerJob(parameters: TestParameters.self) { parameters, context in
            let runningJobs = runningJobCounter.wrappingIncrementThenLoad(by: 1, ordering: .relaxed)
            if runningJobs > maxRunningJobCounter.load(ordering: .relaxed) {
                maxRunningJobCounter.store(runningJobs, ordering: .relaxed)
            }
            try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
            context.logger.info("Parameters=\(parameters)")
            expectation.trigger()
            runningJobCounter.wrappingDecrement(by: 1, ordering: .relaxed)
        }
        try await testJobQueue(jobQueue.processor(options: .init(numWorkers: 4))) {
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

            try await expectation.wait(count: 10)

            #expect(maxRunningJobCounter.load(ordering: .relaxed) > 1)
            #expect(maxRunningJobCounter.load(ordering: .relaxed) <= 4)
        }
    }

    @Test func testErrorRetryAndThenSucceed() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testErrorRetryAndThenSucceed"
        }
        let expectation = TestExpectation()
        let currentJobTryCount: Mutex<Int> = .init(0)
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
                currentJobTryCount.withLock {
                    $0 += 1
                }
            }

            expectation.trigger()
            if (currentJobTryCount.withLock { $0 }) == 0 {
                throw FailedError()
            }
        }
        try await testJobQueue(jobQueue.processor()) {
            try await jobQueue.push(TestParameters())
            try await expectation.wait(count: 2)
        }
        #expect(currentJobTryCount.withLock { $0 } == 2)
    }

    @Test func testErrorRetryCount() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testErrorRetryCount"
        }
        let expectation = TestExpectation()
        let failedJobCount = ManagedAtomic(0)
        let attemptCounter: Mutex<[Int]> = .init([])
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
            expectation.trigger()
            attemptCounter.withLock {
                $0.append(context.attempt)
            }
            throw FailedError()
        }
        try await testJobQueue(jobQueue.processor()) {
            try await jobQueue.push(TestParameters())

            try await expectation.wait(count: 3)
        }
        #expect(failedJobCount.load(ordering: .relaxed) == 1)
        #expect(attemptCounter.withLock { $0 } == [1, 2, 3])
    }

    /// Test retry policy that does different things based on the error passed to it
    @Test func testRetryHandlerErrorChecking() async throws {
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
        let expectation = TestExpectation()
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
            expectation.trigger()
            throw TestError()
        }
        try await testJobQueue(jobQueue.processor()) {
            try await jobQueue.push(TestParameters())

            try await expectation.wait()
        }
        #expect(failedJobCount.load(ordering: .relaxed) == 1)
    }

    @Test func testDelayedJob() async throws {
        struct TestParameters: JobParameters, Equatable {
            static let jobName = "testDelayedJob"
            let value: Int
        }
        let expectation = TestExpectation()
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .debug
        let jobQueue = JobQueue(.memory, logger: logger)
        let delayedJob = ManagedAtomic(0)
        let delayedJobParameters = TestParameters(value: 23)
        let notDelayedJobParameters = TestParameters(value: 89)
        let jobExecutionSequence: Mutex<[TestParameters]> = .init([])
        let delayedJobQueuedAt: Mutex<Date> = .init(Date.now)
        jobQueue.registerJob(parameters: TestParameters.self) { parameters, context in
            context.logger.info("Parameters=\(parameters)")
            jobExecutionSequence.withLock {
                $0.append(parameters)
            }
            expectation.trigger()

            if parameters == delayedJobParameters {
                delayedJob.wrappingDecrement(by: 1, ordering: .relaxed)
                delayedJobQueuedAt.withLock {
                    $0 = context.queuedAt
                }
            }
        }
        try await testJobQueue(jobQueue.processor()) {
            delayedJob.wrappingIncrement(by: 1, ordering: .relaxed)
            try await jobQueue.push(delayedJobParameters, options: .init(delayUntil: Date.now.addingTimeInterval(1)))
            try await jobQueue.push(notDelayedJobParameters)
            #expect(delayedJob.load(ordering: .relaxed) == 1)
            try await expectation.wait(count: 2)
            #expect(delayedJob.load(ordering: .relaxed) == 0)
        }

        #expect(jobExecutionSequence.withLock { $0 } == [notDelayedJobParameters, delayedJobParameters])
        #expect(Date.now > delayedJobQueuedAt.withLock { $0 })
    }

    @Test func testJobParameters() async throws {
        struct TestJobParameters: JobParameters {
            static let jobName: String = "TestJobParameters"
            let id: Int
            let message: String
        }
        let expectation = TestExpectation()
        let jobQueue = JobQueue(.memory, logger: Logger(label: "JobsTests"))
        jobQueue.registerJob(parameters: TestJobParameters.self) { parameters, _ in
            #expect(parameters.id == 23)
            #expect(parameters.message == "Hello!")
            expectation.trigger()
        }
        try await testJobQueue(jobQueue.processor()) {
            try await jobQueue.push(TestJobParameters(id: 23, message: "Hello!"))

            try await expectation.wait()
        }
    }

    /// Verify test job is cancelled when service group is cancelled
    @Test func testShutdownJob() async throws {
        let expectation = TestExpectation()

        let cancelledJobCount = ManagedAtomic(0)
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let jobQueue = JobQueue(
            MemoryQueue { _, error in
                if error is CancellationError {
                    cancelledJobCount.wrappingIncrement(by: 1, ordering: .relaxed)
                }
            },
            logger: logger
        )
        struct SleepJobParameters: JobParameters {
            static let jobName = "Sleep"
            let length: Duration
        }
        jobQueue.registerJob(parameters: SleepJobParameters.self) { parameters, _ in
            expectation.trigger()
            try await Task.sleep(for: parameters.length)
        }
        try await withThrowingTaskGroup(of: Void.self) { group in
            let serviceGroup = ServiceGroup(
                configuration: .init(
                    services: [jobQueue.processor(options: .init(numWorkers: 4))],
                    gracefulShutdownSignals: [.sigterm, .sigint],
                    logger: Logger(label: "JobQueueService")
                )
            )
            group.addTask {
                try await serviceGroup.run()
            }
            try await jobQueue.push(SleepJobParameters(length: .milliseconds(1000)))
            try await expectation.wait()
            group.cancelAll()
        }

        #expect(cancelledJobCount.load(ordering: .relaxed) == 1)
    }

    /// test job fails to decode but queue continues to process
    @Test func testFailToDecode() async throws {
        struct TestIntParameter: JobParameters {
            static let jobName = "testFailToDecode"
            let value: Int
        }
        struct TestStringParameter: JobParameters {
            static let jobName = "testFailToDecode"
            let value: String
        }
        let string: Mutex<String> = .init("")
        let expectation = TestExpectation()

        var logger = Logger(label: "JobsTests")
        logger.logLevel = .debug
        let jobQueue = JobQueue(.memory, logger: logger)
        jobQueue.registerJob(parameters: TestStringParameter.self) { parameters, _ in
            string.withLock { $0 = parameters.value }
            expectation.trigger()
        }
        try await testJobQueue(jobQueue.processor()) {
            try await jobQueue.push(TestIntParameter(value: 2))
            try await jobQueue.push(TestStringParameter(value: "test"))
            try await expectation.wait()
        }
        string.withLock {
            #expect($0 == "test")
        }
    }

    @Test func testMultipleJobQueueHandlers() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testMultipleJobQueueHandlers"
            let value: Int
        }
        let expectation = TestExpectation()
        let job = JobDefinition(parameters: TestParameters.self) { parameters, context in
            context.logger.info("Parameters=\(parameters)")
            try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
            expectation.trigger()
        }
        let logger = {
            var logger = Logger(label: "JobsTests")
            logger.logLevel = .debug
            return logger
        }()
        let jobQueue = JobQueue(.memory, logger: Logger(label: "JobsTests"))
        jobQueue.registerJob(job)
        let jobQueue2 = JobQueue(.memory, logger: Logger(label: "JobsTests"))
        jobQueue2.registerJob(job)

        try await withThrowingTaskGroup(of: Void.self) { group in
            let serviceGroup = ServiceGroup(
                configuration: .init(
                    services: [jobQueue.processor(options: .init(numWorkers: 2)), jobQueue2.processor(options: .init(numWorkers: 1))],
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
                try await expectation.wait(count: 200)
                await serviceGroup.triggerGracefulShutdown()
            } catch {
                Issue.record("\(String(reflecting: error))")
                await serviceGroup.triggerGracefulShutdown()
                throw error
            }
        }
    }

    /// Test we can user `any JobQueueProtocol` to register and push jobs
    @Test func testJobQueueProtocol() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testJobQueueProtocol"
            let value: Int
        }
        let expectation = TestExpectation()
        let jobQueue: any JobQueueProtocol = JobQueue(.memory, logger: Logger(label: "JobsTests"))
        let job = JobDefinition { (parameters: TestParameters, context) in
            context.logger.info("Parameters=\(parameters.value)")
            try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
            expectation.trigger()
        }
        jobQueue.registerJob(job)
        try await testJobQueue(jobQueue.processor(options: .init())) {
            try await jobQueue.push(TestParameters(value: 1))

            try await expectation.wait()
        }
    }

    @Test func testJobTimeout() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testJobTimeout"
        }
        let expectation = TestExpectation()
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let jobQueue = JobQueue(
            MemoryQueue { _, error in
                #expect((error as? JobQueueError)?.code == .jobTimedOut)
                expectation.trigger()
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
        try await testJobQueue(jobQueue.processor()) {
            try await jobQueue.push(TestParameters())

            try await expectation.wait()
        }
    }

    @Test func testJobWithTimeoutThatSucceeded() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testJobWithoutTimeout"
            let value: Int
        }
        let expectation = TestExpectation()
        let failedJobCount = ManagedAtomic(0)
        let jobQueue = JobQueue(
            MemoryQueue { _, _ in failedJobCount.wrappingIncrement(by: 1, ordering: .relaxed) },
            logger: Logger(label: "JobsTests")
        )
        let job = JobDefinition(timeout: .seconds(1)) { (parameters: TestParameters, context) in
            context.logger.info("Parameters=\(parameters.value)")
            try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
            expectation.trigger()
        }
        jobQueue.registerJob(job)
        try await testJobQueue(jobQueue.processor()) {
            try await jobQueue.push(TestParameters(value: 1))

            try await expectation.wait(count: 1)
        }
        #expect(failedJobCount.load(ordering: .relaxed) == 0)
    }

    // verify advance by gives us at least millisecond accuracy across 30000 years
    @Test func testAdvancedBy() {
        for _ in 0..<100 {
            let date = Date(timeIntervalSinceReferenceDate: TimeInterval.random(in: 0..<1_000_000_000_000))
            let offset = Double.random(in: 0..<1_000_000_000)
            let duration = Duration.seconds(offset)
            let newDate = date._advanced(by: duration)
            #expect(abs(date.advanced(by: offset).timeIntervalSinceReferenceDate - newDate.timeIntervalSinceReferenceDate) < 0.001)
        }
    }

    @Test func testCancelledJob() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testCancelledJob"
            let value: Int
        }
        let expectation = TestExpectation()
        let jobProcessed: Mutex<[Int]> = .init([])
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
            jobProcessed.withLock {
                $0.append(parameters.value)
            }
            expectation.trigger()
        }
        try await withThrowingTaskGroup(of: Void.self) { group in
            let serviceGroup = ServiceGroup(
                configuration: .init(
                    services: [jobQueue.processor()],
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

            try await expectation.wait()
            await serviceGroup.triggerGracefulShutdown()
        }
        #expect(jobProcessed.withLock { $0 } == [15])
    }

    @Test func testPausedThenResume() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testPausedAndThenResume"
            let value: Int
        }
        let expectation = TestExpectation()
        let jobRunSequence: Mutex<[Int]> = .init([])
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
            jobRunSequence.withLock {
                $0.append(parameters.value)
            }
            expectation.trigger()
        }

        try await withThrowingTaskGroup(of: Void.self) { group in
            let serviceGroup = ServiceGroup(
                configuration: .init(
                    services: [jobQueue.processor()],
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
            try await expectation.wait(count: 2)
            await serviceGroup.triggerGracefulShutdown()
        }
        #expect(jobRunSequence.withLock { $0 } == [30, 15])
    }

    @Test func testJobQueueGracefulShutdownWaitsForJob() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testJobTimeout"
        }
        let expectationStarted = TestExpectation()
        let expectationEnded = TestExpectation()
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let jobQueue = JobQueue(
            .memory,
            logger: logger
        )
        jobQueue.registerJob(
            parameters: TestParameters.self,
            retryStrategy: .dontRetry
        ) { _, _ in
            expectationStarted.trigger()
            try await Task.sleep(for: .milliseconds(100))
            expectationEnded.trigger()
        }
        try await testJobQueue(jobQueue.processor(options: .init(gracefulShutdownTimeout: .seconds(10000)))) {
            _ = try await jobQueue.push(TestParameters())
            try await expectationStarted.wait()
        }
        try await expectationEnded.wait()
    }

    @Test func testJobQueueGracefulShutdownTimeout() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testJobTimeout"
        }
        let expectation = TestExpectation()
        let failedJobCount = ManagedAtomic(0)
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let jobQueue = JobQueue(
            MemoryQueue { _, error in
                #expect(error is CancellationError)
                failedJobCount.wrappingIncrement(by: 1, ordering: .relaxed)
            },
            logger: logger
        )
        jobQueue.registerJob(
            parameters: TestParameters.self,
            retryStrategy: .dontRetry
        ) { _, _ in
            expectation.trigger()
            try await Task.sleep(for: .seconds(10))
        }
        try await testJobQueue(jobQueue.processor(options: .init(gracefulShutdownTimeout: .milliseconds(50)))) {
            try await jobQueue.push(TestParameters())
            try await expectation.wait()
        }
        #expect(failedJobCount.load(ordering: .relaxed) == 1)
    }

    /// Test job rescue functionality where jobs orphaned by one worker are rescued by another worker
    ///
    /// This test validates the critical job rescue scenario:
    /// 1. Worker A picks up a job with a lease duration and starts processing
    /// 2. Worker A crashes/dies while processing (simulated by expired heartbeat)
    /// 3. Worker B starts up and detects the orphaned job
    /// 4. Worker B rescues the job and processes it successfully
    ///
    /// This ensures job reliability in distributed systems where workers can fail.
    @Test func testJobRescue() async throws {
        struct TestParameters: JobParameters, Equatable {
            static let jobName = "testJobRescue"
            let value: Int
        }

        let expectation = TestExpectation()
        let logger = Logger(label: "JobsTests")

        // Setup shared memory driver for multi-worker scenario
        let memoryDriver = MemoryQueue()
        let jobQueue = JobQueue(memoryDriver, logger: logger)

        let deadWorkerID = UUID()

        // Register job handler with lease duration to enable heartbeat functionality
        let jobDefinition = JobDefinition(
            name: JobName<TestParameters>("testJobRescue"),
            parameters: TestParameters.self,
            leaseDuration: .milliseconds(500)  // For heartbeats every 250ms during job execution
        ) { parameters, context in
            #expect(parameters.value == 42)
            context.logger.info("Job successfully rescued and executed", metadata: ["value": "\(parameters.value)"])
            expectation.trigger()
        }
        jobQueue.registerJob(jobDefinition)

        // Push a job with lease duration (required for orphan detection and recovery)
        let jobID = try await jobQueue.push(
            TestParameters(value: 42),
            options: MemoryQueue.JobOptions(leaseDuration: .seconds(2))  // For orphan detection timeout
        )

        // Simulate Worker A picking up the job and then dying/crashing
        try await memoryDriver.queue.simulateJobPickupAndWorkerDeath(jobID: jobID, workerID: deadWorkerID)

        // Now start a new worker processor that should rescue the orphaned job
        try await testJobQueue(jobQueue.processor()) {
            // The processor will automatically call recoverOrphanedJobs(currentWorkerID:) on startup
            // and should find the job orphaned by the dead worker

            // Verify the rescue was successful by waiting for job execution
            try await expectation.wait(count: 1, timeout: .seconds(5))
        }
    }

    /// Test worker restart scenario where jobs from dead workers are recovered by new workers
    ///
    /// This test validates the critical scenario where:
    /// 1. Worker A is processing jobs with fresh heartbeats
    /// 2. Worker A process restarts and gets a new worker ID
    /// 3. New worker starts up and should recover jobs from old Worker A even though heartbeats are fresh
    /// 4. Without proper currentWorkerID logic, these jobs would get stuck forever
    @Test func testJobRescueAfterWorkerRestart() async throws {
        struct TestParameters: JobParameters, Equatable {
            static let jobName = "testJobRescueAfterWorkerRestart"
            let value: Int
        }

        let expectation = TestExpectation()
        let logger = Logger(label: "JobsTests")

        let memoryDriver = MemoryQueue()
        let jobQueue = JobQueue(memoryDriver, logger: logger)

        let oldWorkerID = UUID()
        let newWorkerID = UUID()

        // Register job handler
        jobQueue.registerJob(parameters: TestParameters.self) { parameters, context in
            #expect(parameters.value == 123)
            context.logger.info("Job rescued after worker restart", metadata: ["value": "\(parameters.value)"])
            expectation.trigger()
        }

        // Push a job with lease duration
        let jobID = try await jobQueue.push(
            TestParameters(value: 123),
            options: MemoryQueue.JobOptions(leaseDuration: .seconds(10))
        )

        // Simulate job being picked up by old worker with FRESH heartbeat (not expired)
        try await memoryDriver.queue.simulateJobPickupWithFreshHeartbeat(jobID: jobID, workerID: oldWorkerID)

        // Now simulate worker restart - new worker process starts up with different ID
        try await testJobQueue(jobQueue.processor()) {
            // Set the new worker ID - different from old worker
            await memoryDriver.setWorkerID(newWorkerID)

            // Manually test the recovery with explicit worker ID
            let recoveredJobs = try await memoryDriver.queue.recoverOrphanedJobs(excludingWorkerID: newWorkerID)

            // Should recover the job even though heartbeat is fresh because worker ID is different
            #expect(recoveredJobs.count == 1)
            #expect(recoveredJobs.contains(jobID))

            // Wait for the rescued job to be processed
            try await expectation.wait(count: 1, timeout: .seconds(5))
        }
    }

    /// Test database restart scenario where same worker reconnects but loses in-memory job state
    ///
    /// This test validates the scenario where:
    /// 1. Worker is processing jobs
    /// 2. Database restarts but worker maintains same ID and reconnects quickly
    /// 3. Jobs still have fresh heartbeats and same worker ID
    /// 4. Worker has lost in-memory state of what jobs it was processing
    /// 5. Recovery should handle this based on lease expiration timeout
    @Test func testJobRescueAfterDatabaseRestart() async throws {
        struct TestParameters: JobParameters, Equatable {
            static let jobName = "testJobRescueAfterDatabaseRestart"
            let value: Int
        }

        let expectation = TestExpectation()
        let logger = Logger(label: "JobsTests")

        let memoryDriver = MemoryQueue()
        let jobQueue = JobQueue(memoryDriver, logger: logger)

        let workerID = UUID()

        // Register job handler
        jobQueue.registerJob(parameters: TestParameters.self) { parameters, context in
            #expect(parameters.value == 456)
            context.logger.info("Job recovered after database restart", metadata: ["value": "\(parameters.value)"])
            expectation.trigger()
        }

        // Push a job with SHORT lease duration to test expiration-based recovery
        let jobID = try await jobQueue.push(
            TestParameters(value: 456),
            options: MemoryQueue.JobOptions(leaseDuration: .milliseconds(100))  // Very short lease
        )

        // Simulate job being picked up by worker, then database restart
        // Worker maintains same ID but loses in-memory state
        try await memoryDriver.queue.simulateJobPickupWithExpiredLease(jobID: jobID, workerID: workerID)

        // Worker reconnects with same ID after database restart
        try await testJobQueue(jobQueue.processor()) {
            await memoryDriver.setWorkerID(workerID)  // Same worker ID

            // Recovery should find job with expired lease even though worker ID matches
            let recoveredJobs = try await memoryDriver.queue.recoverOrphanedJobs(excludingWorkerID: workerID)

            // Should recover based on expired lease, not worker ID difference
            #expect(recoveredJobs.count == 1)
            #expect(recoveredJobs.contains(jobID))

            // Wait for the recovered job to be processed
            try await expectation.wait(count: 1, timeout: .seconds(5))
        }
    }
}

extension MemoryQueue.Internal {
    /// Test helper to simulate a worker picking up a job and then dying/crashing
    ///
    /// This simulates the complete lifecycle of a job being orphaned:
    /// 1. Job is in the queue waiting to be processed
    /// 2. Worker picks up the job (moves from queue to pending/processing state)
    /// 3. Worker dies/crashes (heartbeat expires)
    /// 4. Job remains in processing state with expired heartbeat, making it orphaned
    func simulateJobPickupAndWorkerDeath(jobID: MemoryQueue.JobID, workerID: UUID) throws {
        // Find the job in the queue
        guard let jobIndex = self.queue.firstIndex(where: { $0.job.id == jobID }) else {
            throw TestError.jobNotFound
        }

        let jobRequest = self.queue.remove(at: jobIndex)

        // Simulate job pickup: move to pending/processing state with expired heartbeat
        self.pendingJobs[jobID] = jobRequest.job.jobBuffer
        self.processingJobs[jobID] = JobStatus(
            workerID: workerID,
            lastHeartbeat: Date().addingTimeInterval(-5 * 60),  // 5 minutes ago (expired)
            leaseDuration: jobRequest.options.leaseDuration
        )
    }

    /// Test helper to simulate a job being picked up with an EXPIRED lease (database restart scenario)
    func simulateJobPickupWithExpiredLease(jobID: MemoryQueue.JobID, workerID: UUID) throws {
        // Find the job in the queue
        guard let jobIndex = self.queue.firstIndex(where: { $0.job.id == jobID }) else {
            throw TestError.jobNotFound
        }

        let jobRequest = self.queue.remove(at: jobIndex)

        // Simulate job pickup with expired lease - worker lost in-memory state after DB restart
        // Heartbeat is recent but lease has conceptually expired due to lost processing state
        self.pendingJobs[jobID] = jobRequest.job.jobBuffer
        self.processingJobs[jobID] = JobStatus(
            workerID: workerID,
            lastHeartbeat: Date().addingTimeInterval(-1),  // Recent but past the short lease duration
            leaseDuration: jobRequest.options.leaseDuration
        )
    }

    /// Test helper to simulate a job being picked up with a FRESH heartbeat (worker restart scenario)
    func simulateJobPickupWithFreshHeartbeat(jobID: MemoryQueue.JobID, workerID: UUID) throws {
        // Find the job in the queue
        guard let jobIndex = self.queue.firstIndex(where: { $0.job.id == jobID }) else {
            throw TestError.jobNotFound
        }

        let jobRequest = self.queue.remove(at: jobIndex)

        // Simulate job pickup with FRESH heartbeat (not expired) - this is the key difference
        // In a worker restart scenario, old worker jobs may still have recent heartbeats
        self.pendingJobs[jobID] = jobRequest.job.jobBuffer
        self.processingJobs[jobID] = JobStatus(
            workerID: workerID,
            lastHeartbeat: Date(),  // Fresh heartbeat - NOT expired
            leaseDuration: jobRequest.options.leaseDuration
        )
    }

    enum TestError: Error {
        case jobNotFound
    }
}

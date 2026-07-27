//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

#if compiler(>=6.2.3)

@_spi(JobsAPI) import Jobs
import Logging
import Testing

struct JobsAPITests {
    @Test func testPendingProcessingStatus() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testPendingProcessingStatus"
            let value: Int
        }
        let logger = Logger(label: "JobsTests")
        let jobQueue = JobQueue(.memory, logger: logger)
        let (jobStartStream, jobStartCont) = AsyncStream.makeStream(of: Void.self)
        let (jobDoneStream, jobDoneCont) = AsyncStream.makeStream(of: Void.self)
        let job = JobDefinition { (parameters: TestParameters, context) in
            context.logger.info("Parameters=\(parameters.value)")
            jobStartCont.yield()
            await jobDoneStream.first { _ in true }
        }
        jobQueue.registerJob(job)
        let id = try await jobQueue.push(TestParameters(value: 1))
        let id2 = try await jobQueue.push(TestParameters(value: 1))

        // we've pushed two jobs. We should have two pending jobs
        let jobs = try await jobQueue.queue.getJobs(maxNumber: .max, paginationToken: nil)
        #expect(jobs.jobs.count == 2)
        #expect(jobs.jobs[0].status == .pending)
        #expect(jobs.jobs[0].name == "testPendingProcessingStatus")
        #expect(jobs.jobs[0].id == id2)
        #expect(jobs.jobs[1].status == .pending)
        #expect(jobs.jobs[1].name == "testPendingProcessingStatus")
        #expect(jobs.jobs[1].id == id)

        try await testJobQueue(JobQueueProcessor(queue: jobQueue, logger: logger, options: .init(numWorkers: 1))) {
            await jobStartStream.first { _ in true }

            // Now the queue is being processed and we have waiting for one job to start we should have one
            // job pending and one job processing
            let jobs = try await jobQueue.queue.getJobs(maxNumber: .max, paginationToken: nil)
            #expect(jobs.jobs.count == 2)
            #expect(jobs.jobs[0].status == .pending)
            #expect(jobs.jobs[0].name == "testPendingProcessingStatus")
            #expect(jobs.jobs[0].id == id2)
            #expect(jobs.jobs[1].status == .processing)
            #expect(jobs.jobs[1].name == "testPendingProcessingStatus")
            #expect(jobs.jobs[1].id == id)

            jobDoneCont.yield()

            await jobStartStream.first { _ in true }

            // We wait for another job to start the first job is no longer available as the memory queue doesn't
            // store completed jobs, but the second job is processing
            let jobs2 = try await jobQueue.queue.getJobs(maxNumber: .max, paginationToken: nil)
            #expect(jobs2.jobs.count == 1)
            #expect(jobs2.jobs[0].id == id2)
            #expect(jobs2.jobs[0].status == .processing)

            jobDoneCont.yield()
        }
    }

    @Test func testPausedStatus() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testPausedStatus"
            let value: Int
        }
        let logger = Logger(label: "JobsTests")
        let jobQueue = JobQueue(.memory, logger: logger)
        let job = JobDefinition { (parameters: TestParameters, context) in
            context.logger.info("Parameters=\(parameters.value)")
        }
        jobQueue.registerJob(job)
        let id = try await jobQueue.push(TestParameters(value: 1))
        try await jobQueue.pauseJob(jobID: id)

        // we've pushed two jobs. We should have two pending jobs
        let jobs = try await jobQueue.queue.getJobs(maxNumber: .max, paginationToken: nil)
        #expect(jobs.jobs.count == 1)
        #expect(jobs.jobs[0].status == .paused)
        #expect(jobs.jobs[0].name == "testPausedStatus")
        #expect(jobs.jobs[0].id == id)
    }

    @Test func testFailedStatus() async throws {
        struct TestError: Error {}
        struct TestParameters: JobParameters {
            static let jobName = "testFailedStatus"
            let fail: Bool
        }
        let logger = Logger(label: "JobsTests")
        let jobQueue = JobQueue(.memory, logger: logger)
        let (jobStartStream, jobStartCont) = AsyncStream.makeStream(of: Void.self)
        let (jobDoneStream, jobDoneCont) = AsyncStream.makeStream(of: Void.self)
        let job = JobDefinition { (parameters: TestParameters, context) in
            jobStartCont.yield()
            if parameters.fail {
                await jobDoneStream.first { _ in true }
                throw TestError()
            }
            await jobDoneStream.first { _ in true }
        }
        jobQueue.registerJob(job)
        let id = try await jobQueue.push(TestParameters(fail: true))
        let id2 = try await jobQueue.push(TestParameters(fail: false))

        // we've pushed two jobs. We should have two pending jobs
        let jobs = try await jobQueue.queue.getJobs(maxNumber: .max, paginationToken: nil)
        #expect(jobs.jobs.count == 2)
        #expect(jobs.jobs[0].status == .pending)
        #expect(jobs.jobs[0].name == "testFailedStatus")
        #expect(jobs.jobs[0].id == id2)
        #expect(jobs.jobs[1].status == .pending)
        #expect(jobs.jobs[1].name == "testFailedStatus")
        #expect(jobs.jobs[1].id == id)

        try await testJobQueue(JobQueueProcessor(queue: jobQueue, logger: logger, options: .init(numWorkers: 1))) {
            // trigger job, have it finish and trigger next job
            await jobStartStream.first { _ in true }
            jobDoneCont.yield()
            await jobStartStream.first { _ in true }

            // We wait for another job to start the first job is no longer available as the memory queue doesn't
            // store completed jobs, but the second job is processing
            let jobs2 = try await jobQueue.queue.getJobs(maxNumber: .max, paginationToken: nil)
            #expect(jobs2.jobs.count == 2)
            #expect(jobs2.jobs[1].id == id)
            #expect(jobs2.jobs[1].status == .failed)

            jobDoneCont.yield()
        }
    }

    @Test func testPendingPagination() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testPendingPagination"
            let value: Int
        }
        let logger = Logger(label: "JobsTests")
        let jobQueue = JobQueue(.memory, logger: logger)
        let job = JobDefinition { (parameters: TestParameters, context) in
            context.logger.info("Parameters=\(parameters.value)")
        }
        jobQueue.registerJob(job)
        var ids: [MemoryQueue.JobID] = []
        for i in 0..<10 {
            try await ids.append(jobQueue.push(TestParameters(value: i)))
        }

        // we've pushed ten jobs. We should have ten pending jobs across two call to getJobs
        var response = try await jobQueue.queue.getJobs(maxNumber: 6, paginationToken: nil)
        #expect(response.jobs.count == 6)
        for job in response.jobs {
            #expect(job.status == .pending)
            #expect(job.name == "testPendingPagination")
        }
        response = try await jobQueue.queue.getJobs(maxNumber: 6, paginationToken: response.paginationToken)
        #expect(response.jobs.count == 4)
        for job in response.jobs {
            #expect(job.status == .pending)
            #expect(job.name == "testPendingPagination")
        }
    }

    @Test func testPagination() async throws {
        struct TestError: Error {}
        struct TestParameters: JobParameters {
            static let jobName = "testPagination"
            let value: Int
        }
        let logger = Logger(label: "JobsTests")
        let jobQueue = JobQueue(.memory, logger: logger)
        let (jobStartStream, jobStartCont) = AsyncStream.makeStream(of: Void.self)
        let (jobDoneStream, jobDoneCont) = AsyncStream.makeStream(of: Void.self)
        let job = JobDefinition { (parameters: TestParameters, context) in
            context.logger.info("Parameters=\(parameters.value)")
            jobStartCont.yield()
            await jobDoneStream.first { _ in true }
            throw TestError()
        }
        jobQueue.registerJob(job)
        var ids: [MemoryQueue.JobID] = []
        for i in 0..<10 {
            try await ids.append(jobQueue.push(TestParameters(value: i)))
        }

        try await testJobQueue(JobQueueProcessor(queue: jobQueue, logger: logger, options: .init(numWorkers: 1))) {
            await jobStartStream.first { _ in true }
            jobDoneCont.yield()

            await jobStartStream.first { _ in true }

            // we've pushed ten jobs. One has failed and one is in progress. We should have ten pending jobs across
            // two call to getJobs. And the last job should be failed and the second last processing
            var response = try await jobQueue.queue.getJobs(maxNumber: 6, paginationToken: nil)
            #expect(response.jobs.count == 6)
            for job in response.jobs {
                #expect(job.status == .pending)
                #expect(job.name == "testPagination")
            }
            response = try await jobQueue.queue.getJobs(maxNumber: 6, paginationToken: response.paginationToken)
            #expect(response.jobs.count == 4)
            for job in response.jobs.dropLast(2) {
                #expect(job.status == .pending)
                #expect(job.name == "testPagination")
            }
            #expect(response.jobs[2].status == .processing)
            #expect(response.jobs[3].status == .failed)
            jobDoneCont.yield()
            for _ in 0..<8 {
                jobDoneCont.yield()
            }
        }
    }

    @Test func testCreatedAt() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testCreatedAt"
            let value: Int
        }
        let logger = Logger(label: "JobsTests")
        let jobQueue = JobQueue(.memory, logger: logger)
        let job = JobDefinition { (parameters: TestParameters, context) in
            context.logger.info("Parameters=\(parameters.value)")
        }
        jobQueue.registerJob(job)
        let id = try await jobQueue.push(TestParameters(value: 1))

        // we've pushed two jobs. We should have two pending jobs
        let jobs = try await jobQueue.queue.getJobs(maxNumber: .max, paginationToken: nil)
        #expect(jobs.jobs.count == 1)
        #expect(jobs.jobs[0].status == .pending)
        #expect(abs(jobs.jobs[0].createdAt.timeIntervalSinceNow) < 10)
        #expect(jobs.jobs[0].id == id)
    }
}

#endif

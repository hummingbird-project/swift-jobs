//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2024 the Hummingbird authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See hummingbird/CONTRIBUTORS.txt for the list of Hummingbird authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

@testable import Jobs
import Logging
import ServiceLifecycle
import XCTest

final class JobSchedulerTests: XCTestCase {
    func testSchedule(start: String, expectedEnd: String, schedule: Schedule) throws {
        let startDate = try Date.ISO8601FormatStyle.iso8601(timeZone: schedule.calendar.timeZone).parse(start)
        let expectedEndDate = try Date.ISO8601FormatStyle.iso8601(timeZone: schedule.calendar.timeZone).parse(expectedEnd)
        let end = schedule.nextDate(after: startDate)
        XCTAssertEqual(expectedEndDate, end)
    }

    func testMinuteSchedule() throws {
        try self.testSchedule(start: "2021-06-21T21:10:15", expectedEnd: "2021-06-21T21:10:43", schedule: .everyMinute(second: 43))
        try self.testSchedule(start: "1999-12-31T23:59:25", expectedEnd: "2000-01-01T00:00:15", schedule: .everyMinute(second: 15))
    }

    func testHourlySchedule() throws {
        try self.testSchedule(start: "2021-06-21T21:10:15", expectedEnd: "2021-06-21T21:58:00", schedule: .hourly(minute: 58))
        try self.testSchedule(start: "1999-12-31T23:59:25", expectedEnd: "2000-01-01T00:01:00", schedule: .hourly(minute: 1))
    }

    func testDailySchedule() throws {
        try self.testSchedule(start: "2021-06-21T21:10:15", expectedEnd: "2021-06-22T01:15:00", schedule: .daily(hour: 1, minute: 15, timeZone: .gmt))
        try self.testSchedule(start: "1999-12-31T23:59:25", expectedEnd: "2000-01-01T06:15:00", schedule: .daily(hour: 6, minute: 15))
        try self.testSchedule(start: "2024-02-28T23:59:25", expectedEnd: "2024-02-29T06:15:00", schedule: .daily(hour: 6, minute: 15))
    }

    func testWeeklySchedule() throws {
        try self.testSchedule(start: "2021-06-21T21:10:15", expectedEnd: "2021-06-27T04:00:00", schedule: .weekly(day: .sunday, hour: 4))
        try self.testSchedule(start: "2024-03-19T23:59:56", expectedEnd: "2024-03-24T04:00:00", schedule: .weekly(day: .sunday, hour: 4, timeZone: .gmt))
        try self.testSchedule(start: "2024-03-19T23:59:56", expectedEnd: "2024-03-24T04:00:00", schedule: .weekly(day: .sunday, hour: 4))
        try self.testSchedule(start: "1999-12-31T23:59:25", expectedEnd: "2000-01-01T08:00:00", schedule: .weekly(day: .saturday, hour: 8))
    }

    func testMonthlySchedule() throws {
        try self.testSchedule(start: "2021-06-21T21:10:15", expectedEnd: "2021-07-14T04:00:00", schedule: .monthly(date: 14, hour: 4))
        try self.testSchedule(start: "2024-03-19T23:59:56", expectedEnd: "2024-04-14T04:00:00", schedule: .monthly(date: 14, hour: 4))
        try self.testSchedule(start: "1999-12-31T23:59:25", expectedEnd: "2000-01-14T04:00:00", schedule: .monthly(date: 14, hour: 4))
    }

    func testScheduleTimeZone() throws {
        let startDate = try Date.ISO8601FormatStyle.iso8601(timeZone: .current).parse("2021-06-21T21:10:15")
        let scheduledDate = try XCTUnwrap(Schedule.daily(hour: 4, timeZone: .gmt).nextDate(after: startDate))
        let dateComponents = Calendar.current.dateComponents([.hour], from: scheduledDate)
        // check timezone difference is the same as the difference in the schedule
        XCTAssertEqual(TimeZone.current.secondsFromGMT(), (dateComponents.hour! - 4) * 3600)
    }

    func testJobSchedule() throws {
        struct Job1: JobParameters {
            static var jobName = "Job1"
        }
        struct Job2: JobParameters {
            static var jobName = "Job1"
        }
        var schedule = JobSchedule()
        try schedule.addJob(Job1(), schedule: .hourly(minute: 30))
        try schedule.addJob(Job2(), schedule: .daily(hour: 4))

        let startDate = try Date.ISO8601FormatStyle.iso8601(timeZone: .current).parse("2024-04-14T02:00:00")
        for jobIndex in schedule.scheduledJobs.startIndex..<schedule.scheduledJobs.endIndex {
            schedule.scheduledJobs[jobIndex].nextScheduledDate = schedule.scheduledJobs[jobIndex].schedule.nextDate(after: startDate)!
        }
        // first two jobs should be Job1
        var job = try XCTUnwrap(schedule.nextJob())
        for _ in 0..<2 {
            schedule.updateNextScheduledDate(jobIndex: job.offset)
            XCTAssert(job.element.jobParameters is Job1)
            job = try XCTUnwrap(schedule.nextJob())
        }
        // next job should be Job2
        schedule.updateNextScheduledDate(jobIndex: job.offset)
        XCTAssert(job.element.jobParameters is Job2)
        // next 24 jobs should be Job1
        for _ in 0..<24 {
            job = try XCTUnwrap(schedule.nextJob())
            schedule.updateNextScheduledDate(jobIndex: job.offset)
            XCTAssert(job.element.jobParameters is Job1)
        }
        // next job should be Job2
        job = try XCTUnwrap(schedule.nextJob())
        schedule.updateNextScheduledDate(jobIndex: job.offset)
        XCTAssert(job.element.jobParameters is Job2)
    }

    func testSchedulerService() async throws {
        let (stream, source) = AsyncStream.makeStream(of: Void.self)
        struct TriggerShutdownParameters: JobParameters {
            static let jobName = "TriggerShutdown"
        }

        var logger = Logger(label: "jobs")
        logger.logLevel = .debug

        let jobQueue = JobQueue(MemoryQueue(), logger: logger)
        jobQueue.registerJob(parameters: TriggerShutdownParameters.self) { _, _ in
            source.yield()
        }
        // create schedule that ensures a job will be run in the next second
        let dateComponents = Calendar.current.dateComponents([.hour, .minute, .second], from: Date.now + 1)
        var jobSchedule = JobSchedule()
        try jobSchedule.addJob(TriggerShutdownParameters(), schedule: .everyMinute(second: dateComponents.second!))

        await withThrowingTaskGroup(of: Void.self) { group in
            let serviceGroup = ServiceGroup(
                configuration: .init(
                    services: [jobQueue, jobSchedule.scheduler(on: jobQueue)],
                    logger: logger
                )
            )
            group.addTask {
                try await serviceGroup.run()
            }
            await stream.first { _ in true }
            await serviceGroup.triggerGracefulShutdown()
        }
    }
}

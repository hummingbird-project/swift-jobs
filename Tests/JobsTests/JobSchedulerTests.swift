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

import Foundation
import Logging
import ServiceLifecycle
import Testing

@testable import Jobs

struct JobSchedulerTests {
    func testSchedule(start: String, expectedEnd: String, schedule: Schedule) throws {
        var schedule = schedule
        let dateFormatter = DateFormatter()
        dateFormatter.locale = Locale(identifier: "en_US_POSIX")
        dateFormatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ss'Z'"
        dateFormatter.timeZone = schedule.calendar.timeZone
        guard let startDate = dateFormatter.date(from: start),
            let expectedEndDate = dateFormatter.date(from: expectedEnd)
        else {
            Issue.record("Failed to parse dates")
            return
        }
        let end = schedule.nextDate(after: startDate)
        #expect(expectedEndDate == end)
    }

    func testInitMutatingSchedule(start: String, expectedEnd: String, schedule: inout Schedule) throws -> Date {
        let dateFormatter = DateFormatter()
        dateFormatter.locale = Locale(identifier: "en_US_POSIX")
        dateFormatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ss'Z'"
        dateFormatter.timeZone = schedule.calendar.timeZone
        let startDate = try #require(dateFormatter.date(from: start))
        let expectedEndDate = try #require(dateFormatter.date(from: expectedEnd))
        let optionalEnd = schedule.setInitialNextDate(after: startDate)
        let end = try #require(optionalEnd)
        #expect(expectedEndDate == end)
        return end
    }

    func testMutatingSchedule(date: Date, expectedEnd: String, schedule: inout Schedule) throws -> Date {
        let dateFormatter = DateFormatter()
        dateFormatter.locale = Locale(identifier: "en_US_POSIX")
        dateFormatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ss'Z'"
        dateFormatter.timeZone = schedule.calendar.timeZone
        let expectedEndDate = try #require(dateFormatter.date(from: expectedEnd))
        let optionalEnd = schedule.nextDate(after: date)
        let end = try #require(optionalEnd)
        #expect(expectedEndDate == end)
        return end
    }

    @Test func testMinuteSchedule() throws {
        try self.testSchedule(start: "2021-06-21T21:10:15Z", expectedEnd: "2021-06-21T21:10:43Z", schedule: .everyMinute(second: 43))
        try self.testSchedule(start: "1999-12-31T23:59:25Z", expectedEnd: "2000-01-01T00:00:15Z", schedule: .everyMinute(second: 15))
    }

    @Test func testHourlySchedule() throws {
        try self.testSchedule(start: "2021-06-21T21:10:15Z", expectedEnd: "2021-06-21T21:58:00Z", schedule: .hourly(minute: 58))
        try self.testSchedule(start: "1999-12-31T23:59:25Z", expectedEnd: "2000-01-01T00:01:00Z", schedule: .hourly(minute: 1))
    }

    @Test func testDailySchedule() throws {
        try self.testSchedule(
            start: "2021-06-21T21:10:15Z",
            expectedEnd: "2021-06-22T01:15:00Z",
            schedule: .daily(hour: 1, minute: 15, timeZone: .init(secondsFromGMT: 0)!)
        )
        try self.testSchedule(start: "1999-12-31T23:59:25Z", expectedEnd: "2000-01-01T06:15:00Z", schedule: .daily(hour: 6, minute: 15))
        try self.testSchedule(start: "2024-02-28T23:59:25Z", expectedEnd: "2024-02-29T06:15:00Z", schedule: .daily(hour: 6, minute: 15))
    }

    @Test func testWeeklySchedule() throws {
        try self.testSchedule(start: "2021-06-21T21:10:15Z", expectedEnd: "2021-06-27T04:00:00Z", schedule: .weekly(day: .sunday, hour: 4))
        try self.testSchedule(
            start: "2024-03-19T23:59:56Z",
            expectedEnd: "2024-03-24T04:00:00Z",
            schedule: .weekly(day: .sunday, hour: 4, timeZone: .init(secondsFromGMT: 0)!)
        )
        try self.testSchedule(start: "2024-03-19T23:59:56Z", expectedEnd: "2024-03-24T04:00:00Z", schedule: .weekly(day: .sunday, hour: 4))
        try self.testSchedule(start: "1999-12-31T23:59:25Z", expectedEnd: "2000-01-01T08:00:00Z", schedule: .weekly(day: .saturday, hour: 8))
    }

    @Test func testMonthlySchedule() throws {
        try self.testSchedule(start: "2021-06-21T21:10:15Z", expectedEnd: "2021-07-14T04:00:00Z", schedule: .monthly(date: 14, hour: 4))
        try self.testSchedule(start: "2024-03-19T23:59:56Z", expectedEnd: "2024-04-14T04:00:00Z", schedule: .monthly(date: 14, hour: 4))
        try self.testSchedule(start: "1999-12-31T23:59:25Z", expectedEnd: "2000-01-14T04:00:00Z", schedule: .monthly(date: 14, hour: 4))
    }

    @Test func testMinutesSchedule() throws {
        var schedule = Schedule.onMinutes([0, 15, 30, 45], second: 0)
        let date = try self.testInitMutatingSchedule(start: "2021-06-21T21:10:16Z", expectedEnd: "2021-06-21T21:15:00Z", schedule: &schedule)
        _ = try self.testMutatingSchedule(date: date, expectedEnd: "2021-06-21T21:30:00Z", schedule: &schedule)
    }

    @Test func testMinutesScheduleWithOneValue() throws {
        var schedule = Schedule.onMinutes([0], second: 0)
        let date = try self.testInitMutatingSchedule(start: "2021-06-21T21:10:16Z", expectedEnd: "2021-06-21T22:00:00Z", schedule: &schedule)
        _ = try self.testMutatingSchedule(date: date, expectedEnd: "2021-06-21T23:00:00Z", schedule: &schedule)
    }

    @Test func testHoursSchedule() throws {
        var schedule = Schedule.onHours([8, 20], minute: 0)
        var date = try self.testInitMutatingSchedule(start: "2021-06-21T21:10:16Z", expectedEnd: "2021-06-22T08:00:00Z", schedule: &schedule)
        date = try self.testMutatingSchedule(date: date, expectedEnd: "2021-06-22T20:00:00Z", schedule: &schedule)
        _ = try self.testMutatingSchedule(date: date, expectedEnd: "2021-06-23T08:00:00Z", schedule: &schedule)
    }

    @Test func testHoursScheduleWithOneValue() throws {
        var schedule = Schedule.onHours([8], minute: 0)
        let date = try self.testInitMutatingSchedule(start: "2021-06-21T21:10:16Z", expectedEnd: "2021-06-22T08:00:00Z", schedule: &schedule)
        _ = try self.testMutatingSchedule(date: date, expectedEnd: "2021-06-23T08:00:00Z", schedule: &schedule)
    }

    @Test func testDaysSchedule() throws {
        var schedule = Schedule.onDays([.saturday, .sunday], hour: 4, minute: 0)
        var date = try self.testInitMutatingSchedule(start: "2021-06-21T21:10:16Z", expectedEnd: "2021-06-26T04:00:00Z", schedule: &schedule)
        date = try self.testMutatingSchedule(date: date, expectedEnd: "2021-06-27T04:00:00Z", schedule: &schedule)
        _ = try self.testMutatingSchedule(date: date, expectedEnd: "2021-07-03T04:00:00Z", schedule: &schedule)
    }

    @Test func testDaysScheduleWithOneValue() throws {
        var schedule = Schedule.onDays([.saturday], hour: 4, minute: 0)
        let date = try self.testInitMutatingSchedule(start: "2021-06-21T21:10:16Z", expectedEnd: "2021-06-26T04:00:00Z", schedule: &schedule)
        _ = try self.testMutatingSchedule(date: date, expectedEnd: "2021-07-03T04:00:00Z", schedule: &schedule)
    }

    @Test func testDatesSchedule() throws {
        var schedule = Schedule.onDates([1, 2, 24], hour: 4, minute: 0)
        var date = try self.testInitMutatingSchedule(start: "2021-06-21T21:10:16Z", expectedEnd: "2021-06-24T04:00:00Z", schedule: &schedule)
        date = try self.testMutatingSchedule(date: date, expectedEnd: "2021-07-01T04:00:00Z", schedule: &schedule)
        _ = try self.testMutatingSchedule(date: date, expectedEnd: "2021-07-02T04:00:00Z", schedule: &schedule)
    }

    @Test func testDatesScheduleWithOneValue() throws {
        var schedule = Schedule.onDates([1], hour: 4, minute: 0)
        let date = try self.testInitMutatingSchedule(start: "2021-06-21T21:10:16Z", expectedEnd: "2021-07-01T04:00:00Z", schedule: &schedule)
        _ = try self.testMutatingSchedule(date: date, expectedEnd: "2021-08-01T04:00:00Z", schedule: &schedule)
    }

    @Test func testMonthsSchedule() throws {
        var schedule = Schedule.onMonths([.january, .july], date: 2, hour: 4, minute: 0)
        var date = try self.testInitMutatingSchedule(start: "2021-06-21T21:10:16Z", expectedEnd: "2021-07-02T04:00:00Z", schedule: &schedule)
        date = try self.testMutatingSchedule(date: date, expectedEnd: "2022-01-02T04:00:00Z", schedule: &schedule)
        _ = try self.testMutatingSchedule(date: date, expectedEnd: "2022-07-02T04:00:00Z", schedule: &schedule)
    }

    @Test func testMonthsScheduleWithOneValue() throws {
        var schedule = Schedule.onMonths([.april], date: 2, hour: 4, minute: 0)
        let date = try self.testInitMutatingSchedule(start: "2021-06-21T21:10:16Z", expectedEnd: "2022-04-02T04:00:00Z", schedule: &schedule)
        _ = try self.testMutatingSchedule(date: date, expectedEnd: "2023-04-02T04:00:00Z", schedule: &schedule)
    }

    @Test func testScheduleWithNoValues() throws {
        var schedule = Schedule.onMonths([], date: 2, hour: 4, minute: 0)
        let date = try self.testInitMutatingSchedule(start: "2021-06-21T21:10:16Z", expectedEnd: "2021-07-02T04:00:00Z", schedule: &schedule)
        _ = try self.testMutatingSchedule(date: date, expectedEnd: "2021-08-02T04:00:00Z", schedule: &schedule)
    }

    @Test func testScheduleTimeZone() throws {
        let startDate = ISO8601DateFormatter().date(from: "2021-06-21T21:10:15Z")!
        var schedule = Schedule.daily(hour: 4, timeZone: .init(secondsFromGMT: 7200)!)
        let optionalScheduleDate = schedule.nextDate(after: startDate)
        let scheduledDate = try #require(optionalScheduleDate)
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = .init(secondsFromGMT: 0)!

        let dateComponents = calendar.dateComponents([.hour], from: scheduledDate)
        // check timezone difference is the same as the difference in the schedule
        #expect((dateComponents.hour! - 4) * 3600 == -7200)
    }

    @Test func testJobSchedule() throws {
        let jobName = JobName<String>("Job1")
        var schedule = JobSchedule()
        schedule.addJob(jobName, parameters: "Test job", schedule: .hourly(minute: 30), accuracy: .all)

        schedule.setInitialNextDate(after: .now, logger: Logger(label: "test"))

        let date = schedule[0].nextScheduledDate
        let minutes = Calendar.current.component(.minute, from: date)
        #expect(minutes == 30)
        let job = try #require(schedule.nextJob())
        #expect(job.element.jobName == "Job1")

        schedule.updateNextScheduledDate(jobIndex: 0)
        let date2 = schedule[0].nextScheduledDate
        let minutes2 = Calendar.current.component(.minute, from: date2)
        #expect(minutes2 == 30)
        let job2 = try #require(schedule.nextJob())
        #expect(job2.element.jobName == "Job1")
    }

    @Test func testJobScheduleWithTwoJobs() throws {
        struct Job1: JobParameters {
            static let jobName = "Job1"
        }
        struct Job2: JobParameters {
            static let jobName = "Job2"
        }
        var schedule = JobSchedule()
        schedule.addJob(Job1(), schedule: .hourly(minute: 30), accuracy: .all)
        schedule.addJob(Job2(), schedule: .daily(hour: 4), accuracy: .all)

        let dateFormatter = DateFormatter()
        dateFormatter.locale = Locale(identifier: "en_US_POSIX")
        dateFormatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ss'Z'"
        let startDate = dateFormatter.date(from: "2024-04-14T02:00:00Z")!
        schedule.setInitialNextDate(after: startDate, logger: Logger(label: "test"))

        // first two jobs should be Job1
        var job = try #require(schedule.nextJob())
        for _ in 0..<2 {
            schedule.updateNextScheduledDate(jobIndex: job.offset)
            #expect(job.element.jobName == "Job1")
            job = try #require(schedule.nextJob())
        }
        // next job should be Job2
        schedule.updateNextScheduledDate(jobIndex: job.offset)
        #expect(job.element.jobName == "Job2")
        // next 24 jobs should be Job1
        for _ in 0..<24 {
            job = try #require(schedule.nextJob())
            schedule.updateNextScheduledDate(jobIndex: job.offset)
            #expect(job.element.jobName == "Job1")
        }
        // next job should be Job2
        job = try #require(schedule.nextJob())
        schedule.updateNextScheduledDate(jobIndex: job.offset)
        #expect(job.element.jobName == "Job2")
    }

    @Test func testJobScheduledAtSameTimeSequence() async throws {
        struct Job1: JobParameters {
            static let jobName = "Job1"
        }
        struct Job2: JobParameters {
            static let jobName = "Job2"
        }
        var jobSchedule = JobSchedule([
            .init(job: Job1(), schedule: .everyMinute(second: 45)),
            .init(job: Job2(), schedule: .everyMinute(second: 45)),
        ])
        jobSchedule.setInitialNextDate(after: .now, logger: Logger(label: "test"))

        let job = try #require(jobSchedule.nextJob())
        jobSchedule.updateNextScheduledDate(jobIndex: job.offset)
        #expect(job.element.jobName == "Job1")
        let job2 = try #require(jobSchedule.nextJob())
        jobSchedule.updateNextScheduledDate(jobIndex: job2.offset)
        #expect(job2.element.jobName == "Job2")

    }

    @Test func testJobScheduleSequence() async throws {
        struct Job1: JobParameters {
            static let jobName = "Job1"
        }
        struct Job2: JobParameters {
            static let jobName = "Job2"
        }
        var logger = Logger(label: "JobSequence")
        logger.logLevel = .debug
        // create schedule that ensures a job will be run in the next second
        let dateComponents = Calendar.current.dateComponents([.hour, .minute, .second], from: Date.now + 1)
        let jobSchedule = JobSchedule([
            .init(job: Job1(), schedule: .everyMinute(second: dateComponents.second!)),
            .init(job: Job2(), schedule: .everyMinute(second: (dateComponents.second! + 1) % 60)),
        ])
        let sequence = JobSchedule.JobSequence(jobSchedule: jobSchedule, logger: logger)
        var jobIterator = sequence.makeIterator()
        let job = jobIterator.next()
        #expect(job?.element.jobName == "Job1")
        let job2 = jobIterator.next()
        #expect(job2?.element.jobName == "Job2")
    }

    func testScheduleAfterReadingLastData(
        schedule: Schedule,
        accuracy: JobSchedule.ScheduleAccuracy,
        lastScheduled: String,
        now: String,
        expected: [String],
        file: StaticString = #filePath,
        line: UInt = #line
    ) throws {
        struct TestParameters: JobParameters {
            static let jobName = "testScheduleAfterLastDate"
        }
        var jobSchedule = JobSchedule()
        jobSchedule.addJob(TestParameters(), schedule: schedule, accuracy: accuracy)

        let dateFormatter = DateFormatter()
        dateFormatter.locale = Locale(identifier: "en_US_POSIX")
        dateFormatter.timeZone = .init(secondsFromGMT: 0)
        dateFormatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ss'Z'"
        var logger = Logger(label: "jobs")
        logger.logLevel = .debug

        // test we return now as next scheduled date when initial date is a long time before
        do {
            let lastScheduledDate = dateFormatter.date(from: lastScheduled)!
            let nowDate = dateFormatter.date(from: now)!
            jobSchedule.setInitialNextDate(after: lastScheduledDate, now: nowDate, logger: logger)
            for dateString in expected {
                let date = dateFormatter.date(from: dateString)
                #expect(jobSchedule.elements[0].nextScheduledDate == date)
                jobSchedule.updateNextScheduledDate(jobIndex: 0)
            }
        }
    }

    // test we are getting the right dates after restarting scheduler
    @Test func testScheduleLastDateWithEveryHour() async throws {
        try testScheduleAfterReadingLastData(
            schedule: .hourly(minute: 44),
            accuracy: .latest,
            lastScheduled: "2023-04-14T02:17:00Z",
            now: "2024-04-14T02:29:00Z",
            expected: ["2024-04-14T02:29:00Z", "2024-04-14T02:44:00Z", "2024-04-14T03:44:00Z"]
        )
        try testScheduleAfterReadingLastData(
            schedule: .hourly(minute: 44),
            accuracy: .latest,
            lastScheduled: "2024-04-14T02:15:00Z",
            now: "2024-04-14T02:19:00Z",
            expected: ["2024-04-14T02:44:00Z", "2024-04-14T03:44:00Z", "2024-04-14T04:44:00Z"]
        )
    }

    // test we are getting the right dates after restarting scheduler
    @Test func testScheduleLastDateWithOnMinutes() async throws {
        try testScheduleAfterReadingLastData(
            schedule: .onMinutes([0, 10, 20, 30, 40, 50]),
            accuracy: .latest,
            lastScheduled: "2023-04-14T02:17:00Z",
            now: "2024-04-14T02:29:00Z",
            expected: ["2024-04-14T02:29:00Z", "2024-04-14T02:30:00Z", "2024-04-14T02:40:00Z"]
        )
        try testScheduleAfterReadingLastData(
            schedule: .onMinutes([0, 10, 20, 30, 40, 50]),
            accuracy: .latest,
            lastScheduled: "2024-04-14T02:15:00Z",
            now: "2024-04-14T02:19:00Z",
            expected: ["2024-04-14T02:20:00Z", "2024-04-14T02:30:00Z", "2024-04-14T02:40:00Z"]
        )
        try testScheduleAfterReadingLastData(
            schedule: .onMinutes([0, 10, 20, 30, 40, 50]),
            accuracy: .all,
            lastScheduled: "2024-04-13T02:15:00Z",
            now: "2024-04-14T02:19:00Z",
            expected: ["2024-04-13T02:20:00Z", "2024-04-13T02:30:00Z", "2024-04-13T02:40:00Z"]
        )
    }

    // test we are getting the right dates after restarting scheduler
    @Test func testScheduleLastDateWithOnDates() async throws {
        try testScheduleAfterReadingLastData(
            schedule: .onDates([4, 6, 8], hour: 8, timeZone: .init(secondsFromGMT: 0)!),
            accuracy: .latest,
            lastScheduled: "2023-04-14T02:17:00Z",
            now: "2024-04-14T02:29:00Z",
            expected: ["2024-04-14T02:29:00Z", "2024-05-04T08:00:00Z", "2024-05-06T08:00:00Z"]
        )
        try testScheduleAfterReadingLastData(
            schedule: .onDates([4, 6, 8], hour: 8, timeZone: .init(secondsFromGMT: 0)!),
            accuracy: .latest,
            lastScheduled: "2024-04-05T02:15:00Z",
            now: "2024-04-05T02:19:00Z",
            expected: ["2024-04-06T08:00:00Z", "2024-04-08T08:00:00Z", "2024-05-04T08:00:00Z"]
        )
    }

    // test we are getting the right dates after restarting scheduler
    @Test func testScheduleLastDateWithMultipleRanges() async throws {
        try testScheduleAfterReadingLastData(
            schedule: .crontab("0 3-5 4 * *", timeZone: .init(secondsFromGMT: 0)!),
            accuracy: .latest,
            lastScheduled: "2024-04-03T05:00:00Z",
            now: "2024-04-04T02:29:00Z",
            expected: ["2024-04-04T03:00:00Z", "2024-04-04T04:00:00Z", "2024-04-04T05:00:00Z", "2024-05-04T03:00:00Z"]
        )
        try testScheduleAfterReadingLastData(
            schedule: .crontab("0 3-5 4 * *", timeZone: .init(secondsFromGMT: 0)!),
            accuracy: .latest,
            lastScheduled: "2024-04-04T02:17:00Z",
            now: "2024-04-04T03:17:00Z",
            expected: ["2024-04-04T03:17:00Z", "2024-04-04T04:00:00Z", "2024-04-04T05:00:00Z", "2024-05-04T03:00:00Z"]
        )
        try testScheduleAfterReadingLastData(
            schedule: .crontab("0 3-5 4 * *", timeZone: .init(secondsFromGMT: 0)!),
            accuracy: .latest,
            lastScheduled: "2024-04-04T03:17:00Z",
            now: "2024-04-04T03:17:00Z",
            expected: ["2024-04-04T04:00:00Z", "2024-04-04T05:00:00Z", "2024-05-04T03:00:00Z"]
        )
        try testScheduleAfterReadingLastData(
            schedule: .crontab("0 3-5 4 * *", timeZone: .init(secondsFromGMT: 0)!),
            accuracy: .latest,
            lastScheduled: "2024-04-04T03:17:00Z",
            now: "2024-04-04T04:17:00Z",
            expected: ["2024-04-04T04:17:00Z", "2024-04-04T05:00:00Z", "2024-05-04T03:00:00Z"]
        )
        try testScheduleAfterReadingLastData(
            schedule: .crontab("0 3-5 3-4 * *", timeZone: .init(secondsFromGMT: 0)!),
            accuracy: .latest,
            lastScheduled: "2024-04-04T03:17:00Z",
            now: "2024-04-04T04:17:00Z",
            expected: ["2024-04-04T04:17:00Z", "2024-04-04T05:00:00Z", "2024-05-03T03:00:00Z"]
        )
    }

    // test we are getting the right dates after restarting scheduler
    @Test func testScheduleLastDateWithOutOfRangeDates() async throws {
        try testScheduleAfterReadingLastData(
            schedule: .crontab("0 10 27-30 * *", timeZone: .init(secondsFromGMT: 0)!),
            accuracy: .latest,
            lastScheduled: "2023-02-03T05:00:00Z",
            now: "2023-02-03T05:29:00Z",
            expected: ["2023-02-27T10:00:00Z", "2023-02-28T10:00:00Z", "2023-03-27T10:00:00Z", "2023-03-28T10:00:00Z"]
        )
        try testScheduleAfterReadingLastData(
            schedule: .crontab("0 10 27-30 * *", timeZone: .init(secondsFromGMT: 0)!),
            accuracy: .latest,
            lastScheduled: "2024-02-03T05:00:00Z",
            now: "2024-02-03T05:29:00Z",
            expected: ["2024-02-27T10:00:00Z", "2024-02-28T10:00:00Z", "2024-02-29T10:00:00Z", "2024-03-27T10:00:00Z"]
        )
    }

    // test we are getting the right dates from accuracy all
    @Test func testScheduleLastDateAccuracyAll() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testScheduleAfterLastDate"
        }
        let dateFormatter = DateFormatter()
        dateFormatter.locale = Locale(identifier: "en_US_POSIX")
        dateFormatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ss'Z'"
        var logger = Logger(label: "jobs")
        logger.logLevel = .debug
        var jobSchedule = JobSchedule()
        jobSchedule.addJob(TestParameters(), schedule: .onMinutes([0, 10, 20, 30, 40, 50]), accuracy: .all)

        // test we return now as next scheduled date when initial date is a long time before
        let initialDate = dateFormatter.date(from: "2023-04-14T02:00:17Z")!
        let nowDate = dateFormatter.date(from: "2024-04-14T02:00:29Z")!
        let expectedDate = dateFormatter.date(from: "2023-04-14T02:00:20Z")!
        let expectedDate2 = dateFormatter.date(from: "2023-04-14T02:00:30Z")!
        jobSchedule.setInitialNextDate(after: initialDate, now: nowDate, logger: logger)
        jobSchedule.elements[0].nextScheduledDate = expectedDate
        _ = jobSchedule.nextJob()
        jobSchedule.elements[0].nextScheduledDate = expectedDate2

    }

    @Test func testSchedulerService() async throws {
        let (stream, source) = AsyncStream.makeStream(of: Void.self)
        struct TriggerShutdownParameters: JobParameters {
            static let jobName = "TriggerShutdown"
        }

        var logger = Logger(label: "jobs")
        logger.logLevel = .debug

        let jobQueue = JobQueue(MemoryQueue(), logger: logger)
        jobQueue.registerJob(parameters: TriggerShutdownParameters.self) { _, context in
            #expect(context.nextScheduledAt != nil)
            #expect(context.nextScheduledAt! > context.queuedAt)

            source.yield()
        }
        // create schedule that ensures a job will be run in the next second
        let dateComponents = Calendar.current.dateComponents([.hour, .minute, .second], from: Date.now + 1)
        var jobSchedule = JobSchedule()
        jobSchedule.addJob(TriggerShutdownParameters(), schedule: .everyMinute(second: dateComponents.second!))

        await withThrowingTaskGroup(of: Void.self) { group in
            let serviceGroup = await ServiceGroup(
                configuration: .init(
                    services: [jobQueue.processor(), jobSchedule.scheduler(on: jobQueue)],
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

    @Test func testSchedulerLastDate() async throws {
        let (stream, source) = AsyncStream.makeStream(of: Void.self)
        struct TriggerShutdownParameters: JobParameters {
            static let jobName = "TriggerShutdown"
        }

        var logger = Logger(label: "jobs")
        logger.logLevel = .debug

        let jobQueue = JobQueue(MemoryQueue(), logger: logger)
        jobQueue.registerJob(parameters: TriggerShutdownParameters.self) { _, context in
            source.yield()
        }
        // create schedule that ensures a job should have been run 15 seconds ago
        let dateTriggered = Date.now - 15
        let dateComponents = Calendar.current.dateComponents([.hour, .minute, .second], from: dateTriggered)
        var jobSchedule = JobSchedule()
        jobSchedule.addJob(TriggerShutdownParameters(), schedule: .everyMinute(second: dateComponents.second!))

        // Set last date scheduled task ran as 2 seconds before scheduled job triggered
        try await jobQueue.queue.setMetadata(key: .jobScheduleLastDate(schedulerName: "test", jobName: "TriggerShutdown"), value: dateTriggered - 2)
        await withThrowingTaskGroup(of: Void.self) { group in
            let serviceGroup = await ServiceGroup(
                configuration: .init(
                    services: [jobQueue.processor(), jobSchedule.scheduler(on: jobQueue, named: "test")],
                    logger: logger
                )
            )
            group.addTask {
                try await serviceGroup.run()
            }
            await stream.first { _ in true }
            await serviceGroup.triggerGracefulShutdown()
        }
        let lastDate = try await jobQueue.queue.getMetadata(.jobScheduleLastDate(schedulerName: "test", jobName: "TriggerShutdown"))
        let lastDate2 = try #require(lastDate)
        #expect(abs(lastDate2.timeIntervalSince1970 - Date.now.timeIntervalSince1970) < 1.0)
    }

    @Test func testSchedulerLastDateAccuracyAll() async throws {
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
        // create schedule that ensures a job should have been run 15 seconds ago
        let dateTriggered = Date.now - 15
        let dateComponents = Calendar.current.dateComponents([.hour, .minute, .second], from: dateTriggered)
        var jobSchedule = JobSchedule()
        jobSchedule.addJob(TriggerShutdownParameters(), schedule: .everyMinute(second: dateComponents.second!), accuracy: .all)

        // Set last date scheduled task ran as 1 minute and 1 second before scheduled job triggered
        // so job triggers twice
        try await jobQueue.queue.setMetadata(
            key: .jobScheduleLastDate(schedulerName: "testLastDateAccuracy", jobName: "TriggerShutdown"),
            value: dateTriggered - 61
        )
        await withThrowingTaskGroup(of: Void.self) { group in
            let serviceGroup = await ServiceGroup(
                configuration: .init(
                    services: [jobQueue.processor(), jobSchedule.scheduler(on: jobQueue, named: "testLastDateAccuracy")],
                    logger: logger
                )
            )
            group.addTask {
                try await serviceGroup.run()
            }
            await stream.first { _ in true }
            await stream.first { _ in true }
            await serviceGroup.triggerGracefulShutdown()
        }
        let lastDate = try await jobQueue.queue.getMetadata(.jobScheduleLastDate(schedulerName: "testLastDateAccuracy", jobName: "TriggerShutdown"))
        let lastDate2 = try #require(lastDate)
        #expect(abs(lastDate2.timeIntervalSince1970 - dateTriggered.timeIntervalSince1970) < 1.0)
    }

    @Test func testMultipleSchedulers() async throws {
        let (stream, source) = AsyncStream.makeStream(of: Void.self)
        var logger = Logger(label: "jobs")
        logger.logLevel = .debug

        let jobQueue = JobQueue(MemoryQueue(), logger: logger)
        jobQueue.registerJob(name: "testMultipleSchedulers", parameters: String.self) { _, _ in
            source.yield()
        }
        // create schedule that ensures a job will be run in the next second
        let dateComponents = Calendar.current.dateComponents([.hour, .minute, .second], from: Date.now + 1)
        var jobSchedule = JobSchedule()
        jobSchedule.addJob("testMultipleSchedulers", parameters: "Hello", schedule: .everyMinute(second: dateComponents.second!))

        await withThrowingTaskGroup(of: Void.self) { group in
            let serviceGroup = await ServiceGroup(
                configuration: .init(
                    services: [
                        jobQueue.processor(),
                        jobSchedule.scheduler(on: jobQueue, options: .init(schedulerLock: .acquire(every: .seconds(30), for: .seconds(40)))),
                        jobSchedule.scheduler(on: jobQueue, options: .init(schedulerLock: .acquire(every: .seconds(30), for: .seconds(40)))),
                    ],
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

extension JobSchedule {
    mutating func setInitialNextDate(
        after date: Date,
        now: Date = .now,
        logger: Logger
    ) {
        for i in 0..<self.count {
            self[i].setInitialNextDate(after: date, now: now, logger: logger)
        }
    }
}

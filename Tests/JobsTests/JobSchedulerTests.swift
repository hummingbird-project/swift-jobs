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
}

//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2025 the Hummingbird authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See hummingbird/CONTRIBUTORS.txt for the list of Hummingbird authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import XCTest

@testable import Jobs

final class ScheduleTests: XCTestCase {
    func testCrontabNumbers() throws {
        XCTAssertEqual(
            try Schedule.crontab("5 12 20 4 *"),
            Schedule(minute: 5, hour: 12, date: 20, month: .init(.april), day: .any)
        )
    }
    func testCrontabDays() throws {
        XCTAssertEqual(
            try Schedule.crontab("0 12 * * 0"),
            Schedule(minute: 0, hour: 12, date: .any, month: .any, day: .init(.sunday))
        )
        XCTAssertEqual(
            try Schedule.crontab("0 12 * * 7"),
            Schedule(minute: 0, hour: 12, date: .any, month: .any, day: .init(.sunday))
        )
        XCTAssertEqual(
            try Schedule.crontab("0 12 * * 4"),
            Schedule(minute: 0, hour: 12, date: .any, month: .any, day: .init(.thursday))
        )
    }
    func testCrontabRanges() throws {
        XCTAssertEqual(
            try Schedule.crontab("5 12-14 20 4 *"),
            Schedule(minute: 5, hour: [12, 13, 14], date: 20, month: .init(.april), day: .any)
        )
        XCTAssertEqual(
            try Schedule.crontab("5 12 20 6-9 *"),
            Schedule(
                minute: 5,
                hour: 12,
                date: 20,
                month: [.june, .july, .august, .september],
                day: .any
            )
        )
        XCTAssertEqual(
            try Schedule.crontab("5 12 * * 0-2"),
            Schedule(minute: 5, hour: 12, date: .any, month: .any, day: [.sunday, .monday, .tuesday])
        )
        XCTAssertEqual(
            try Schedule.crontab("5 12 * * 6-7"),
            Schedule(minute: 5, hour: 12, date: .any, month: .any, day: [.saturday, .sunday])
        )
    }
    func testCrontabEvery() throws {
        XCTAssertEqual(
            try Schedule.crontab("5 */6 * * *"),
            Schedule(minute: 5, hour: [0, 6, 12, 18], date: .any, month: .any, day: .any)
        )
        XCTAssertEqual(
            try Schedule.crontab("5 */5 * * *"),
            Schedule(minute: 5, hour: [0, 5, 10, 15, 20], date: .any, month: .any, day: .any)
        )
        XCTAssertEqual(
            try Schedule.crontab("15 10 * * */2"),
            Schedule(minute: 15, hour: 10, date: .any, month: .any, day: [.sunday, .tuesday, .thursday, .saturday])
        )
        XCTAssertEqual(
            try Schedule.crontab("15 10 */6 * *"),
            Schedule(minute: 15, hour: 10, date: [1, 7, 13, 19, 25, 31], month: .any, day: .any)
        )
        XCTAssertEqual(
            try Schedule.crontab("15 10 1 */3 *"),
            Schedule(minute: 15, hour: 10, date: 1, month: [.january, .april, .july, .october], day: .any)
        )
    }
    func testCrontabEveryInRange() throws {
        XCTAssertEqual(
            try Schedule.crontab("5 3-15/6 * * *"),
            Schedule(minute: 5, hour: [3, 9, 15], date: .any, month: .any, day: .any)
        )
        XCTAssertEqual(
            try Schedule.crontab("15 10 * * 1-6/2"),
            Schedule(minute: 15, hour: 10, date: .any, month: .any, day: [.monday, .wednesday, .friday])
        )
        XCTAssertEqual(
            try Schedule.crontab("15 10 1 2-12/3 *"),
            Schedule(minute: 15, hour: 10, date: 1, month: [.february, .may, .august, .november], day: .any)
        )
    }
    func testCrontabSelection() throws {
        XCTAssertEqual(
            try Schedule.crontab("5 8,16 * * *"),
            Schedule(minute: 5, hour: [8, 16], date: .any, month: .any, day: .any)
        )
        XCTAssertEqual(
            try Schedule.crontab("5,10 6 * * *"),
            Schedule(minute: [5, 10], hour: 6, date: .any, month: .any, day: .any)
        )
        XCTAssertEqual(
            try Schedule.crontab("15 10 2 1,6,9 *"),
            Schedule(minute: 15, hour: 10, date: 2, month: [.january, .june, .september], day: .any)
        )
    }
    func testFixedSelections() throws {
        XCTAssertEqual(
            try Schedule.crontab("* 8 * * *"),
            Schedule(minute: .init(Array(0..<60)), hour: 8, date: .any, month: .any, day: .any)
        )
        XCTAssertEqual(
            try Schedule.crontab("* * * 6 *"),
            Schedule(
                minute: .init(0..<60),
                hour: .init(0..<24),
                date: .any,
                month: .init(.june),
                day: .init([.sunday, .monday, .tuesday, .wednesday, .thursday, .friday, .saturday])
            )
        )
    }
    func testDayNames() throws {
        XCTAssertEqual(
            try Schedule.crontab("0 8 * * mon"),
            Schedule(minute: 0, hour: 8, date: .any, month: .any, day: .init(.monday))
        )
        XCTAssertEqual(
            try Schedule.crontab("0 19 * 6 tue-sat"),
            Schedule(
                minute: 0,
                hour: 19,
                date: .any,
                month: .init(.june),
                day: .init([.tuesday, .wednesday, .thursday, .friday, .saturday])
            )
        )
        XCTAssertEqual(
            try Schedule.crontab("0 19 * 6 sun,wed,fri"),
            Schedule(
                minute: 0,
                hour: 19,
                date: .any,
                month: .init(.june),
                day: .init([.sunday, .wednesday, .friday])
            )
        )
        XCTAssertThrowsError(try Schedule.crontab("0 19 * 6 sun,jan,fri"))
    }
    func testMonthNames() throws {
        XCTAssertEqual(
            try Schedule.crontab("0 8 * jan *"),
            Schedule(minute: 0, hour: 8, date: .any, month: .init(.january), day: .any)
        )
        XCTAssertEqual(
            try Schedule.crontab("0 19 * feb-may *"),
            Schedule(
                minute: 0,
                hour: 19,
                date: .any,
                month: [.february, .march, .april, .may],
                day: .any
            )
        )
        XCTAssertEqual(
            try Schedule.crontab("0 19 * jun,jul,aug *"),
            Schedule(
                minute: 0,
                hour: 19,
                date: .any,
                month: [.june, .july, .august],
                day: .any
            )
        )
        XCTAssertThrowsError(try Schedule.crontab("0 19 * 6 jan-sat"))
    }
}

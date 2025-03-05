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

import RegexBuilder

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

extension Schedule {
    ///  Create Scehdule using crontab style string
    /// - Parameters:
    ///   - crontab: Crontab string
    ///   - timeZone: Timezone to run schedule in
    /// - Throws: ScheduleError for corrupt crontabs and crontabs we don't suppoty
    static func crontab(_ crontab: String, timeZone: TimeZone = .current) throws -> Self {
        if crontab.first == "@" {
            guard let crontab = Self.crontabNicknames[crontab] else { throw ScheduleError("Unrecognised crontab nick name \(crontab)") }
            return try Self.crontab(crontab, timeZone: timeZone)
        }
        let values = crontab.split(separator: " ", omittingEmptySubsequences: true)
        guard values.count == 5 else { throw ScheduleError("Crontab string requires 5 values") }
        let minutes = try Self.parse(values[0], range: 0...59, count: 60)
        let hours = try Self.parse(values[1], range: 0...23, count: 24)
        let dayOfMonth = try Self.parse(values[2], range: 1...31, count: 31)
        let month = try Self.parse(values[3], range: 1...12, count: 12) {
            if let month = Int($0) { return month }
            if let month = Self.monthNameNumberMap[$0] { return month }
            return nil
        }.map {
            guard let month = Month(rawValue: $0) else { throw ScheduleError("Invalid month value") }
            return month
        }
        let dayOfWeek = try Self.parse(values[4], range: 0...7, count: 7) {
            if let day = Int($0) { return day }
            if let day = Self.dayNameNumberMap[$0] { return day }
            return nil
        }.map {
            guard let day = Day(rawValue: $0 + 1) else {
                if $0 == 7 {
                    return Day.sunday
                }
                throw ScheduleError("Invalid day value")
            }
            return day
        }.sorted()

        let schedule = Self(second: .specific(0), minute: minutes, hour: hours, date: dayOfMonth, month: month, day: dayOfWeek, timeZone: timeZone)

        // if we have a selection set for either day or date we don't support setting the other value
        switch (schedule.day, schedule.date) {
        case (.selection, .specific), (.selection, .selection), (.specific, .selection):
            throw ScheduleError(
                "Schedule does not support a combination of date and weekday where one is a selection of values and the other is not the wildcard '*'"
            )
        default:
            break
        }
        return schedule
    }

    static func parse(
        _ entry: Substring,
        range: ClosedRange<Int>,
        count: Int,
        transform: (Substring) -> Int? = { Int($0) }
    ) throws -> Parameter<Int> {
        let numberRegex = Regex {
            Anchor.startOfLine
            Capture {
                OneOrMore(.word)
            }
            Anchor.endOfLine
        }
        let rangeRegex = Regex {
            Anchor.startOfLine
            Capture {
                OneOrMore(.word)
            }
            "-"
            Capture {
                OneOrMore(.word)
            }
            Anchor.endOfLine
        }
        let everyRegex = Regex {
            Anchor.startOfLine
            "*/"
            Capture {
                OneOrMore(.digit)
            } transform: {
                Int($0)!
            }
            Anchor.endOfLine
        }
        let everyInRangeRegex = Regex {
            Anchor.startOfLine
            Capture {
                OneOrMore(.word)
            }
            "-"
            Capture {
                OneOrMore(.word)
            }
            "/"
            Capture {
                OneOrMore(.digit)
            } transform: {
                Int($0)!
            }
            Anchor.endOfLine
        }
        let selectionRegex: Regex = Regex {
            Anchor.startOfLine
            Capture {
                OneOrMore {
                    OneOrMore(.word)
                    ","
                }
                OneOrMore(.word)
            }
            Anchor.endOfLine
        }
        if entry == "*" { return .any }
        if let values = try? numberRegex.wholeMatch(in: entry), let value = transform(values.1) {
            return .init(value)
        } else if let values = try? rangeRegex.wholeMatch(in: entry), let lower = transform(values.1), let upper = transform(values.2) {
            guard lower < upper else { throw ScheduleError("Crontab range requires first value to be before the second: \(values.0)") }
            return .init(lower...upper)
        } else if let values = try? everyRegex.wholeMatch(in: entry) {
            let numberOfValues = (count + values.1 - 1) / values.1
            let array = (0..<numberOfValues).map { range.lowerBound + $0 * values.1 }
            return .init(array)
        } else if let values = try? everyInRangeRegex.wholeMatch(in: entry), let lower = transform(values.1), let upper = transform(values.2) {
            guard lower < upper else { throw ScheduleError("Crontab range requires first value to be before the second: \(values.0)") }
            let range = lower...upper
            let numberOfValues = (range.count + values.3 - 1) / values.3
            let array = (0..<numberOfValues).map { range.lowerBound + $0 * values.3 }
            return .init(array)
        } else if let values = try? selectionRegex.wholeMatch(in: entry) {
            let values = try values.1.split(separator: ",").map {
                guard let value = transform($0) else { throw ScheduleError("Unrecognised values \($0)") }
                return value
            }
            return .init(values)
        } else {
            throw ScheduleError("Unrecognised crontab element \(entry)")
        }
    }

    private static let crontabNicknames: [String: String] = [
        "@yearly": "0 0 1 1 *",
        "@annually": "0 0 1 1 *",
        "@monthly": "0 0 1 * *",
        "@weekly": "0 0 * * 0",
        "@daily": "0 0 * * *",
        "@hourly": "0 * * * *",
    ]

    private static let dayNameNumberMap: [Substring: Int] = [
        "sun": 0,
        "mon": 1,
        "tue": 2,
        "wed": 3,
        "thu": 4,
        "fri": 5,
        "sat": 6,
    ]

    private static let monthNameNumberMap: [Substring: Int] = [
        "jan": 1,
        "feb": 2,
        "mar": 3,
        "apr": 4,
        "may": 5,
        "jun": 6,
        "jul": 7,
        "aug": 8,
        "sep": 9,
        "oct": 10,
        "nov": 11,
        "dec": 12,
    ]
}

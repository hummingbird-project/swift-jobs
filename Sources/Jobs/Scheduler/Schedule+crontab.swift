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

#if os(Linux)
@preconcurrency import Foundation
#else
import Foundation
#endif

extension Schedule {
    ///  Initialize Scehdule using crontab style string
    /// - Parameters:
    ///   - crontab: Crontab string
    ///   - timeZone: Timezone to run schedule in
    /// - Throws: ScheduleError for corrupt crontabs and crontabs we don't suppoty
    init(crontab: String, timeZone: TimeZone = .current) throws {
        let values = crontab.split(separator: " ", omittingEmptySubsequences: true)
        guard values.count == 5 else { throw ScheduleError("Crontab string requires 5 values") }
        let minutes = try Self.parse(values[0], range: 0...60) { $0 }
        let hours = try Self.parse(values[1], range: 0...24) { $0 }
        let date = try Self.parse(values[2], range: 0...31) { $0 }
        let month = try Self.parse(values[3], range: 1...12) {
            guard let month = Month(rawValue: $0) else { throw ScheduleError("Invalid month value") }
            return month
        }
        let day = try Self.parse(values[4], range: 0...6) {
            guard let day = Day(rawValue: $0 + 1) else {
                if $0 == 7 {
                    return Day.sunday
                }
                throw ScheduleError("Invalid day value")
            }
            return day
        }
        self.init(second: .specific(0), minute: minutes, hour: hours, date: date, month: month, day: day, timeZone: timeZone)

        // if we have a selection set for either day or date we don't support setting the other value
        switch (self.day, self.date) {
        case (.selection, .specific), (.selection, .selection), (.specific, .selection):
            throw ScheduleError(
                "Schedule does not support a combination of date and weekday where one is a selection of values and the other is not the wildcard '*'"
            )
        default:
            break
        }
    }

    static func parse<Value>(_ entry: Substring, range: ClosedRange<Int>, transform: (Int) throws -> Value) throws -> Parameter<Value> {
        let numberRegex = Regex {
            Anchor.startOfLine
            Capture {
                OneOrMore(.digit)
            } transform: {
                Int($0)!
            }
            Anchor.endOfLine
        }
        let rangeRegex = Regex {
            Anchor.startOfLine
            Capture {
                OneOrMore(.digit)
            } transform: {
                Int($0)!
            }
            "-"
            Capture {
                OneOrMore(.digit)
            } transform: {
                Int($0)!
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
        let selectionRegex: Regex = Regex {
            Anchor.startOfLine
            Capture {
                OneOrMore {
                    OneOrMore(.digit)
                    ","
                }
                OneOrMore(.digit)
            } transform: {
                $0.split(separator: ",").map { Int($0)! }
            }
            Anchor.endOfLine
        }
        if entry == "*" { return .any }
        if let values = try? numberRegex.wholeMatch(in: entry) {
            return try .init(transform(values.1))
        } else if let values = try? rangeRegex.wholeMatch(in: entry) {
            let array = try (values.1...values.2).map { try transform($0) }
            return .init(array)
        } else if let values = try? everyRegex.wholeMatch(in: entry) {
            let numberOfValues = (range.count + 1) / values.1
            let array = try (0..<numberOfValues).map { try transform($0 * values.1) }
            return .init(array)
        } else if let values = try? selectionRegex.wholeMatch(in: entry) {
            let array = try values.1.map { try transform($0) }
            return .init(array)
        } else {
            throw ScheduleError("Unrecognised crontab element \(entry)")
        }
    }
}

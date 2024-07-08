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

#if os(Linux)
@preconcurrency import Foundation
#else
import Foundation
#endif

/// Schedule
public struct Schedule: Sendable {
    /// Day of week
    public enum Day: Int, Sendable {
        case sunday = 1
        case monday = 2
        case tuesday = 3
        case wednesday = 4
        case thursday = 5
        case friday = 6
        case saturday = 7
    }

    /// Month of the year
    public enum Month: Int, Sendable {
        case january = 1
        case february = 2
        case march = 3
        case april = 4
        case may = 5
        case june = 6
        case july = 7
        case august = 8
        case september = 9
        case october = 10
        case november = 11
        case december = 12
    }

    /// Schedule parameter
    enum Parameter<Value: Sendable>: Sendable {
        case any
        case specific(Value)
    }

    let second: Parameter<Int>
    let minute: Parameter<Int>
    let hour: Parameter<Int>
    let date: Parameter<Int>
    let month: Parameter<Month>
    let day: Parameter<Day>
    let calendar: Calendar

    init(
        second: Parameter<Int> = .any,
        minute: Parameter<Int> = .any,
        hour: Parameter<Int> = .any,
        date: Parameter<Int> = .any,
        month: Parameter<Month> = .any,
        day: Parameter<Day> = .any,
        timeZone: TimeZone = .current
    ) {
        self.second = second
        self.minute = minute
        self.hour = hour
        self.date = date
        self.month = month
        self.day = day
        if timeZone == .current {
            self.calendar = .current
        } else {
            var calendar = Calendar(identifier: .gregorian)
            calendar.timeZone = timeZone
            self.calendar = calendar
        }
    }

    /// Return Schedule that returns a Date for every minute
    /// - Parameter second: Second value it should return a Date at
    public static func everyMinute(second: Int = 0) -> Self {
        .init(second: .specific(second))
    }

    ///  Return a schedule that returns a Date for every hour
    /// - Parameter minute: Minute value it should return the Date at
    public static func hourly(minute: Int = 0) -> Self {
        .init(minute: .specific(minute))
    }

    ///  Return a schedule that returns a Date once a day
    /// - Parameters:
    ///   - hour: Hour value it should return Date at
    ///   - minute: Minute value it should return Date at
    ///   - timeZone: Time zone to use when scheduling
    public static func daily(hour: Int = 0, minute: Int = 0, timeZone: TimeZone = .current) -> Self {
        .init(minute: .specific(minute), hour: .specific(hour), timeZone: timeZone)
    }

    ///  Return a schedule that returns a Date once a week
    /// - Parameters:
    ///   - day: Day on which it should return Date at
    ///   - hour: Hour value is should return Date at
    ///   - timeZone: Time zone to use when scheduling
    public static func weekly(day: Day, hour: Int = 0, timeZone: TimeZone = .current) -> Self {
        .init(hour: .specific(hour), day: .specific(day), timeZone: timeZone)
    }

    ///  Return a schedule that returns a Date once a month
    /// - Parameters:
    ///   - date: Date on which it should return Date at
    ///   - hour: Hour value is should return Date at
    ///   - timeZone: Time zone to use when scheduling
    public static func monthly(date: Int, hour: Int = 0, timeZone: TimeZone = .current) -> Self {
        .init(hour: .specific(hour), date: .specific(date), timeZone: timeZone)
    }

    ///  Return a schedule that returns a Date once a month
    /// - Parameters:
    ///   - month: Month on which it should return Date at
    ///   - date: Date on which it should return Date at
    ///   - hour: Hour value is should return Date at
    ///   - timeZone: Time zone to use when scheduling
    public static func yearly(month: Month, date: Int, hour: Int = 0, timeZone: TimeZone = .current) -> Self {
        .init(hour: .specific(hour), date: .specific(date), month: .specific(month), timeZone: timeZone)
    }

    ///  Return next date in schedule after the supplied Date
    /// - Parameter date: start date
    public func nextDate(after date: Date = .now) -> Date? {
        var dateComponents = DateComponents()
        dateComponents.nanosecond = 0
        if case .specific(let second) = self.second {
            dateComponents.second = second
        }
        if case .specific(let minute) = self.minute {
            dateComponents.minute = minute
        }
        if case .specific(let hour) = self.hour {
            dateComponents.hour = hour
        }
        if case .specific(let day) = self.day {
            dateComponents.weekday = day.rawValue
        }
        if case .specific(let date) = self.date {
            dateComponents.day = date
        }
        return self.calendar.nextDate(after: date, matching: dateComponents, matchingPolicy: .strict)
    }
}

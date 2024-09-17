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

import Collections
#if os(Linux)
@preconcurrency import Foundation
#else
import Foundation
#endif

/// Generates a Date at regular intervals (hourly, daily, weekly etc)
public struct Schedule: Sendable {
    /// Day of week
    public enum Day: Int, Sendable, Comparable {
        case sunday = 1
        case monday = 2
        case tuesday = 3
        case wednesday = 4
        case thursday = 5
        case friday = 6
        case saturday = 7

        public static func < (lhs: Schedule.Day, rhs: Schedule.Day) -> Bool {
            lhs.rawValue < rhs.rawValue
        }
    }

    /// Month of the year
    public enum Month: Int, Sendable, Comparable {
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

        public static func < (lhs: Schedule.Month, rhs: Schedule.Month) -> Bool {
            lhs.rawValue < rhs.rawValue
        }
    }

    /// Schedule parameter
    enum Parameter<Value: Sendable>: Sendable {
        case any
        case specific(Value)
        case selection(Deque<Value>)

        mutating func nextValue() -> Value? {
            switch self {
            case .specific(let value):
                return value
            case .selection(var values):
                let second = values.popFirst()
                if let second {
                    values.append(second)
                    self = .selection(values)
                }
                return second
            case .any:
                return nil
            }
        }
    }

    var second: Parameter<Int>
    var minute: Parameter<Int>
    var hour: Parameter<Int>
    var date: Parameter<Int>
    var month: Parameter<Month>
    var day: Parameter<Day>
    var calendar: Calendar

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

    /// Return Schedule that generates a Date for every minute
    /// - Parameter second: Second value it should return a Date at
    public static func everyMinute(second: Int = 0) -> Self {
        .init(second: .specific(second))
    }

    /// Return schedule that generates a Date for minutes, hours or seconds
    /// - Parameter minute: minute value e.g 5
    /// - Parameter hour: hour value e.g 3
    /// - Parameter timeZone defaults to GMT
    public static func every(minute: Int = 5, hour: Int = 0, timeZone: TimeZone = .gmt) -> Self {
        let maxNumber = if minute > 0, hour == 0 {
            60 / minute
        } else {
            24 / hour
        }

        var multiples: [Int] = []
        var index = 0
        let number = hour > 0 ? hour : minute
        repeat {
            let multiple = number * index

            if hour > 0, multiple > 24 {
                let remainder = multiple % 24
                multiples.append(remainder - hour)
            } else {
                multiples.append(multiple)
            }

            index = index + 1
        } while index < maxNumber

        if hour == 0 {
            return .onMinutes(multiples)
        }
        return .onHours(multiples, timeZone: timeZone)
    }

    /// Return Schedule that generates a Date for a selection of minutes
    /// - Parameters
    ///   - minutes: Array of minutes if should return Dates for
    ///   - second: Second value it should return a Date at
    public static func onMinutes(_ minutes: some Collection<Int>, second: Int = 0) -> Self {
        let parameter: Parameter = minutes.count != 1 ?
            .selection(Deque(minutes.sorted())) :
            .specific(minutes.first!)
        return .init(second: .specific(second), minute: parameter)
    }

    ///  Return a schedule that generates a Date for every hour
    /// - Parameter minute: Minute value it should return the Date at
    public static func hourly(minute: Int = 0) -> Self {
        .init(minute: .specific(minute))
    }

    /// Return Schedule that generates a Date for a selection of hours
    /// - Parameters:
    ///   - hours: Array of hours if should return Dates for
    ///   - minute: Minute value it should return a Date at
    ///   - timeZone: TimeZone to run schedule in
    public static func onHours(_ hours: some Collection<Int>, minute: Int = 0, timeZone: TimeZone = .current) -> Self {
        let parameter: Parameter = hours.count != 1 ?
            .selection(Deque(hours.sorted())) :
            .specific(hours.first!)
        return .init(minute: .specific(minute), hour: parameter, timeZone: timeZone)
    }

    ///  Return a schedule that generates a Date once a day
    /// - Parameters:
    ///   - hour: Hour value it should return Date at
    ///   - minute: Minute value it should return Date at
    ///   - timeZone: Time zone to use when scheduling
    public static func daily(hour: Int = 0, minute: Int = 0, timeZone: TimeZone = .current) -> Self {
        .init(minute: .specific(minute), hour: .specific(hour), timeZone: timeZone)
    }

    /// Return Schedule that generates a Date for a selection of days of the week
    /// - Parameters:
    ///   - days: Array of week days it should return Dates for
    ///   - hour: Hour it should return a Date at
    ///   - minute: Minute value it should return a Date at
    ///   - timeZone: TimeZone to run schedule in
    public static func onDays(_ days: some Collection<Day>, hour: Int = 0, minute: Int = 0, timeZone: TimeZone = .current) -> Self {
        let parameter: Parameter = days.count != 1 ?
            .selection(Deque(days.sorted())) :
            .specific(days.first!)
        return .init(minute: .specific(minute), hour: .specific(hour), day: parameter, timeZone: timeZone)
    }

    ///  Return a schedule that generates a Date once a week
    /// - Parameters:
    ///   - day: Week day on which it should return Date at
    ///   - hour: Hour value is should return Date at
    ///   - timeZone: Time zone to use when scheduling
    public static func weekly(day: Day, hour: Int = 0, minute: Int = 0, timeZone: TimeZone = .current) -> Self {
        .init(minute: .specific(minute), hour: .specific(hour), day: .specific(day), timeZone: timeZone)
    }

    /// Return Schedule that generates a Date for a selection of month dates
    /// - Parameters:
    ///   - dates: Array of dates of the month it should return Dates for
    ///   - hour: Hour it should return a Date at
    ///   - minute: Minute value it should return a Date at
    ///   - timeZone: TimeZone to run schedule in
    /// - Returns:
    public static func onDates(_ dates: some Collection<Int>, hour: Int = 0, minute: Int = 0, timeZone: TimeZone = .current) -> Self {
        let parameter: Parameter = dates.count != 1 ?
            .selection(Deque(dates.sorted())) :
            .specific(dates.first!)
        return .init(minute: .specific(minute), hour: .specific(hour), date: parameter, timeZone: timeZone)
    }

    ///  Return a schedule that generates a Date once a month
    /// - Parameters:
    ///   - date: Day of month on which it should return Date at
    ///   - hour: Hour value is should return Date at
    ///   - minute: Minute value it should return a Date at
    ///   - timeZone: Time zone to use when scheduling
    public static func monthly(date: Int, hour: Int = 0, minute: Int = 0, timeZone: TimeZone = .current) -> Self {
        .init(minute: .specific(minute), hour: .specific(hour), date: .specific(date), timeZone: timeZone)
    }

    /// Return Schedule that generates a Date for a selection of months
    /// - Parameters:
    ///   - months: Array of months it should return Dates for
    ///   - date: Date it should return a Date at
    ///   - hour: Hour it should return a Date at
    ///   - minute: Minute value it should return a Date at
    ///   - timeZone: TimeZone to run schedule in
    /// - Returns:
    public static func onMonths(_ months: some Collection<Month>, date: Int, hour: Int = 0, minute: Int = 0, timeZone: TimeZone = .current) -> Self {
        let parameter: Parameter = months.count != 1 ?
            .selection(Deque(months.sorted())) :
            .specific(months.first!)
        return .init(minute: .specific(minute), hour: .specific(hour), date: .specific(date), month: parameter, timeZone: timeZone)
    }

    ///  Return a schedule that generates a Date once a year
    /// - Parameters:
    ///   - month: Month on which it should return Date at
    ///   - date: Date on which it should return Date at
    ///   - hour: Hour value is should return Date at
    ///   - minute: Minute value it should return a Date at
    ///   - timeZone: Time zone to use when scheduling
    public static func yearly(month: Month, date: Int, hour: Int = 0, minute: Int = 0, timeZone: TimeZone = .current) -> Self {
        .init(minute: .specific(minute), hour: .specific(hour), date: .specific(date), month: .specific(month), timeZone: timeZone)
    }

    ///  Return next date in schedule after the supplied Date
    /// - Parameter date: start date
    public mutating func nextDate(after date: Date) -> Date? {
        var dateComponents = DateComponents()
        dateComponents.nanosecond = 0
        dateComponents.second = self.second.nextValue()
        dateComponents.minute = self.minute.nextValue()
        dateComponents.hour = self.hour.nextValue()
        dateComponents.weekday = self.day.nextValue()?.rawValue
        dateComponents.day = self.date.nextValue()
        dateComponents.month = self.month.nextValue()?.rawValue
        return self.calendar.nextDate(after: date, matching: dateComponents, matchingPolicy: .strict)
    }

    ///  Set up scheduler to return the correct next date, based on a supplied Date.
    /// - Parameter date: start date
    public mutating func setInitialNextDate(after date: Date) -> Date? {
        guard var nextDate = self.nextDate(after: date) else {
            return nil
        }
        var prevDate = date
        // Repeat while the nextDate is greater than the prevDate. At the point the nextDate is less than
        // the previous date we know any schedules with multiple values have selected the correct next value
        while prevDate < nextDate {
            prevDate = nextDate
            guard let nextDateUnwrapped = self.nextDate(after: date) else {
                return nil
            }
            nextDate = nextDateUnwrapped
        }

        return nextDate
    }
}

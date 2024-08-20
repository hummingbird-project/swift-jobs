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

public enum JobScheduleError: Swift.Error {
    case notScheduled
}

public struct JobSchedule: Sendable {
    struct ScheduledJob: Sendable {
        var nextScheduledDate: Date
        let schedule: Schedule
        let jobParameters: any JobParameters

        init(parameters: JobParameters, schedule: Schedule) throws {
            guard let nextScheduledDate = schedule.nextDate() else { throw JobScheduleError.notScheduled }
            self.nextScheduledDate = nextScheduledDate
            self.schedule = schedule
            self.jobParameters = parameters
        }
    }

    public init() {
        self.scheduledJobs = []
    }

    public mutating func addJob(_ parameters: JobParameters, schedule: Schedule) throws {
        try self.scheduledJobs.append(.init(parameters: parameters, schedule: schedule))
    }

    public func scheduler<Queue: JobQueueDriver>(on jobQueue: JobQueue<Queue>) -> Scheduler<Queue> {
        .init(jobQueue: jobQueue, jobSchedule: self)
    }

    func nextJob(after date: Date = .now) -> (offset: Int, element: ScheduledJob)? {
        return self.scheduledJobs.lazy.enumerated().min(by: { $0.element.nextScheduledDate < $1.element.nextScheduledDate })
    }

    mutating func updateNextScheduledDate(jobIndex: Int) {
        if let nextScheduledDate = self.scheduledJobs[jobIndex].schedule.nextDate(after: self.scheduledJobs[jobIndex].nextScheduledDate) {
            self.scheduledJobs[jobIndex].nextScheduledDate = nextScheduledDate
        } else {
            self.scheduledJobs[jobIndex].nextScheduledDate = .distantFuture
        }
    }

    public struct Scheduler<Driver: JobQueueDriver>: Service, CustomStringConvertible {
        enum StreamScheduledJobValue {
            case job(Int)
            case gracefulShutdown
        }

        let jobQueue: JobQueue<Driver>
        let jobSchedule: JobSchedule

        init(jobQueue: JobQueue<Driver>, jobSchedule: JobSchedule) {
            self.jobQueue = jobQueue
            self.jobSchedule = jobSchedule
        }

        public func run() async throws {
            let (jobAsyncStream, jobCont) = AsyncStream.makeStream(of: StreamScheduledJobValue.self)

            await withGracefulShutdownHandler {
                await withThrowingTaskGroup(of: Void.self) { group in
                    var jobSchedule = self.jobSchedule
                    func scheduleJob() {
                        guard let job = jobSchedule.nextJob() else {
                            return
                        }
                        self.jobQueue.logger.debug(
                            "Scheduled Job",
                            metadata: [
                                "_job_type": .stringConvertible(type(of: job.element.jobParameters).jobName),
                                "_job_time": .stringConvertible(job.element.nextScheduledDate),
                            ]
                        )
                        group.addTask {
                            let timeInterval = job.element.nextScheduledDate.timeIntervalSinceNow
                            try await Task.sleep(for: .seconds(timeInterval))
                            jobCont.yield(.job(job.offset))
                        }
                    }
                    scheduleJob()
                    for await value in jobAsyncStream {
                        switch value {
                        case .gracefulShutdown:
                            return
                        case .job(let jobIndex):
                            do {
                                _ = try await jobSchedule.scheduledJobs[jobIndex].jobParameters.push(to: self.jobQueue)
                                jobSchedule.updateNextScheduledDate(jobIndex: jobIndex)
                            } catch {}
                            scheduleJob()
                        }
                    }
                }
            } onGracefulShutdown: {
                jobCont.yield(.gracefulShutdown)
            }
        }

        public var description: String { "JobScheduler" }
    }

    var scheduledJobs: [ScheduledJob]
}

public struct Schedule: Sendable {
    public enum Day: Int, Sendable {
        case sunday = 1
        case monday = 2
        case tuesday = 3
        case wednesday = 4
        case thursday = 5
        case friday = 6
        case saturday = 7
    }

    public enum Month: Int, Sendable {
        case january = 0
        case february = 1
        case march = 2
        case april = 3
        case may = 4
        case june = 5
        case july = 6
        case august = 7
        case september = 8
        case october = 9
        case november = 10
        case december = 11
    }

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

    public static func everyMinute(second: Int = 0) -> Self {
        .init(second: .specific(second))
    }

    public static func hourly(minute: Int = 0) -> Self {
        .init(minute: .specific(minute))
    }

    public static func daily(hour: Int = 0, minute: Int = 0, timeZone: TimeZone = .current) -> Self {
        .init(minute: .specific(minute), hour: .specific(hour), timeZone: timeZone)
    }

    public static func weekly(day: Day, hour: Int = 0, timeZone: TimeZone = .current) -> Self {
        .init(hour: .specific(hour), day: .specific(day), timeZone: timeZone)
    }

    public static func monthly(date: Int, hour: Int = 0, timeZone: TimeZone = .current) -> Self {
        .init(hour: .specific(hour), date: .specific(date), timeZone: timeZone)
    }

    func nextDate(after date: Date = .now) -> Date? {
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

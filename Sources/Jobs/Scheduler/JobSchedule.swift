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

import Logging
import ServiceLifecycle

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Protocol used to push an existential job
protocol SchedulableJobRequest: Sendable & Codable {
    func push<Queue: JobQueueDriver>(
        to jobQueue: JobQueue<Queue>,
        options: Queue.JobOptions
    ) async throws -> Queue.JobID
}

/// An array of Jobs with schedules detailing when they should be run
///
/// Create your own Job Schedule and then create a scheduler Service to
/// generate Jobs on a specified JobQueue when the schedule requires it
///
/// ```swift
/// let jobSchedule = JobSchedule([
///     .init(job: RemoveDeadSessionsJob(), schedule: .weekly(day: .sunday, hour: 4))
/// ])
/// let serviceGroup = ServiceGroup(
///     configuration: .init(
///         services: [jobQueue, jobSchedule.scheduler(on: jobQueue)]
///     )
/// )
/// ```
public struct JobSchedule: MutableCollection, Sendable {
    /// What to do when job scheduler gets behind schedule
    public struct ScheduleAccuracy: Equatable, Sendable {
        private enum _Internal: Sendable {
            case latest
            case all
        }

        private let value: _Internal
        private init(_ value: _Internal) {
            self.value = value
        }

        /// Only run latest job
        public static var latest: Self { .init(.latest) }
        /// Run all jobs
        public static var all: Self { .init(.all) }
    }

    /// A single scheduled Job
    public struct Element: Sendable {
        var jobName: String
        var nextScheduledDate: Date
        var schedule: Schedule
        let accuracy: ScheduleAccuracy
        let createJobRequest: @Sendable (Date, Date?) -> any SchedulableJobRequest

        public init<Parameters: JobParameters>(job: Parameters, schedule: Schedule, accuracy: ScheduleAccuracy = .latest) {
            self.nextScheduledDate = .now
            self.schedule = schedule
            self.accuracy = accuracy
            self.jobName = Parameters.jobName
            self.createJobRequest = { queuedAt, nextScheduledAt in
                JobRequest(
                    name: Parameters.jobName,
                    parameters: job,
                    queuedAt: queuedAt,
                    attempt: 1,
                    nextScheduledAt: nextScheduledAt
                )
            }
        }

        public init<Parameters: Sendable & Codable>(
            _ jobName: JobName<Parameters>,
            parameters: Parameters,
            schedule: Schedule,
            accuracy: ScheduleAccuracy = .latest
        ) {
            self.nextScheduledDate = .now
            self.schedule = schedule
            self.accuracy = accuracy
            self.jobName = jobName.name
            self.createJobRequest = { queuedAt, nextScheduledAt in
                JobRequest(
                    name: jobName.name,
                    parameters: parameters,
                    queuedAt: queuedAt,
                    attempt: 1,
                    nextScheduledAt: nextScheduledAt
                )
            }
        }

        mutating func setInitialNextDate(after date: Date, now: Date = .now, logger: Logger) {
            switch self.accuracy {
            case .all:
                // set nextScheduledDate based on date supplied
                self.nextScheduledDate = self.schedule.setInitialNextDate(after: date) ?? .distantFuture
            case .latest:
                // set nextScheduledDate based on date supplied
                self.nextScheduledDate = self.schedule.setInitialNextDate(after: date) ?? .distantFuture
                // if schedule accuracy is set to latest that means it should only supply at the most one job
                // prior to current date. If there are one of more jobs scheduled between date supplied and now
                // return now. If there are no jobs scheduled between date supplied and now return the next scheduled
                // date
                if self.accuracy == .latest, self.nextScheduledDate < now {
                    self.schedule.setInitialNextDateJustBefore(date: now)
                    self.nextScheduledDate = now
                }
            default:
                preconditionFailure("Unsupported schedule accuracy")
            }
            logger.debug(
                "First scheduled date for job",
                metadata: [
                    "JobName": .stringConvertible(self.jobName),
                    "JobTime": .stringConvertible(self.nextScheduledDate),
                ]
            )
        }
    }

    /// Initialize JobSchedule with no jobs
    public init() {
        self.elements = []
    }

    /// Initialize JobSchedule with array of jobs
    ///
    /// - Parameter elements: Array of Jobs and schedules
    public init(_ elements: [JobSchedule.Element]) {
        self.elements = elements
    }

    ///  Add Job to Schedule
    /// - Parameters:
    ///   - job: Job parameters
    ///   - schedule: Schedule for job
    public mutating func addJob(_ job: some JobParameters, schedule: Schedule, accuracy: ScheduleAccuracy = .latest) {
        self.elements.append(.init(job: job, schedule: schedule, accuracy: accuracy))
    }

    ///  Add Job to Schedule
    /// - Parameters:
    ///   - job: Job parameters
    ///   - schedule: Schedule for job
    public mutating func addJob<Parameters: Sendable & Codable>(
        _ jobName: JobName<Parameters>,
        parameters: Parameters,
        schedule: Schedule,
        accuracy: ScheduleAccuracy = .latest
    ) {
        self.elements.append(.init(jobName, parameters: parameters, schedule: schedule, accuracy: accuracy))
    }

    ///  Create Job scheduler Service
    /// - Parameters
    ///   - jobQueue: Job queue to place jobs
    ///   - name: Job schedule name, used when storing metadata for schedule
    ///   - jobOptions: Job options applied to all jobs scheduled
    /// - Returns: JobScheduler
    public func scheduler<Queue: JobQueueDriver>(
        on jobQueue: JobQueue<Queue>,
        named name: String = "default",
        jobOptions: Queue.JobOptions = .init()
    ) async -> Scheduler<Queue> {
        await .init(named: name, jobQueue: jobQueue, jobOptions: jobOptions, jobSchedule: self)
    }

    func nextJob() -> (offset: Int, element: Element)? {
        self.lazy.enumerated().min(by: { $0.element.nextScheduledDate < $1.element.nextScheduledDate })
    }

    mutating func updateNextScheduledDate(jobIndex: Int) {
        let dateFrom: Date = self[jobIndex].nextScheduledDate
        if let nextScheduledDate = self[jobIndex].schedule.nextDate(after: dateFrom) {
            self[jobIndex].nextScheduledDate = nextScheduledDate
        } else {
            self[jobIndex].nextScheduledDate = .distantFuture
        }
    }

    mutating func setInitialNextDate(
        logger: Logger,
        getLastScheduledDate: (String) async throws -> Date?
    ) async throws {
        for index in 0..<self.count {
            let date: Date
            if let lastDate = try await getLastScheduledDate(self.elements[index].jobName) {
                date = lastDate
                logger.info("Last scheduled date \(date).")
            } else {
                date = .now
                logger.info("No last scheduled date so scheduling from now.")
            }
            self[index].setInitialNextDate(after: date, logger: logger)
        }
    }

    /// AsyncSequence of Jobs based on a JobSchedule
    struct JobSequence: AsyncSequence {
        struct Element {
            let element: JobSchedule.Element
            let date: Date
            let nextScheduledAt: Date?
        }
        let jobSchedule: JobSchedule
        let logger: Logger

        struct AsyncIterator: AsyncIteratorProtocol {
            var jobSchedule: JobSchedule
            let logger: Logger

            init(schedule: JobSchedule, logger: Logger) {
                self.jobSchedule = schedule
                self.logger = logger
            }

            mutating func next() async -> Element? {
                guard let job = self.jobSchedule.nextJob() else {
                    return nil
                }
                self.logger.debug(
                    "Next Scheduled Job",
                    metadata: [
                        "JobName": .stringConvertible(job.element.jobName),
                        "JobTime": .stringConvertible(job.element.nextScheduledDate),
                    ]
                )
                let scheduledDate = job.element.nextScheduledDate
                let timeInterval = scheduledDate.timeIntervalSinceNow
                do {
                    if timeInterval > 0 {
                        try await Task.sleep(until: .now + .seconds(timeInterval))
                    }
                    self.jobSchedule.updateNextScheduledDate(jobIndex: job.offset)
                    return Element(
                        element: job.element,
                        date: scheduledDate,
                        nextScheduledAt: self.jobSchedule[job.offset].nextScheduledDate
                    )
                } catch {
                    return nil
                }
            }
        }

        func makeAsyncIterator() -> AsyncIterator {
            .init(schedule: self.jobSchedule, logger: self.logger)
        }
    }

    /// Job Scheduler Service
    public struct Scheduler<Driver: JobQueueDriver & JobMetadataDriver>: Service, CustomStringConvertible {
        let name: String
        let jobQueue: JobQueue<Driver>
        let jobSchedule: JobSchedule
        let jobOptions: Driver.JobOptions

        init(named name: String, jobQueue: JobQueue<Driver>, jobOptions: Driver.JobOptions, jobSchedule: JobSchedule) async {
            self.name = name
            self.jobQueue = jobQueue
            self.jobSchedule = jobSchedule
            self.jobOptions = jobOptions
        }

        /// Run Job scheduler
        public func run() async throws {
            try await self.jobQueue.queue.waitUntilReady()
            var jobSchedule = self.jobSchedule
            // Update next scheduled date for each job schedule based off the last scheduled date stored
            do {
                try await jobSchedule.setInitialNextDate(logger: self.jobQueue.logger) { jobName in
                    try await self.jobQueue.queue.getMetadata(.jobScheduleLastDate(schedulerName: self.name, jobName: jobName))
                }
            } catch {
                self.jobQueue.logger.error(
                    "Failed to get last scheduled job date.",
                    metadata: [
                        "error": "\(String(reflecting: error))"
                    ]
                )
            }
            let scheduledJobSequence = JobSequence(
                jobSchedule: jobSchedule,
                logger: self.jobQueue.logger
            )
            for await job in scheduledJobSequence.cancelOnGracefulShutdown() {
                do {
                    let request = job.element.createJobRequest(job.date, job.nextScheduledAt)
                    _ = try await request.push(to: self.jobQueue, options: self.jobOptions)
                    try await self.jobQueue.queue.setMetadata(
                        key: .jobScheduleLastDate(schedulerName: self.name, jobName: job.element.jobName),
                        value: job.date
                    )
                } catch {
                    self.jobQueue.logger.error(
                        "Failed: to schedule job",
                        metadata: [
                            "error": "\(String(reflecting: error))"
                        ]
                    )
                }
            }
        }

        public var description: String { "JobScheduler" }
    }

    var elements: [Element]
}

/// Extensions to JobSchedule to make it conform to `MutableCollection`
extension JobSchedule {
    public typealias Index = Array<Element>.Index
    /// The position of the first element
    public var startIndex: Index { self.elements.startIndex }
    /// The position of the element just after the last element
    public var endIndex: Index { self.elements.endIndex }
    /// Access element at specific position
    public subscript(_ index: Index) -> Element {
        get { self.elements[index] }
        set { self.elements[index] = newValue }
    }

    /// Returns the index immediately after the given index
    public func index(after index: Index) -> Index { self.elements.index(after: index) }
}

extension JobMetadataKey where Value == Date {
    static func jobScheduleLastDate(schedulerName: String, jobName: String) -> Self { "\(schedulerName).\(jobName).jobScheduleLastDate" }
}

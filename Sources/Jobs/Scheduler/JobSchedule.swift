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

import ExtrasBase64
import Logging
import NIOCore
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
        options: Scheduler<Queue>.Options = .init()
    ) async -> Scheduler<Queue> {
        await .init(named: name, jobQueue: jobQueue, options: options, jobSchedule: self)
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
    struct JobSequence: Sequence {
        struct Element {
            let element: JobSchedule.Element
            let date: Date
            let nextScheduledAt: Date?
        }
        let jobSchedule: JobSchedule
        let logger: Logger

        struct Iterator: IteratorProtocol {
            var jobSchedule: JobSchedule
            let logger: Logger

            init(schedule: JobSchedule, logger: Logger) {
                self.jobSchedule = schedule
                self.logger = logger
            }

            mutating func next() -> Element? {
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
                self.jobSchedule.updateNextScheduledDate(jobIndex: job.offset)
                return Element(
                    element: job.element,
                    date: scheduledDate,
                    nextScheduledAt: self.jobSchedule[job.offset].nextScheduledDate
                )
            }
        }

        func makeIterator() -> Iterator {
            .init(schedule: self.jobSchedule, logger: self.logger)
        }
    }

    /// Job Scheduler Service
    public struct Scheduler<Driver: JobQueueDriver & JobMetadataDriver>: Service, CustomStringConvertible {
        /// Defines how often if at all a lock should be acquired
        public struct ExclusiveLock: Sendable {
            enum Value: Sendable {
                case ignore
                case acquire(every: TimeInterval, for: TimeInterval)
            }
            let value: Value
            /// Ignore lock
            public static var ignore: Self { .init(value: .ignore) }
            /// Acquire lock
            /// - Parameters:
            ///   - every: Frequency of acquiring the lock
            ///   - for: How long the lock should be acquired for
            public static func acquire(every: Duration, for: Duration) -> Self {
                precondition(`for` > every, "The time between acquiring each lock shoud be less then the time you acquire the lock for.")
                return .init(value: .acquire(every: .init(duration: every), for: .init(duration: `for`)))
            }
        }
        /// Scheduler options
        public struct Options: Sendable {
            ///
            let jobOptions: Driver.JobOptions
            let schedulerLock: ExclusiveLock

            ///  Initialize Scheduler Options
            /// - Parameters:
            ///   - jobOptions: Job options given to scheduled jobs
            ///   - schedulerLock: Define how scheduler lock should be acquired if at all. If you have multiple scheduler
            ///       processes you should set this to acquire a lock so one scheduler can be defined the primary
            public init(
                jobOptions: Driver.JobOptions = .init(delayUntil: .now),
                schedulerLock: ExclusiveLock = .ignore
            ) {
                self.jobOptions = jobOptions
                self.schedulerLock = schedulerLock
            }
        }
        let name: String
        let jobQueue: JobQueue<Driver>
        let jobSchedule: JobSchedule
        let options: Options

        init(named name: String, jobQueue: JobQueue<Driver>, options: Options, jobSchedule: JobSchedule) async {
            self.name = name
            self.jobQueue = jobQueue
            self.jobSchedule = jobSchedule
            self.options = options
        }

        /// Run Job scheduler
        public func run() async throws {
            let bytes: [UInt8] = (0..<16).map { _ in UInt8.random(in: 0...255) }
            let lockID = ByteBuffer(string: Base64.encodeToString(bytes: bytes))

            try await self.jobQueue.queue.waitUntilReady()

            /// If we are locking the scheduler to one process then attempt to acquire the lock
            /// if it fails try again in later on
            switch self.options.schedulerLock.value {
            case .acquire(let every, let length):
                while !Task.isShuttingDownGracefully, !Task.isCancelled {
                    guard await self.acquireLock(lockID, expiresIn: length) else {
                        self.jobQueue.logger.info("Failed to acquire scheduler lock")
                        try? await cancelWhenGracefulShutdown {
                            try await Task.sleep(for: .seconds(every))
                        }
                        continue
                    }
                    self.jobQueue.logger.info("Scheduler running")
                    await runScheduler(lockID)
                }
                // Release lock so another process can pick up the scheduler
                await self.releaseLock(lockID)

            case .ignore:
                await runScheduler(lockID)
            }
        }

        /// Run Job scheduler
        public func runScheduler(_ lockID: ByteBuffer) async {
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
            for job in scheduledJobSequence {
                do {
                    try await cancelWhenGracefulShutdown {
                        /// Wait until job is ready to schedule. Depending on if we are locking the scheduler
                        /// acquire the scheduler lock every so often
                        switch self.options.schedulerLock.value {
                        case .ignore:
                            try await Task.sleep(for: .seconds(job.date.timeIntervalSinceNow))

                        case .acquire(let every, let length):
                            guard await self.acquireLock(lockID, expiresIn: length) else {
                                self.jobQueue.logger.info("Failed to acquire scheduler lock")
                                break
                            }

                            while true {
                                let timeInterval = job.date.timeIntervalSinceNow
                                guard timeInterval > every else {
                                    if timeInterval > 0 {
                                        try await Task.sleep(for: .seconds(timeInterval))
                                    }
                                    break
                                }
                                try await Task.sleep(for: .seconds(every))
                                guard await self.acquireLock(lockID, expiresIn: length) else {
                                    self.jobQueue.logger.info("Failed to acquire scheduler lock")
                                    break
                                }
                            }
                        }
                    }
                } catch {
                    break
                }
                do {
                    let request = job.element.createJobRequest(job.date, job.nextScheduledAt)
                    _ = try await request.push(to: self.jobQueue, options: self.options.jobOptions)
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

        private func acquireLock(_ lockID: ByteBuffer, expiresIn: TimeInterval) async -> Bool {
            do {
                return try await self.jobQueue.queue.acquireLock(
                    key: .jobSchedulerLock(schedulerName: self.name),
                    id: lockID,
                    expiresIn: expiresIn
                )
            } catch {
                return false
            }
        }

        private func releaseLock(_ lockID: ByteBuffer) async {
            do {
                try await self.jobQueue.queue.releaseLock(
                    key: .jobSchedulerLock(schedulerName: self.name),
                    id: lockID
                )
            } catch {
                self.jobQueue.logger.debug("Failed to release schedule lock")
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

extension JobMetadataKey where Value == ByteBuffer {
    static func jobSchedulerLock(schedulerName: String) -> Self { "\(schedulerName).jobSchedulerLock" }
}

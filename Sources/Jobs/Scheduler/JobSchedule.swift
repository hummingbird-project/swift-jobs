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

/// Errors thrown by JobSchedule
public struct JobScheduleError: Error {
    private enum _Internal {
        case notScheduled
    }

    private let _value: _Internal
    private init(_ value: _Internal) {
        self._value = value
    }

    /// Job did not get scheduled due to invalid schedule
    public static var notScheduled: Self { .init(.notScheduled) }
}

/// An array of Jobs with schedules detailing when they should be run
///
/// Create your own Job Schedule and then create a scheduler Service to
/// generate Jobs on a specified JobQueue when the schedule requires it
///
/// ```swift
/// let jobSchedule = JobSchedule([(job: RemoveDeadSessionsJob(), schedule: .weekly(day: .sunday, hour: 4))])
/// let serviceGroup = ServiceGroup(
///     configuration: .init(
///         services: [jobQueue, jobSchedule.scheduler(on: jobQueue)]
///     )
/// )
/// ```
public struct JobSchedule: MutableCollection, Sendable {
    /// A single scheduled Job
    public struct Element: Sendable {
        var nextScheduledDate: Date
        let schedule: Schedule
        let jobParameters: any JobParameters

        init(job: JobParameters, schedule: Schedule) throws {
            guard let nextScheduledDate = schedule.nextDate() else { throw JobScheduleError.notScheduled }
            self.nextScheduledDate = nextScheduledDate
            self.schedule = schedule
            self.jobParameters = job
        }
    }

    /// Initialize JobSchedule with no jobs
    public init() {
        self.elements = []
    }

    /// Initialize JobSchedule with array of jobs
    ///
    /// - Parameter elements: Array of Jobs and schedules
    public init(_ elements: [(job: JobParameters, schedule: Schedule)]) throws {
        self.elements = try elements.map { try .init(job: $0.job, schedule: $0.schedule) }
    }

    ///  Add Job to Schedule
    /// - Parameters:
    ///   - job: Job parameters
    ///   - schedule: Schedule for job
    public mutating func addJob(_ job: JobParameters, schedule: Schedule) throws {
        try self.elements.append(.init(job: job, schedule: schedule))
    }

    ///  Create JobScheduler Service
    /// - Parameter jobQueue: Job queue to place jobs
    /// - Returns: JobScheduler
    public func scheduler<Queue: JobQueueDriver>(on jobQueue: JobQueue<Queue>) -> Scheduler<Queue> {
        .init(jobQueue: jobQueue, jobSchedule: self)
    }

    func nextJob() -> (offset: Int, element: Element)? {
        return self.lazy.enumerated().min(by: { $0.element.nextScheduledDate < $1.element.nextScheduledDate })
    }

    mutating func updateNextScheduledDate(jobIndex: Int) {
        if let nextScheduledDate = self[jobIndex].schedule.nextDate(after: self[jobIndex].nextScheduledDate) {
            self[jobIndex].nextScheduledDate = nextScheduledDate
        } else {
            self[jobIndex].nextScheduledDate = .distantFuture
        }
    }

    /// AsyncSequence of Jobs based on a JobSchedule
    struct JobSequence: AsyncSequence {
        typealias Element = JobParameters
        let jobSchedule: JobSchedule
        let logger: Logger

        struct AsyncIterator: AsyncIteratorProtocol {
            var jobSchedule: JobSchedule
            let logger: Logger

            init(schedule: JobSchedule, logger: Logger) {
                self.jobSchedule = schedule
                self.logger = logger
            }

            mutating func next() async -> JobParameters? {
                guard let job = self.jobSchedule.nextJob() else {
                    return nil
                }
                self.logger.debug(
                    "Next Scheduled Job",
                    metadata: [
                        "_job_type": .stringConvertible(type(of: job.element.jobParameters).jobName),
                        "_job_time": .stringConvertible(job.element.nextScheduledDate),
                    ]
                )
                let timeInterval = job.element.nextScheduledDate.timeIntervalSinceNow
                do {
                    try await Task.sleep(until: .now + .seconds(timeInterval))
                    self.jobSchedule.updateNextScheduledDate(jobIndex: job.offset)
                    return job.element.jobParameters
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
    public struct Scheduler<Driver: JobQueueDriver>: Service, CustomStringConvertible {
        let jobQueue: JobQueue<Driver>
        let jobSchedule: JobSchedule

        init(jobQueue: JobQueue<Driver>, jobSchedule: JobSchedule) {
            self.jobQueue = jobQueue
            self.jobSchedule = jobSchedule
        }

        /// Run Job scheduler
        public func run() async throws {
            let scheduledJobSequence = JobSequence(
                jobSchedule: self.jobSchedule,
                logger: self.jobQueue.logger
            )
            for await job in scheduledJobSequence.cancelOnGracefulShutdown() {
                do {
                    _ = try await job.push(to: self.jobQueue)
                } catch {
                    self.jobQueue.logger.debug("Failed: to schedule job")
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
        get { return self.elements[index] }
        set { self.elements[index] = newValue }
    }

    /// Returns the index immediately after the given index
    public func index(after index: Index) -> Index { self.elements.index(after: index) }
}

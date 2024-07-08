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
/// let jobSchedule = JobSchedule([(job: RemoveUnusedSessionsJob(), schedule: .weekly(day: .sunday))])
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

    /// Job Scheduler Service
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

        /// Run Job scheduler
        public func run() async throws {
            let (jobStream, jobSource) = AsyncStream.makeStream(of: StreamScheduledJobValue.self)

            await withGracefulShutdownHandler {
                await withDiscardingTaskGroup { group in
                    var jobSchedule = self.jobSchedule
                    func scheduleJob() {
                        // get next job to schedule
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
                        // add task to add job to queue once it reaches its scheduled date
                        group.addTask {
                            let timeInterval = job.element.nextScheduledDate.timeIntervalSinceNow
                            do {
                                try await Task.sleep(until: .now + .seconds(timeInterval))
                                jobSource.yield(.job(job.offset))
                            } catch {}
                        }
                    }

                    scheduleJob()
                    loop: for await value in jobStream {
                        switch value {
                        case .gracefulShutdown:
                            break loop
                        case .job(let jobIndex):
                            do {
                                _ = try await jobSchedule[jobIndex].jobParameters.push(to: self.jobQueue)
                                jobSchedule.updateNextScheduledDate(jobIndex: jobIndex)
                            } catch {}
                            scheduleJob()
                        }
                    }
                    // cancel any running tasks
                    group.cancelAll()
                }
            } onGracefulShutdown: {
                jobSource.yield(.gracefulShutdown)
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

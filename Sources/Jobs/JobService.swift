//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

public import Logging
public import ServiceLifecycle

/// JobService brings together all the components of swift-jobs into one type
public struct JobService<Queue: JobQueueDriver>: JobQueueProtocol where Queue: JobServiceDriver {
    public struct Options: Sendable {
        var queue: JobQueueOptions
        var processor: JobQueueProcessorOptions
        var scheduler: JobSchedule.Scheduler<Queue>.Options
        var cleanup: Queue.CleanupOptions

        public init(
            queue: JobQueueOptions = .init(),
            processor: JobQueueProcessorOptions = .init(),
            scheduler: JobSchedule.Scheduler<Queue>.Options = .init(),
            cleanup: Queue.CleanupOptions = .default
        ) {
            self.queue = queue
            self.processor = processor
            self.scheduler = scheduler
            self.cleanup = cleanup
        }
    }

    /// Job queue that service uses
    public let queue: JobQueue<Queue>

    let serviceOptions: Options
    @usableFromInline
    var schedule: JobSchedule

    public var logger: Logger { queue.logger }

    public init(
        _ queue: Queue,
        logger: Logger,
        options: Options = .init(),
        @JobMiddlewareBuilder middleware: () -> some JobMiddleware = { NullJobMiddleware() }
    ) {
        self.serviceOptions = options
        self.queue = .init(queue, logger: logger, options: options.queue, middleware: middleware)
        self.schedule = .init()
        queue.scheduleQueueCleanup(&self.schedule, options: options.cleanup)
    }

    public init(
        _ queue: Queue,
        logger: Logger,
        options: Options = .init(),
        @JobMiddlewareBuilder middleware: (Queue) -> some JobMiddleware
    ) {
        self.serviceOptions = options
        self.queue = .init(queue, logger: logger, options: options.queue, middleware: middleware)
        self.schedule = .init()
        queue.scheduleQueueCleanup(&self.schedule, options: options.cleanup)
    }

    ///  Push Job onto queue
    /// - Parameters:
    ///   - parameters: Job parameters
    ///   - parameters: parameters for the job
    /// - Returns: Identifier of queued job
    @discardableResult
    @inlinable
    public func push<Parameters: Sendable>(
        jobRequest: JobRequest<Parameters>,
        options: Queue.JobOptions
    ) async throws -> Queue.JobID {
        try await self.queue.push(jobRequest: jobRequest, options: options)
    }

    ///  Register job type
    /// - Parameters:
    ///   - job: Job definition
    @inlinable
    public func registerJob(_ job: JobDefinition<some Sendable & Codable>) {
        self.queue.registerJob(job)
    }

    ///  Create JobQueue processor that will process jobs pushed to the queue
    /// - Parameter options: Processor options
    @inlinable
    public func processor(options: JobQueueProcessorOptions) -> any Service {
        self.queue.processor(options: options)
    }

    /// Job queue options
    @inlinable
    public var options: JobQueueOptions { self.queue.options }

    ///  Add Job to Schedule
    /// - Parameters:
    ///   - job: Job parameters
    ///   - schedule: Schedule for job
    @inlinable
    public mutating func addScheduledJob(_ job: some JobParameters, schedule: Schedule, accuracy: JobSchedule.ScheduleAccuracy = .latest) {
        self.schedule.addJob(job, schedule: schedule, accuracy: accuracy)
    }

    ///  Add Job to Schedule
    /// - Parameters:
    ///   - job: Job parameters
    ///   - schedule: Schedule for job
    @inlinable
    public mutating func addScheduledJob<Parameters: Sendable & Codable>(
        _ jobName: JobName<Parameters>,
        parameters: Parameters,
        schedule: Schedule,
        accuracy: JobSchedule.ScheduleAccuracy = .latest
    ) {
        self.schedule.addJob(jobName, parameters: parameters, schedule: schedule, accuracy: accuracy)
    }
}

extension JobService: Service {
    public func run() async throws {
        let serviceGroup = await ServiceGroup(
            services: [
                self.queue.processor(options: self.serviceOptions.processor),
                self.schedule.scheduler(on: queue, named: self.queue.queue.context.queueName, options: self.serviceOptions.scheduler),
            ],
            gracefulShutdownSignals: [],
            logger: self.logger
        )
        try await serviceGroup.run()
    }
}

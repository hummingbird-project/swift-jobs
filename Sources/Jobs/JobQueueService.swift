//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

import Logging
import ServiceLifecycle

/// Service that combines a job queue, job queue processor and scheduler in one type
public struct JobQueueService<Queue: JobQueueDriver & JobMetadataDriver>: Service {
    /// Job queue service options. Includes
    public struct Options: Sendable {
        public var queue: JobQueueOptions
        public var processor: JobQueueProcessorOptions
        public var scheduler: JobSchedule.Scheduler<Queue>.Options

        public init(
            queue: JobQueueOptions = .init(),
            processor: JobQueueProcessorOptions = .init(),
            scheduler: JobSchedule.Scheduler<Queue>.Options = .init()
        ) {
            self.queue = queue
            self.processor = processor
            self.scheduler = scheduler
        }
    }
    /// Job queue
    public let jobQueue: JobQueue<Queue>
    /// Options for job queue, job processor and job scheduler
    public var options: Options
    /// Job schedule, add jobs here to have them scheduled by service
    public var schedule: JobSchedule

    /// Initialize JobQueue service
    /// - Parameters:
    ///   - queue: JobQueue driver
    ///   - logger: Logger
    ///   - options: Options for job queue, job processor and job scheduler
    ///   - middleware: Middleware to run on job queue
    public init(
        _ queue: Queue,
        logger: Logger,
        options: Options = .init(),
        @JobMiddlewareBuilder middleware: () -> some JobMiddleware = { NullJobMiddleware() }
    ) {
        self.jobQueue = JobQueue(queue, logger: logger, options: options.queue, middleware: middleware)
        self.options = options
        self.schedule = .init()
    }

    /// Initialize JobQueue service
    /// - Parameters:
    ///   - queue: JobQueue driver
    ///   - logger: Logger
    ///   - options: Options for job queue, job processor and job scheduler
    ///   - middleware: Middleware to run on job queue
    public init(
        _ queue: Queue,
        logger: Logger,
        options: Options = .init(),
        @JobMiddlewareBuilder middleware: (Queue) -> some JobMiddleware
    ) {
        self.jobQueue = JobQueue(queue, logger: logger, options: options.queue, middleware: middleware)
        self.options = options
        self.schedule = .init()
    }

    /// Run job queue service
    public func run() async throws {
        let serviceGroup = await ServiceGroup(
            services: [
                jobQueue.processor(options: self.options.processor),
                schedule.scheduler(
                    on: self.jobQueue,
                    named: "\(jobQueue.queue.context.queueName).service",
                    options: self.options.scheduler
                ),
            ],
            logger: jobQueue.logger
        )
        try await serviceGroup.run()
    }
}

//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2021-2024 the Hummingbird authors
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
import NIOCore
import ServiceLifecycle

/// Object handling a single job queue
final class JobQueueHandler<Queue: JobQueueDriver>: Sendable {
    init(queue: Queue, numWorkers: Int, logger: Logger, options: JobQueueOptions, middleware: any JobMiddleware) {
        self.queue = queue
        self.numWorkers = numWorkers
        self.logger = logger
        self.jobRegistry = .init()
        self.options = options
        self.middleware = middleware
    }

    ///  Register job
    /// - Parameters:
    ///   - id: Job Identifier
    ///   - maxRetryCount: Maximum number of times job is retried before being flagged as failed
    ///   - execute: Job code
    func registerJob(_ job: JobDefinition<some Codable & Sendable>) {
        self.jobRegistry.registerJob(job: job)
    }

    func run() async throws {
        try await withGracefulShutdownHandler {
            try await withThrowingTaskGroup(of: Void.self) { group in
                var iterator = self.queue.makeAsyncIterator()
                for _ in 0..<self.numWorkers {
                    if let job = try await iterator.next() {
                        group.addTask {
                            try await self.runJob(job)
                        }
                    }
                }
                while true {
                    try await group.next()
                    guard let job = try await iterator.next() else { break }
                    group.addTask {
                        try await self.runJob(job)
                    }
                }
            }
            await self.queue.shutdownGracefully()
        } onGracefulShutdown: {
            Task {
                await self.queue.stop()
            }
        }
    }

    func runJob(_ queuedJob: QueuedJob<Queue.JobID>) async throws {
        var logger = logger
        logger[metadataKey: "JobID"] = .stringConvertible(queuedJob.id)

        let job: any JobInstanceProtocol
        do {
            job = try self.jobRegistry.decode(queuedJob.jobBuffer)
            await self.middleware.onPopJob(result: .success(job), jobInstanceID: queuedJob.id.description)
        } catch let error as JobQueueError {
            if let jobName = error.jobName {
                logger[metadataKey: "JobName"] = .string(jobName)
            }
            if let details = error.details {
                logger[metadataKey: "Error"] = .string(details)
            }
            switch error.code {
            case .decodeJobFailed:
                logger.debug("Job failed to decode")
            case .unrecognisedJobId:
                logger.debug("Failed to find Job with ID while decoding")
            default:
                logger.debug("Job failed to decode")
            }
            try await self.queue.failed(jobId: queuedJob.id, error: error)
            await self.middleware.onPopJob(result: .failure(error), jobInstanceID: queuedJob.id.description)
            return
        } catch {
            logger[metadataKey: "Error"] = .string("\(error)")
            logger.debug("Job failed to decode")
            try await self.queue.failed(jobId: queuedJob.id, error: JobQueueError(code: .decodeJobFailed, jobName: nil))
            await self.middleware.onPopJob(
                result: .failure(JobQueueError(code: .decodeJobFailed, jobName: nil)),
                jobInstanceID: queuedJob.id.description
            )
            return
        }
        logger[metadataKey: "JobName"] = .string(job.name)

        logger.debug("Starting Job")
        do {
            do {
                let context = JobContext(jobInstanceID: queuedJob.id.description, logger: logger)
                try await self.middleware.handleJob(job: job, context: context) { job, context in
                    try await job.execute(context: context)
                }
            } catch let error as CancellationError {
                logger.debug("Job cancelled")
                // Job failed is called but due to the fact the task is cancelled, depending on the
                // job queue driver, the process of failing the job might not occur because itself
                // might get cancelled
                try await self.queue.failed(jobId: queuedJob.id, error: error)
                return
            } catch {
                if job.didFail {
                    logger.debug("Job: failed")
                    try await self.queue.failed(jobId: queuedJob.id, error: error)
                    return
                }

                let attempts = (job.attempts ?? 0) + 1

                let delay = self.calculateBackoff(attempts: attempts)

                // remove from processing lists
                try await self.queue.finished(jobId: queuedJob.id)
                // push new job in the queue
                let newJobId = try await self.queue.push(
                    self.queue.encode(job, attempts: attempts),
                    options: .init(
                        delayUntil: delay
                    )
                )

                logger.debug(
                    "Retrying Job",
                    metadata: [
                        "JobID": .stringConvertible(newJobId),
                        "JobName": .string(job.name),
                        "attempts": .stringConvertible(attempts),
                        "delayedUntil": .stringConvertible(delay),
                    ]
                )
                return
            }
            logger.debug("Finished Job")
            try await self.queue.finished(jobId: queuedJob.id)
        } catch {
            logger.debug("Failed to set job status")
        }
    }

    private let jobRegistry: JobRegistry
    private let queue: Queue
    private let options: JobQueueOptions
    private let numWorkers: Int
    let middleware: any JobMiddleware
    let logger: Logger
}

extension JobQueueHandler: CustomStringConvertible {
    public var description: String { "JobQueueHandler<\(String(describing: Queue.self))>" }
}

extension JobQueueHandler {
    func calculateBackoff(attempts: Int) -> Date {
        let exp = exp2(Double(attempts))
        let delay = min(exp, self.options.maximumBackoff)
        return Date.now.addingTimeInterval(TimeInterval(self.options.jitter + delay))
    }
}

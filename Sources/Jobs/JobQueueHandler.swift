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

import Logging
import NIOCore
import ServiceLifecycle

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Object handling a single job queue
final class JobQueueHandler<Queue: JobQueueDriver>: Sendable {
    init(queue: Queue, numWorkers: Int, logger: Logger, options: JobQueueOptions, middleware: any JobMiddleware) {
        self.queue = queue
        self.numWorkers = numWorkers
        self.logger = logger
        self.options = options
        self.middleware = middleware
    }

    func run() async throws {
        try await withGracefulShutdownHandler {
            try await withThrowingTaskGroup(of: Void.self) { group in
                var iterator = self.queue.makeAsyncIterator()
                for _ in 0..<self.numWorkers {
                    if let jobResult = try await withExponentialBackoff(
                        "Pop next job",
                        logger: self.logger,
                        operation: { try await iterator.next() }
                    ) {
                        group.addTask {
                            try await self.processJobResult(jobResult)
                        }
                    }
                }
                while true {
                    try await group.next()
                    guard
                        let jobResult = try await withExponentialBackoff(
                            "Pop next job",
                            logger: self.logger,
                            operation: { try await iterator.next() }
                        )
                    else { break }
                    group.addTask {
                        try await self.processJobResult(jobResult)
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

    /// Run operation and retry with exponential backoff if it fails
    func withExponentialBackoff<Value: Sendable>(
        _ message: @autoclosure () -> String,
        logger: Logger,
        operation: () async throws -> Value
    ) async throws -> Value {
        var attempt = 0
        while true {
            do {
                return try await operation()
            } catch let error as JobQueueDriverError where error.code == .connectionError {
                logger.debug("\(message()) failed")
                if self.options.driverRetryStrategy.shouldRetry(attempt: attempt, error: error) {
                    let wait = self.options.driverRetryStrategy.calculateBackoff(attempt: attempt)
                    try await cancelWhenGracefulShutdown {
                        try await Task.sleep(for: .seconds(wait))
                    }
                    attempt += 1
                } else {
                    throw error
                }
            }
            logger.debug("Retrying \(message())")
        }
    }

    /// Process job result from queue
    func processJobResult(_ jobResult: JobQueueResult<Queue.JobID>) async throws {
        var logger = self.logger
        logger[metadataKey: "JobID"] = .stringConvertible(jobResult.id)

        switch jobResult.result {
        case .success(let job):
            await self.middleware.onPopJob(result: .success(job), jobInstanceID: jobResult.id.description)
            logger[metadataKey: "JobName"] = .string(job.name)
            try await self.runJob(id: jobResult.id, job: job, logger: logger)

        case .failure(let error):
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
            try await withExponentialBackoff("Tag job as failed", logger: logger) {
                try await self.queue.failed(jobID: jobResult.id, error: error)
            }
            await self.middleware.onPopJob(result: .failure(error), jobInstanceID: jobResult.id.description)
        }
    }

    /// Run job
    func runJob(id jobID: Queue.JobID, job: any JobInstanceProtocol, logger: Logger) async throws {
        logger.debug("Starting Job")
        do {
            do {
                let context = JobContext(
                    jobInstanceID: jobID.description,
                    logger: logger,
                    queuedAt: job.queuedAt,
                    nextScheduledAt: job.nextScheduledAt
                )
                try await self.middleware.handleJob(job: job, context: context) { job, context in
                    try await job.execute(context: context)
                }
            } catch let error as CancellationError {
                logger.debug("Job cancelled")
                // We need to wrap the failed call in a unstructured Task to stop propagation of
                // Task cancellation to the `queue.fail()` call. We are calling `get` on the Task
                // as soon as we create it so can guarantee the Task is done when we leave the
                // function.
                try await Task {
                    try await withExponentialBackoff("Tag job as failed", logger: logger) {
                        try await self.queue.failed(jobID: jobID, error: error)
                    }
                }.value
                return
            } catch {
                if !job.shouldRetry(error: error) {
                    logger.debug("Job: failed")
                    try await withExponentialBackoff("Tag job as failed", logger: logger) {
                        try await self.queue.failed(jobID: jobID, error: error)
                    }
                    return
                }

                let attempts = (job.attempts ?? 0)
                let delay = job.retryStrategy.calculateBackoff(attempt: attempts)
                let delayUntil = Date.now.addingTimeInterval(delay)

                /// retry the current job
                try await withExponentialBackoff("Retry Job", logger: logger) {
                    try await self.queue.retry(
                        jobID,
                        job: job,
                        attempts: attempts + 1,
                        options: .init(delayUntil: delayUntil)
                    )
                }
                logger.debug(
                    "Retrying Job",
                    metadata: [
                        "JobID": .stringConvertible(jobID),
                        "JobName": .string(job.name),
                        "attempts": .stringConvertible(attempts),
                        "delayedUntil": .stringConvertible(delayUntil),
                    ]
                )
                return
            }
            logger.debug("Finished Job")
            try await withExponentialBackoff("Finish Job", logger: logger) {
                try await self.queue.finished(jobID: jobID)
            }
        } catch {
            logger.debug("Failed to set job status")
        }
    }

    let queue: Queue
    let options: JobQueueOptions
    private let numWorkers: Int
    let middleware: any JobMiddleware
    let logger: Logger
}

extension JobQueueHandler: CustomStringConvertible {
    public var description: String { "JobQueueHandler<\(String(describing: Queue.self))>" }
}

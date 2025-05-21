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
@usableFromInline
final class JobQueueHandler<Queue: JobQueueDriver>: Sendable {
    init(queue: Queue, numWorkers: Int, logger: Logger, options: JobQueueOptions, middleware: any JobMiddleware) {
        self.queue = queue
        self.numWorkers = numWorkers
        self.logger = logger
        self.options = options
        self.middleware = middleware
    }

    func run() async throws {
        let (stream, cont) = AsyncStream.makeStream(of: Void.self)
        try await withTaskCancellationOrGracefulShutdownHandler {
            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask {
                    try await withThrowingTaskGroup(of: Void.self) { group in
                        var iterator = self.queue.makeAsyncIterator()
                        for _ in 0..<self.numWorkers {
                            if let jobResult = try await iterator.next() {
                                group.addTask {
                                    try await self.processJobResult(jobResult)
                                }
                            }
                        }
                        while true {
                            try await group.next()
                            guard let jobResult = try await iterator.next() else { break }
                            group.addTask {
                                try await self.processJobResult(jobResult)
                            }
                        }

                        try await group.waitForAll()
                    }
                }

                group.addTask {
                    // wait until graceful shutdown or cancellation has been triggered
                    await stream.first { _ in true }
                    try await Task.sleep(for: self.options.gracefulShutdownTimeout)
                }
                // wait on first child task to return. If the first task to return is the queue handler then
                // cancel timeout task. If the first child task to return is the timeout task then cancel the
                // job queue handler
                try await group.next()
                group.cancelAll()
            }
        } onCancelOrGracefulShutdown: {
            // trigger timeout
            cont.finish()
            Task {
                await self.queue.stop()
            }
        }
        await self.queue.shutdownGracefully()
    }

    /// Process job result from queue
    func processJobResult(_ jobResult: JobQueueResult<Queue.JobID>) async throws {
        var logger = self.logger
        logger[metadataKey: "JobID"] = .stringConvertible(jobResult.id)

        switch jobResult.result {
        case .success(let job):
            await self.middleware.onPopJob(
                result: .success(job),
                context: .init(jobID: jobResult.id.description)
            )
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
            try await self.queue.failed(jobID: jobResult.id, error: error)
            await self.middleware.onPopJob(
                result: .failure(error),
                context: .init(jobID: jobResult.id.description)
            )
        }
    }

    /// Run job
    func runJob(id jobID: Queue.JobID, job: any JobInstanceProtocol, logger: Logger) async throws {
        logger.debug("Starting Job")
        do {
            do {
                let context = JobExecutionContext(
                    jobID: jobID.description,
                    logger: logger,
                    queuedAt: job.queuedAt,
                    nextScheduledAt: job.nextScheduledAt,
                    attempt: job.attempt
                )
                try await handleJob(job: job, context: context)
            } catch let error as CancellationError {
                logger.debug("Job cancelled")
                // We need to wrap the failed call in a unstructured Task to stop propagation of
                // Task cancellation to the `queue.fail()` call. We are calling `get` on the Task
                // as soon as we create it so can guarantee the Task is done when we leave the
                // function.
                try await Task {
                    try await self.queue.failed(jobID: jobID, error: error)
                }.value
                return
            } catch {
                if !job.shouldRetry(error: error) {
                    logger.debug("Job: failed")
                    try await self.queue.failed(jobID: jobID, error: error)
                    return
                }

                let delay = job.retryStrategy.calculateBackoff(attempt: job.attempt)
                let delayUntil = Date.now._advanced(by: delay)
                let attempt = job.attempt + 1

                /// retry the current job
                try await self.queue.retry(
                    jobID,
                    job: job,
                    attempt: attempt,
                    options: .init(delayUntil: delayUntil)
                )

                logger.debug(
                    "Retrying Job",
                    metadata: [
                        "JobID": .stringConvertible(jobID),
                        "JobName": .string(job.name),
                        "attempt": .stringConvertible(attempt),
                        "delayedUntil": .stringConvertible(delayUntil),
                    ]
                )
                return
            }
            logger.debug("Finished Job")
            try await self.queue.finished(jobID: jobID)
        } catch {
            logger.debug("Failed to set job status")
        }
    }

    func handleJob(job: any JobInstanceProtocol, context: JobExecutionContext) async throws {
        if let timeout = job.timeout {
            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask {
                    try await self.middleware.handleJob(job: job, context: context) { job, context in
                        try await job.execute(context: context)
                    }
                }
                group.addTask {
                    try await Task.sleep(for: timeout)
                    throw JobQueueError(code: .jobTimedOut, jobName: job.name)
                }
                do {
                    try await group.next()
                } catch let error as JobQueueError where error.code == .jobTimedOut {
                    group.cancelAll()
                    logger.debug("Job: timed out")
                    throw error
                }
                group.cancelAll()
            }
        } else {
            try await self.middleware.handleJob(job: job, context: context) { job, context in
                try await job.execute(context: context)
            }
        }
    }

    let queue: Queue
    let options: JobQueueOptions
    private let numWorkers: Int
    @usableFromInline
    let middleware: any JobMiddleware
    let logger: Logger
}

extension JobQueueHandler: CustomStringConvertible {
    public var description: String { "JobQueueHandler<\(String(describing: Queue.self))>" }
}

extension Date {
    // private version of advancing Date by Duration
    internal func _advanced(by duration: Duration) -> Date {
        var timeValue = self.timeIntervalSinceReferenceDate
        timeValue += Double(duration.components.seconds)
        timeValue += Double(duration.components.attoseconds) / 1_000_000_000_000_000_000
        return Date(timeIntervalSinceReferenceDate: timeValue)
    }
}

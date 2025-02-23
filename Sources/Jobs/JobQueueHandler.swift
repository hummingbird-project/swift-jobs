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
        self.options = options
        self.middleware = middleware
    }

    func run() async throws {
        try await withGracefulShutdownHandler {
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
            }
            await self.queue.shutdownGracefully()
        } onGracefulShutdown: {
            Task {
                await self.queue.stop()
            }
        }
    }

    /// Process job result from queue
    func processJobResult(_ jobResult: JobQueueResult<Queue.JobID>) async throws {
        var logger = self.logger
        logger[metadataKey: "JobID"] = .stringConvertible(jobResult.id)
        logger[metadataKey: "Queue"] = .stringConvertible(options.queueName)

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
            try await self.queue.failed(jobID: jobResult.id, error: error)
            await self.middleware.onPopJob(result: .failure(error), jobInstanceID: jobResult.id.description)
        }
    }

    /// Run job
    func runJob(id jobID: Queue.JobID, job: any JobInstanceProtocol, logger: Logger) async throws {
        logger.debug("Starting Job")
        do {
            do {
                let context = JobContext(jobInstanceID: jobID.description, logger: logger)
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
                    try await self.queue.failed(jobID: jobID, error: error)
                }.value
                return
            } catch {
                if job.didFail {
                    logger.debug("Job: failed")
                    try await self.queue.failed(jobID: jobID, error: error)
                    return
                }

                let attempts = (job.attempts ?? 0) + 1

                let delay = self.calculateBackoff(attempts: attempts)

                /// update the current job
                try await self.queue.retry(
                    jobID,
                    job: job,
                    attempts: attempts,
                    options: .init(
                        delayUntil: delay
                    )
                )

                logger.debug(
                    "Retrying Job",
                    metadata: [
                        "JobID": .stringConvertible(jobID),
                        "JobName": .string(job.name),
                        "attempts": .stringConvertible(attempts),
                        "delayedUntil": .stringConvertible(delay),
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

    let queue: Queue
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

extension JobQueueHandler {
    public var queueName: String {
        self.options.queueName
    }
}

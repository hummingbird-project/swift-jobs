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
import Metrics
import NIOCore
import ServiceLifecycle

/// Object handling a single job queue
final class JobQueueHandler<Queue: JobQueueDriver>: Sendable {
    init(queue: Queue, numWorkers: Int, logger: Logger, options: JobQueueOptions) {
        self.queue = queue
        self.numWorkers = numWorkers
        self.logger = logger
        self.jobRegistry = .init()
        self.options = options
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
        let startTime = DispatchTime.now().uptimeNanoseconds
        logger[metadataKey: "JobID"] = .stringConvertible(queuedJob.id)
        let job: any JobInstanceProtocol
        do {
            job = try self.jobRegistry.decode(queuedJob.jobBuffer)
        } catch let error as JobQueueError where error == .unrecognisedJobId {
            logger.debug("Failed to find Job with ID while decoding")
            try await self.queue.failed(jobId: queuedJob.id, error: error)
            Meter(label: JobMetricsHelper.discardedMeter, dimensions: [
                ("reason", "INVALID_JOB_ID"),
                ("jobID", queuedJob.id.description),
            ]).increment()
            return
        } catch {
            logger.debug("Job failed to decode")
            try await self.queue.failed(jobId: queuedJob.id, error: JobQueueError.decodeJobFailed)
            Meter(label: JobMetricsHelper.discardedMeter, dimensions: [
                ("reason", "DECODE_FAILED"),
                ("jobID", queuedJob.id.description),
            ]).increment()
            return
        }
        logger[metadataKey: "JobName"] = .string(job.name)

        // Calculate wait time from queued to processing
        let jobQueuedDuration = Date.now.timeIntervalSince(job.queuedAt)
        Timer(
            label: "\(JobMetricsHelper.metricsLabel).queued.duration",
            dimensions: [("name", job.name)],
            preferredDisplayUnit: .seconds
        ).recordSeconds(jobQueuedDuration)

        // Decrement the current job by 1
        Meter(label: JobMetricsHelper.meterLabel, dimensions: [
            ("status", JobMetricsHelper.JobStatus.queued.rawValue),
            ("name", job.name),
        ]).decrement()

        logger.debug("Starting Job")
        // Processing start here
        Meter(label: JobMetricsHelper.meterLabel, dimensions: [
            ("status", JobMetricsHelper.JobStatus.processing.rawValue),
            ("name", job.name),
        ]).increment()

        do {
            do {
                try await job.execute(context: .init(logger: logger))
            } catch let error as CancellationError {
                logger.debug("Job cancelled")
                // Job failed is called but due to the fact the task is cancelled, depending on the
                // job queue driver, the process of failing the job might not occur because itself
                // might get cancelled
                try await self.queue.failed(jobId: queuedJob.id, error: error)
                JobMetricsHelper.updateMetrics(for: job.name, startTime: startTime, error: error)
                return
            } catch {
                if job.didFail {
                    logger.debug("Job: failed")
                    try await self.queue.failed(jobId: queuedJob.id, error: error)
                    JobMetricsHelper.updateMetrics(for: job.name, startTime: startTime, error: error)
                    return
                }

                let attempts = (job.attempts ?? 0) + 1

                let delay = self.calculateBackoff(attempts: attempts)

                // remove from processing lists
                try await self.queue.finished(jobId: queuedJob.id)
                // push new job in the queue
                _ = try await self.queue.push(
                    self.queue.encode(job, attempts: attempts),
                    options: .init(
                        delayUntil: delay
                    )
                )
                JobMetricsHelper.updateMetrics(for: job.name, startTime: startTime, retrying: true)
                logger.debug("Retrying Job", metadata: [
                    "attempts": .stringConvertible(attempts),
                    "delayedUntil": .stringConvertible(delay),
                ])
                return
            }
            logger.debug("Finished Job")
            try await self.queue.finished(jobId: queuedJob.id)
            JobMetricsHelper.updateMetrics(for: job.name, startTime: startTime)
        } catch {
            logger.debug("Failed to set job status")
            JobMetricsHelper.updateMetrics(for: job.name, startTime: startTime, error: error)
        }
    }

    private let jobRegistry: JobRegistry
    private let queue: Queue
    private let options: JobQueueOptions
    private let numWorkers: Int
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

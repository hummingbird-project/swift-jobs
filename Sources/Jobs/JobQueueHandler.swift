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

import Dispatch
import Logging
import Metrics
import ServiceLifecycle

/// Object handling a single job queue
final class JobQueueHandler<Queue: JobQueueDriver>: Service {
    init(queue: Queue, numWorkers: Int, logger: Logger) {
        self.queue = queue
        self.numWorkers = numWorkers
        self.logger = logger
        self.jobRegistry = .init()
        Gauge(label: "worker_count").record(Double(1))
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
        try await self.queue.onInit()

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
        logger[metadataKey: "JobId"] = .stringConvertible(queuedJob.id)
        let job: any Job
        do {
            job = try self.jobRegistry.decode(queuedJob.jobBuffer)
        } catch let error as JobQueueError where error == .unrecognisedJobId {
            logger.debug("Failed to find Job with ID while decoding")
            try await self.queue.failed(jobId: queuedJob.id, error: error)
            return
        } catch {
            logger.debug("Job failed to decode")
            try await self.queue.failed(jobId: queuedJob.id, error: JobQueueError.decodeJobFailed)
            return
        }
        logger[metadataKey: "job_name"] = .string(job.name)

        var count = job.maxRetryCount
        logger.debug("Starting Job")

        do {
            while true {
                do {
                    try await job.execute(context: .init(logger: logger))
                    break
                } catch let error as CancellationError {
                    logger.debug("Job cancelled")
                    // Job failed is called but due to the fact the task is cancelled, depending on the
                    // job queue driver, the process of failing the job might not occur because itself
                    // might get cancelled
                    try await self.queue.failed(jobId: queuedJob.id, error: error)
                    self.updateJobMetrics(for: job.name, startTime: startTime, error: error)
                    return
                } catch {
                    if count <= 0 {
                        logger.debug("Job failed")
                        try await self.queue.failed(jobId: queuedJob.id, error: error)
                        self.updateJobMetrics(for: job.name, startTime: startTime, error: error)
                        return
                    }
                    count -= 1
                    logger.debug("Retrying Job")
                    Counter(
                        label: "retry_jobs_counter",
                        dimensions: [("job_name", job.name)]
                    ).increment()
                }
            }
            logger.debug("Finished Job")
            self.updateJobMetrics(for: job.name, startTime: startTime)
            try await self.queue.finished(jobId: queuedJob.id)
        } catch {
            logger.debug("Failed to set job status")
            self.updateJobMetrics(for: job.name, startTime: startTime, error: error)
        }
    }

    private let jobRegistry: JobRegistry
    private let queue: Queue
    private let numWorkers: Int
    let logger: Logger
}

extension JobQueueHandler: CustomStringConvertible {
    public var description: String { "JobQueueHandler<\(String(describing: Queue.self))>" }

    /// Used for the histogram which can be useful to see by job status
    private enum JobStatus: String, Codable, Sendable {
        case cancelled
        case failed
        case succeeded
    }

    private func updateJobMetrics(
        for name: String,
        startTime: UInt64,
        error: Error? = nil
    ) {
        let jobStatus: JobStatus = if let error {
            if error is CancellationError {
                .cancelled
            } else {
                .failed
            }
        } else {
            .succeeded
        }

        // Calculate job execution time
        Timer(
            label: "swift_jobs_duration",
            dimensions: [
                ("job_status", jobStatus.rawValue),
                ("job_name", name),
            ],
            preferredDisplayUnit: .seconds
        ).recordNanoseconds(DispatchTime.now().uptimeNanoseconds - startTime)

        // Increment job counter base on status
        if let error {
            if error is CancellationError {
                Counter(
                    label: "cancelled_jobs_counter",
                    dimensions: [("job_name", name)]
                ).increment()
            } else {
                Counter(
                    label: "failed_jobs_counter",
                    dimensions: [("job_name", name)]
                ).increment()
            }
        } else {
            Counter(
                label: "successful_jobs_counter",
                dimensions: [("job_name", name)]
            ).increment()
        }
    }
}

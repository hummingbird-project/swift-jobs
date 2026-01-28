//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

import Dispatch
import Metrics

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Add publishing of Metrics to a job queue
public struct MetricsJobMiddleware: JobMiddleware {
    @usableFromInline
    let queueName: String

    public init(queueName: String = "default") {
        self.queueName = queueName
    }

    /// Counter label
    @usableFromInline
    static let counterLabel: String = "swift.jobs"
    /// Job duration timer label
    @usableFromInline
    static let timerLabel: String = "swift.jobs.duration"
    /// Job queued timer label
    @usableFromInline
    static let queuedTimerLabel: String = "swift.jobs.queued.duration"
    /// Meter label for Processsing, Queued
    @usableFromInline
    static let meterLabel: String = "swift.jobs.meter"
    /// Counter label for discarded jobs
    @usableFromInline
    static let discardedCounter: String = "swift.jobs.discarded"
    /// Used for the histogram which can be useful to see by job status
    @usableFromInline
    enum JobStatus: String, Codable, Sendable {
        case queued
        case processing
        case retried
        case cancelled
        case failed
        case succeeded
        case completed
    }

    /// Job has been pushed onto the queue
    ///
    /// - Parameters:
    ///   - name: Job name
    ///   - parameters: Job parameters
    ///   - context: Job queue context
    @inlinable
    public func onPushJob<Parameters>(name: String, parameters: Parameters, context: JobPushQueueContext) async {
        if context.attempt > 1 {
            self.updateMetricsForRetry(for: name)
        }
        Meter(
            label: Self.meterLabel,
            dimensions: [
                ("status", JobStatus.queued.rawValue),
                ("name", name),
                ("queue", self.queueName),
            ]
        ).increment()

    }

    /// Job has been popped off the queue and decoded (with decode errors reported)
    ///
    /// - Parameters:
    ///   - result: Result of popping the job from the queue (Either job instance or error)
    ///   - context: Job queue context
    @inlinable
    public func onPopJob(result: Result<any JobInstanceProtocol, JobQueueError>, context: JobPopQueueContext) async {

        switch result {
        case .failure(let error):
            var counterDimensions: [(String, String)] = [
                ("reason", error.code.description),
                ("queue", self.queueName),
            ]

            if let jobName = error.jobName {
                let jobNameDimension = ("name", jobName)
                counterDimensions.append(jobNameDimension)

                Meter(
                    label: Self.meterLabel,
                    dimensions: [
                        ("status", JobStatus.queued.rawValue),
                        ("queue", self.queueName),
                        jobNameDimension,
                    ]
                ).decrement()
            }

            Counter(
                label: Self.discardedCounter,
                dimensions: counterDimensions
            ).increment()

        case .success(let job):
            // Decrement the current queue by 1
            Meter(
                label: Self.meterLabel,
                dimensions: [
                    ("status", JobStatus.queued.rawValue),
                    ("name", job.name),
                    ("queue", self.queueName),
                ]
            ).decrement()

            // Calculate wait time from queued to processing
            let jobQueuedDuration = Date.now.timeIntervalSince(job.queuedAt)
            Timer(
                label: Self.queuedTimerLabel,
                dimensions: [
                    ("name", job.name),
                    ("queue", self.queueName),
                ],
                preferredDisplayUnit: .seconds
            ).recordSeconds(jobQueuedDuration)
        }
    }

    /// Handle job and pass it onto next handler
    ///
    /// - Parameters:
    ///   - job: Job instance
    ///   - context: Job context
    ///   - next: Next handler
    /// - Throws:
    @inlinable
    public func handleJob(
        job: any JobInstanceProtocol,
        context: JobExecutionContext,
        next: (any JobInstanceProtocol, JobExecutionContext) async throws -> Void
    ) async throws {
        let startTime = DispatchTime.now().uptimeNanoseconds
        Meter(
            label: Self.meterLabel,
            dimensions: [
                ("status", JobStatus.processing.rawValue),
                ("name", job.name),
                ("queue", self.queueName),
            ]
        ).increment()
        defer {
            Meter(
                label: Self.meterLabel,
                dimensions: [
                    ("status", JobStatus.processing.rawValue),
                    ("name", job.name),
                    ("queue", self.queueName),
                ]
            ).decrement()
        }

        try await next(job, context)
        self.updateSuccessfulMetrics(for: job.name, startTime: startTime)
    }

    /// Job has completed or failed and if it failed will not be retried
    ///
    /// - Parameters:
    ///   - job: Job instance
    ///   - result: Result of completing job
    ///   - context: Job queue context
    public func onCompletedJob(job: any JobInstanceProtocol, result: Result<Void, any Error>, context: JobCompletedQueueContext) async {
        switch result {
        case .failure(let error):
            self.updateFailedMetrics(for: job.name, error: error)
        case .success:
            // success was recorded in handleJob
            break
        }
    }

    @usableFromInline
    func updateSuccessfulMetrics(
        for name: String,
        startTime: UInt64
    ) {
        let jobStatus: JobStatus = .succeeded
        let dimensions: [(String, String)] = [
            ("name", name),
            ("status", jobStatus.rawValue),
            ("queue", self.queueName),
        ]

        // Calculate job execution time
        Timer(
            label: Self.timerLabel,
            dimensions: dimensions,
            preferredDisplayUnit: .seconds
        ).recordNanoseconds(DispatchTime.now().uptimeNanoseconds - startTime)

        // Increment job counter base on status
        Counter(
            label: Self.counterLabel,
            dimensions: dimensions
        ).increment()
    }

    @usableFromInline
    func updateFailedMetrics(
        for name: String,
        error: Error
    ) {
        let jobStatus: JobStatus =
            if error is CancellationError {
                .cancelled
            } else {
                .failed
            }

        let dimensions: [(String, String)] = [
            ("name", name),
            ("status", jobStatus.rawValue),
            ("queue", self.queueName),
        ]

        // Increment job counter base on status
        Counter(
            label: Self.counterLabel,
            dimensions: dimensions
        ).increment()
    }

    @usableFromInline
    func updateMetricsForRetry(for name: String) {
        Counter(
            label: Self.counterLabel,
            dimensions: [("name", name), ("status", JobStatus.retried.rawValue), ("queue", self.queueName)]
        ).increment()
    }
}

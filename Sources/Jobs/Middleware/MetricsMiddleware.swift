//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2025 the Hummingbird authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See hummingbird/CONTRIBUTORS.txt for the list of Hummingbird authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Dispatch
import Metrics

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Add publishing of Metrics to a job queue
public struct MetricsJobMiddleware: JobMiddleware {
    public init() {}

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
    ///   - parameters: Job parameters
    ///   - jobInstanceID: Job instance identifier
    @inlinable
    public func onPushJob<Parameters: JobParameters>(parameters: Parameters, jobInstanceID: String) async {
        Meter(
            label: Self.meterLabel,
            dimensions: [
                ("status", JobStatus.queued.rawValue),
                ("name", Parameters.jobName),
            ]
        ).increment()
    }

    /// Job has been popped off the queue and decoded (with decode errors reported)
    ///
    /// - Parameters:
    ///   - result: Result of popping the job from the queue (Either job instance or error)
    ///   - jobInstanceID: Job instance identifer
    @inlinable
    public func onPopJob(result: Result<any JobInstanceProtocol, JobQueueError>, jobInstanceID: String) async {

        switch result {
        case .failure(let error):
            var counterDimensions: [(String, String)] = [
                ("reason", error.code.description)
            ]

            if let jobName = error.jobName {
                let jobNameDimension = ("name", jobName)
                counterDimensions.append(jobNameDimension)

                Meter(
                    label: Self.meterLabel,
                    dimensions: [
                        ("status", JobStatus.queued.rawValue),
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
                ]
            ).decrement()

            // Calculate wait time from queued to processing
            let jobQueuedDuration = Date.now.timeIntervalSince(job.queuedAt)
            Timer(
                label: Self.queuedTimerLabel,
                dimensions: [("name", job.name)],
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
        context: JobContext,
        next: (any JobInstanceProtocol, JobContext) async throws -> Void
    ) async throws {
        let startTime = DispatchTime.now().uptimeNanoseconds
        Meter(
            label: Self.meterLabel,
            dimensions: [
                ("status", JobStatus.processing.rawValue),
                ("name", job.name),
            ]
        ).increment()
        defer {
            Meter(
                label: Self.meterLabel,
                dimensions: [
                    ("status", JobStatus.processing.rawValue),
                    ("name", job.name),
                ]
            ).decrement()
        }

        do {
            try await next(job, context)
            self.updateMetrics(for: job.name, startTime: startTime)
        } catch let error as CancellationError {
            self.updateMetrics(
                for: job.name,
                startTime: startTime,
                error: error
            )
            throw error
        } catch {
            if !job.shouldRetry(error: error) {
                self.updateMetrics(
                    for: job.name,
                    startTime: startTime,
                    error: error
                )
            } else {
                // Guard against negative queue values, this is needed because we call
                // the job queue directly in the retrying step
                Meter(
                    label: Self.meterLabel,
                    dimensions: [
                        ("status", JobStatus.queued.rawValue),
                        ("name", job.name),
                    ]
                ).increment()

                self.updateMetrics(
                    for: job.name,
                    startTime: startTime,
                    retrying: true
                )

            }
            throw error
        }
    }

    /// Update job metrics
    /// - Parameters:
    ///   - name: String Job name
    ///   - startTime: UInt64 when the job started
    ///   - error: Error? job error
    ///   - retrying: Bool if the job is being retried
    ///
    @usableFromInline
    func updateMetrics(
        for name: String,
        startTime: UInt64,
        error: Error? = nil,
        retrying: Bool = false
    ) {
        if retrying {
            Counter(
                label: Self.counterLabel,
                dimensions: [("name", name), ("status", JobStatus.retried.rawValue)]
            ).increment()
            return
        }

        let jobStatus: JobStatus =
            if let error {
                if error is CancellationError {
                    .cancelled
                } else {
                    .failed
                }
            } else {
                .succeeded
            }

        let dimensions: [(String, String)] = [
            ("name", name),
            ("status", jobStatus.rawValue),
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
}

//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2024 the Hummingbird authors
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

/// OTEL labels and dimensions
internal enum JobMetricsHelper {
    /// Metrics label
    static let metricsLabel: String = "swift.jobs"
    /// Meter label for Processsing, Queued, Failed and Completed
    static let meterLabel: String = "swift.jobs.meter"
    /// Meter label for discarded jobs
    static let discardedMeter: String = "swift.jobs.discarded"
    /// Used for the histogram which can be useful to see by job status
    enum JobStatus: String, Codable, Sendable {
        case queued
        case processing
        case retried
        case cancelled
        case failed
        case succeeded
        case completed
    }

    /// Update job metrics
    /// - Parameters:
    ///   - name: String Job name
    ///   - jobID: String is only used for the completed meter
    ///   - startTime: UInt64 when the job started
    ///   - error: Error? job error
    ///   - retrying: Bool if the job is being retried
    ///
    static func updateMetrics(
        for name: String,
        jobID: String,
        startTime: UInt64,
        error: Error? = nil,
        retrying: Bool = false
    ) {
        // This meter can be used to display total job
        // Or decrement processing vector in Prometheus UI or Grafana
        // with something like count(swif_jobs_meter{status="processing"}
        // unless on(jobID) (swif_jobs_meter{status="queued"})
        // or (swif_jobs_meter{status="completed")) or vector(0)
        Meter(label: JobMetricsHelper.meterLabel, dimensions: [
            ("status", JobMetricsHelper.JobStatus.completed.rawValue),
            ("jobID", jobID),
        ]).increment()

        if retrying {
            Counter(
                label: Self.metricsLabel,
                dimensions: [("name", name), ("status", JobStatus.retried.rawValue)]
            ).increment()
            return
        }

        let jobStatus: JobStatus = if let error {
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
            label: "\(Self.metricsLabel).duration",
            dimensions: dimensions,
            preferredDisplayUnit: .seconds
        ).recordNanoseconds(DispatchTime.now().uptimeNanoseconds - startTime)

        // Increment job counter base on status
        Counter(
            label: Self.metricsLabel,
            dimensions: dimensions
        ).increment()
    }
}

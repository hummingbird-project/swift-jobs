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
    /// Counter label
    static let counterLabel: String = "swift.jobs"
    /// Job duration timer label
    static let timerLabel: String = "swift.jobs.duration"
    /// Job queued timer label
    static let queuedTimerLabel: String = "swift.jobs.queued.duration"
    /// Meter label for Processsing, Queued
    static let meterLabel: String = "swift.jobs.meter"
    /// Counter label for discarded jobs
    static let discardedCounter: String = "swift.jobs.discarded"
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
    ///   - startTime: UInt64 when the job started
    ///   - error: Error? job error
    ///   - retrying: Bool if the job is being retried
    ///
    static func updateMetrics(
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

//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2021-2025 the Hummingbird authors
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

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Job queue protocol.
///
/// Defines how to push and pop job data off a queue
public protocol JobQueueDriver: AsyncSequence, Sendable where Element == JobQueueResult<JobID> {
    associatedtype JobID: CustomStringConvertible & Sendable
    associatedtype JobOptions: CoreJobOptions

    /// Wait until job queue is ready
    func waitUntilReady() async throws
    /// Register job definition with driver
    func registerJob<Parameters: Sendable & Codable>(_ job: JobDefinition<Parameters>)
    /// Push Job onto queue
    /// - Parameters
    ///   - jobRequest: Job Request
    ///   - options: JobOptions
    /// - Returns: Identifier of queued jobs
    @discardableResult func push<Parameters>(_ jobRequest: JobRequest<Parameters>, options: JobOptions) async throws -> JobID
    /// Retry an existing Job
    /// - Parameters
    ///   - id: Job instance ID
    ///   - jobRequest: Job Request
    ///   - options: JobOptions
    func retry<Parameters>(_ id: JobID, jobRequest: JobRequest<Parameters>, options: JobRetryOptions) async throws
    /// This is called to say job has finished processing and it can be deleted
    func finished(jobID: JobID) async throws
    /// This is called to say job has failed to run and should be put aside
    func failed(jobID: JobID, error: any Error) async throws
    /// stop serving jobs
    func stop() async
    /// shutdown queue
    func shutdownGracefully() async
}

extension JobQueueDriver {
    // default version of waitUntilReady doing nothing
    public func waitUntilReady() async throws {}
}

extension JobQueueDriver {
    func retry(_ jobID: JobID, job: some JobInstanceProtocol, attempt: Int, options: JobRetryOptions) async throws {
        let jobRequest = JobRequest(name: job.name, parameters: job.parameters, queuedAt: job.queuedAt, attempt: attempt)
        return try await self.retry(jobID, jobRequest: jobRequest, options: options)
    }
}

/// Type returned from iterating a JobQueueDriver
///
/// The `JobQueueResult`` can hold either a job instance or the error created when iterating
/// the job queue. If the error is returned in `JobQueueResult` then the job queue handler will
/// handle the error. If the job queue throws an error when it is iterated then the job queue
/// will throw that error.
public struct JobQueueResult<JobID: Sendable>: Sendable {
    public let id: JobID
    public let result: Result<any JobInstanceProtocol, JobQueueError>

    ///  Initialize JobQueueResult
    /// - Parameters:
    ///   - id: id of job
    ///   - result: Result (job instance or error)
    public init(id: JobID, result: Result<any JobInstanceProtocol, JobQueueError>) {
        self.id = id
        self.result = result
    }
}

/// Core JobOptions
public typealias CoreJobOptions = DelayUntilJobOptionsProtocol & FairnessJobOptionsProtocol

/// Protocol for FairnessJobOptions
public protocol FairnessJobOptionsProtocol {
    /// Fairness key for resource allocation (optional)
    var fairnessKey: String? { get }
    /// Fairness weight for this job type (higher = more resources)
    var fairnessWeight: Double { get }
    /// Required protocol initializer for JobOptionsProtocol compliance
    /// - Parameters:
    ///   - fairnessKey: Fairness key for resource allocation (optional)
    ///   - fairnessWeight: Fairness weight for this job type
    init(fairnessKey: String?, fairnessWeight: Double)
}

/// Protocol for DelayUntilJobOptions
public protocol DelayUntilJobOptionsProtocol: Sendable {
    /// When to execute the job
    var delayUntil: Date { get }
    /// Initialize JobOptionsProtocol
    /// - Parameters:
    ///   - delayUntil: When to execute the job
    init(delayUntil: Date)
}

/// Protocol for job queue drivers that support stride scheduling with dynamic fairness controls.
///
/// ## Overview
///
/// This protocol extends basic job queue functionality with fairness capabilities,
/// allowing for proportional resource allocation across different tenants or job types.
/// Implementations use stride scheduling algorithms to ensure fair execution while
/// supporting runtime weight adjustments.
///
/// ## Fairness Model
///
/// - **Fairness Keys**: String identifiers that group jobs for fair scheduling
/// - **Weights**: Numerical values that determine relative execution frequency
/// - **Pass Values**: Internal scheduling tokens assigned to jobs for ordering
/// - **Virtual Time**: Per-key counters that track fairness progression
///
/// ## Weight Override System
///
/// The protocol supports dynamic weight adjustments that take effect immediately
/// for new jobs while preserving fairness guarantees for jobs already in the queue.
///
/// ## Example Usage
///
/// ```swift
/// // Set up weight overrides for different tenant priorities
/// try await queue.setFairnessWeightOverride(key: "premium-tenant", weight: 10.0)
/// try await queue.setFairnessWeightOverride(key: "standard-tenant", weight: 1.0)
///
/// // Monitor fairness distribution
/// let stats = try await queue.getFairnessStats()
/// for (key, stat) in stats {
///     print("\(key): weight=\(stat.weight), virtualTime=\(stat.virtualTime)")
/// }
///
/// // Remove overrides to restore default behavior
/// try await queue.removeFairnessWeightOverride(key: "premium-tenant")
/// ```
public protocol FairnessCapableJobQueue {
    /// Sets a dynamic weight override for a fairness key.
    ///
    /// This method allows runtime adjustment of job execution frequency for a specific
    /// fairness key without requiring service restarts. Higher weights result in more
    /// frequent execution opportunities through smaller stride values.
    ///
    /// ## Behavior
    /// - Takes effect immediately for newly enqueued jobs
    /// - Existing jobs in the queue maintain their original pass values
    /// - Overrides persist until explicitly removed
    /// - Multiple calls with the same key update the existing override
    ///
    /// ## Weight Guidelines
    /// - Must be positive (typically > 0.001)
    /// - Relative ratios matter more than absolute values
    /// - 2.0 vs 1.0 provides roughly 2:1 execution ratio
    /// - Very large ratios may cause starvation concerns
    ///
    /// - Parameters:
    ///   - key: The fairness key to override
    ///   - weight: The new weight value (higher = more frequent execution)
    /// - Throws: Implementation-specific errors for invalid keys or weights
    func setFairnessWeightOverride(key: String, weight: Double) async throws

    /// Removes a dynamic weight override for a fairness key.
    ///
    /// This method restores the fairness key to use default weights from job options
    /// rather than any active override. The change affects newly enqueued jobs
    /// immediately while preserving existing jobs' scheduling positions.
    ///
    /// ## Behavior
    /// - Takes effect immediately for newly enqueued jobs
    /// - No-op if no override exists for the specified key
    /// - Existing jobs in the queue keep their original pass values
    /// - Restores job-level `fairnessWeight` as the authoritative source
    ///
    /// - Parameter key: The fairness key to restore to default behavior
    /// - Throws: Implementation-specific errors for invalid keys
    func removeFairnessWeightOverride(key: String) async throws

    /// Gets current fairness statistics for monitoring and debugging.
    ///
    /// This method provides visibility into the stride scheduling system's current
    /// state, including virtual time progression, effective weights, and job counts
    /// per fairness key.
    ///
    /// ## Use Cases
    /// - **Monitoring**: Track fairness distribution across tenants
    /// - **Debugging**: Investigate unexpected job execution patterns
    /// - **Capacity Planning**: Analyze load distribution and bottlenecks
    /// - **Override Verification**: Confirm weight override effects
    /// - **Alerting**: Detect fairness imbalances or starvation conditions
    ///
    /// ## Data Freshness
    /// Statistics reflect the current database state and may lag slightly behind
    /// the most recent job enqueuing/execution activity depending on implementation.
    ///
    /// - Returns: A dictionary mapping active fairness keys to their current statistics
    /// - Note: Only includes fairness keys that have been recently active
    /// - Throws: Implementation-specific errors for database access issues
    func getFairnessStats() async throws -> [String: FairnessStats]
}

/// Statistics for monitoring stride scheduling fairness per fairness key.
///
/// ## Overview
///
/// This structure provides insight into the current state of the stride scheduling
/// algorithm for a specific fairness key, including virtual time progression,
/// effective weights, and optional execution metrics.
///
/// ## Virtual Time Interpretation
///
/// Virtual time represents the scheduling "position" for a fairness key:
/// - Lower values indicate higher priority (jobs get selected sooner)
/// - Values advance by stride amounts: `stride = 1000 / max(weight, 0.001)`
/// - Keys with higher weights have slower virtual time progression
/// - Used to calculate pass values for newly enqueued jobs
///
/// ## Weight Sources
///
/// The effective weight reflects the final weight used for scheduling:
/// - Database overrides (from `setFairnessWeightOverride`) take precedence
/// - Falls back to job-level `fairnessWeight` values
/// - Minimum weight is typically clamped to prevent division by zero
///
/// ## Usage Example
///
/// ```swift
/// let stats = try await queue.getFairnessStats()
/// for (key, stat) in stats {
///     let ratio = stat.weight / 1.0  // Compare to baseline weight
///     print("\(key): \(ratio)x priority, virtual_time=\(stat.virtualTime)")
/// }
/// ```
public struct FairnessStats: Sendable, Codable {
    /// Current virtual time for this fairness key.
    ///
    /// This value represents the scheduling position and is used to assign
    /// pass values to newly enqueued jobs. Lower values result in higher
    /// priority job placement.
    public let virtualTime: Double

    /// Effective weight currently being used for this fairness key.
    ///
    /// This reflects the final weight after considering any active database
    /// overrides. Higher weights result in more frequent job execution through
    /// smaller stride values in the scheduling algorithm.
    public let weight: Double

    /// Number of jobs for this fairness key (implementation-specific).
    ///
    /// This field's meaning varies by implementation:
    /// - May represent pending jobs currently in the queue
    /// - May represent total jobs executed historically
    /// - May be `nil` if not tracked by the implementation
    public let executionCount: Int?

    /// Average wait time for jobs with this fairness key.
    ///
    /// This field represents the average time jobs spend waiting in the queue
    /// before execution. May be `nil` if not tracked by the implementation.
    public let averageWaitTime: TimeInterval?

    /// Creates fairness statistics for a specific fairness key.
    /// - Parameters:
    ///   - virtualTime: Current virtual time for this fairness key
    ///   - weight: Current effective weight for this fairness key
    ///   - executionCount: Number of jobs for this fairness key
    ///   - averageWaitTime: Average wait time for jobs with this fairness key
    public init(
        virtualTime: Double,
        weight: Double,
        executionCount: Int? = nil,
        averageWaitTime: TimeInterval? = nil
    ) {
        self.virtualTime = virtualTime
        self.weight = weight
        self.executionCount = executionCount
        self.averageWaitTime = averageWaitTime
    }
}

/// Options for retrying a job
public struct JobRetryOptions {
    /// When to execute the job
    public let delayUntil: Date
    /// Default init JobOptionsProtocol
    /// - Parameters:
    ///   - delayUntil: When to execute the job
    init(delayUntil: Date) {
        self.delayUntil = delayUntil
    }
}

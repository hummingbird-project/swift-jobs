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

import Collections
import NIOCore

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// In memory implementation of job queue driver. Stores job data in a circular buffer
public final class MemoryQueue: JobQueueDriver, CancellableJobQueue, ResumableJobQueue, FairnessCapableJobQueue {
    public typealias Element = JobQueueResult<JobID>
    public typealias JobID = UUID
    /// Job options
    public struct JobOptions: CoreJobOptions {
        /// When to execute the job
        public let delayUntil: Date
        /// Priority level (higher number = higher priority)
        public let priority: Int
        /// Fairness key for resource allocation (optional)
        public let fairnessKey: String?
        /// Fairness weight for this job type (higher = more resources)
        public let fairnessWeight: Double

        /// Requirement from `JobOptionsProtocol`
        /// - Parameters:
        ///   - delayUntil: When to execute the job
        public init(delayUntil: Date = Date.now) {
            self.delayUntil = delayUntil
            self.priority = 1
            self.fairnessKey = nil
            self.fairnessWeight = 1.0
        }
        /// Requirement from `JobOptionsProtocol`
        /// - Parameters:
        ///   - delayUntil: When to execute the job
        ///   - priority: Priority level (higher number = higher priority)
        ///   - fairnessKey: Fairness key for resource allocation (optional)
        ///   - fairnessWeight: Fairness weight for this job type (higher = more resources)
        public init(
            delayUntil: Date = Date.now,
            priority: Int = 1,
            fairnessKey: String? = nil,
            fairnessWeight: Double = 1.0
        ) {
            self.delayUntil = delayUntil
            self.priority = Swift.max(1, priority)
            self.fairnessKey = fairnessKey
            self.fairnessWeight = Swift.max(0.1, fairnessWeight)
        }

        /// Requirement from `JobOptionsProtocol`
        /// - Parameters:
        ///   - fairnessKey: Fairness key for resource allocation (optional)
        ///   - fairnessWeight: Fairness weight for this job type (higher = more resources)
        public init(fairnessKey: String?, fairnessWeight: Double) {
            self.fairnessKey = fairnessKey
            self.fairnessWeight = fairnessWeight
            self.priority = 1
            self.delayUntil = Date.now
        }

        /// Requirement from `JobOptionsProtocol`
        /// - Parameters:
        ///   - fairnessKey: Fairness key for resource allocation (optional)
        ///   - fairnessWeight: Fairness weight for this job type (higher = more resources)
        ///   - delayUntil: Date to delay the job until (optional)
        public init(fairnessKey: String?, fairnessWeight: Double, delayUntil: Date) {
            self.fairnessKey = fairnessKey
            self.fairnessWeight = fairnessWeight
            self.priority = 1
            self.delayUntil = delayUntil
        }

        /// Create job options with fairness key and weight
        /// - Parameters:
        ///   - key: Fairness key for resource allocation (optional)
        ///   - weight: Fairness weight for this job type (higher = more resources)
        ///   - priority: Priority of the job (higher = more resources)
        ///   - delayUntil: Date to delay the job until (optional)
        public static func fairness(
            key: String,
            weight: Double = 1.0,
            priority: Int = 1,
            delayUntil: Date = Date.now
        ) -> JobOptions {
            JobOptions(
                delayUntil: delayUntil,
                priority: priority,
                fairnessKey: key,
                fairnessWeight: weight
            )
        }
    }

    /// queue of jobs
    fileprivate let queue: Internal
    private let onFailedJob: @Sendable (JobID, any Error) -> Void
    private let jobRegistry: JobRegistry

    /// Initialise In memory job queue
    public init(onFailedJob: @escaping @Sendable (JobID, any Error) -> Void = { _, _ in }) {
        self.queue = .init()
        self.onFailedJob = onFailedJob
        self.jobRegistry = .init()
    }

    /// Stop queue serving more jobs
    public func stop() async {
        await self.queue.stop()
    }

    /// Shutdown queue
    public func shutdownGracefully() async {
        await self.queue.shutdown()
    }

    ///  Register job
    /// - Parameters:
    ///   - job: Job Definition
    public func registerJob<Parameters>(_ job: JobDefinition<Parameters>) {
        self.jobRegistry.registerJob(job)
    }

    /// Push job onto queue
    /// - Parameters:
    ///   - jobRequest: Job Request
    ///   - options: Job options
    /// - Returns: Job ID
    @discardableResult public func push<Parameters>(_ jobRequest: JobRequest<Parameters>, options: JobOptions) async throws -> JobID {
        let buffer = try self.jobRegistry.encode(jobRequest: jobRequest)
        return try await self.queue.push(buffer, options: options)
    }

    /// Retry an existing job
    /// - Parameters:
    ///   - id: Job ID
    ///   - jobRequest: Job Request
    ///   - options: Job options
    public func retry<Parameters>(_ id: JobID, jobRequest: JobRequest<Parameters>, options: JobRetryOptions) async throws {
        let buffer = try self.jobRegistry.encode(jobRequest: jobRequest)
        let options = JobOptions(delayUntil: options.delayUntil)
        try await self.queue.retry(id, buffer: buffer, options: options)
    }

    public func finished(jobID: JobID) async throws {
        await self.queue.clearPendingJob(jobID: jobID)
    }

    /// Set a dynamic weight override for a fairness key
    public func setFairnessWeightOverride(key: String, weight: Double) async throws {
        await queue.setFairnessWeightOverride(key: key, weight: weight)
    }

    /// Remove a dynamic weight override for a fairness key
    public func removeFairnessWeightOverride(key: String) async throws {
        await queue.removeFairnessWeightOverride(key: key)
    }

    public func failed(jobID: JobID, error: any Error) async throws {
        if await self.queue.getPendingJob(jobID: jobID) != nil {
            // Track completion for failed jobs too
            await self.queue.clearPendingJob(jobID: jobID)
            self.onFailedJob(jobID, error)
        }
    }

    public func cancel(jobID: JobID) async throws {
        await self.queue.cancelJob(jobID: jobID)
    }

    public func resume(jobID: JobID) async throws {
        await self.queue.resumeJob(jobID: jobID)
    }

    public func pause(jobID: JobID) async throws {
        await self.queue.pauseJob(jobID: jobID)
    }

    /// Get fairness statistics for monitoring
    /// Returns a dictionary mapping fairness keys to their fairness statistics
    public func getFairnessStats() async throws -> [String: FairnessStats] {
        await queue.getFairnessStats()
    }

    /// Internal actor managing the job queue
    fileprivate actor Internal {
        struct QueuedJob: Sendable {
            let id: JobID
            let jobBuffer: ByteBuffer
            let passValue: Int64
        }

        /// Stride scheduling implementation for fair resource allocation
        struct StrideScheduler {
            private static let strideFactor: Double = 1000.0
            private static let minWeight: Double = 0.001

            private var virtualTimeByKey: [String: Double] = [:]
            private var globalVirtualTime: Double = 0
            private var weightOverrides: [String: Double] = [:]
            private var nextJobID: Int = 0

            // Execution tracking for statistics
            private var executionCountByKey: [String: Int] = [:]
            private var totalWaitTimeByKey: [String: TimeInterval] = [:]
            private var jobStartTimes: [JobID: Date] = [:]

            mutating func recordExecution(fairnessKey: String?, executionTime: TimeInterval, weight: Double = 1.0) {
                // Update weight tracking - virtual time is updated when job is selected
                let key = fairnessKey ?? "default"
                updateWeight(fairnessKey: key, weight: weight)

                // Track execution count
                executionCountByKey[key, default: 0] += 1
            }

            // Track job start time for wait time calculation
            mutating func recordJobStart(jobId: JobID, fairnessKey: String?) {
                jobStartTimes[jobId] = Date()
            }

            // Track job completion and calculate wait time
            mutating func recordJobCompletion(jobId: JobID, fairnessKey: String?) {
                guard let startTime = jobStartTimes.removeValue(forKey: jobId) else { return }

                let key = fairnessKey ?? "default"
                let waitTime = Date().timeIntervalSince(startTime)
                totalWaitTimeByKey[key, default: 0] += waitTime
            }

            mutating func updateWeight(fairnessKey: String, weight: Double) {
                // Store weight for this fairness key
                weightOverrides[fairnessKey] = weight
            }

            mutating func setFairnessWeightOverride(key: String, weight: Double) {
                weightOverrides[key] = weight
            }

            mutating func removeFairnessWeightOverride(key: String) {
                weightOverrides.removeValue(forKey: key)
            }

            func getVirtualTime(for fairnessKey: String?, weight: Double) -> Double {
                let key = fairnessKey ?? "default"

                // Return existing virtual time, or global virtual time for new fairness keys
                return virtualTimeByKey[key, default: globalVirtualTime]
            }

            mutating func advanceVirtualTime(for fairnessKey: String?, weight: Double) {
                let key = fairnessKey ?? "default"

                // Get effective weight with overrides
                let effectiveWeight = getEffectiveWeight(key: key, baseWeight: weight)

                // Calculate stride (higher weight = lower stride = more frequent execution)
                let stride = Self.strideFactor / Swift.max(effectiveWeight, Self.minWeight)

                // Get current virtual time for this key, or start at global virtual time
                let currentVirtualTime = virtualTimeByKey[key, default: globalVirtualTime]

                // Advance virtual time for next job from this key
                virtualTimeByKey[key] = currentVirtualTime + stride

                // Update global virtual time to track progress (allows new fairness keys to join fairly)
                if !virtualTimeByKey.isEmpty {
                    let minVirtualTime = virtualTimeByKey.values.min() ?? globalVirtualTime
                    if minVirtualTime > globalVirtualTime {
                        globalVirtualTime = minVirtualTime
                    }
                }
            }

            mutating func getNextJobID() -> Int {
                nextJobID += 1
                return nextJobID
            }

            // Update global virtual time to prevent fairness keys from falling behind
            mutating func updateGlobalVirtualTime() {
                if !virtualTimeByKey.isEmpty {
                    globalVirtualTime = virtualTimeByKey.values.min() ?? globalVirtualTime
                }
            }

            private func getEffectiveWeight(key: String, baseWeight: Double) -> Double {
                weightOverrides[key] ?? Swift.max(baseWeight, Self.minWeight)
            }

            // Reset state - mainly for testing
            mutating func reset() {
                virtualTimeByKey.removeAll()
                globalVirtualTime = 0
                weightOverrides.removeAll()
                nextJobID = 0
                executionCountByKey.removeAll()
                totalWaitTimeByKey.removeAll()
                jobStartTimes.removeAll()
            }

            // Get fairness statistics for monitoring
            func getFairnessStats() -> [String: FairnessStats] {
                var stats: [String: FairnessStats] = [:]
                for (key, virtualTime) in virtualTimeByKey {
                    let weight = weightOverrides[key] ?? 1.0
                    let executionCount = executionCountByKey[key]

                    // Calculate average wait time
                    let averageWaitTime: TimeInterval?
                    if let totalWaitTime = totalWaitTimeByKey[key],
                        let execCount = executionCount,
                        execCount > 0
                    {
                        averageWaitTime = totalWaitTime / Double(execCount)
                    } else {
                        averageWaitTime = nil
                    }

                    stats[key] = FairnessStats(
                        virtualTime: virtualTime,
                        weight: weight,
                        executionCount: executionCount,
                        averageWaitTime: averageWaitTime
                    )
                }
                return stats
            }

            // Get current global virtual time
            func getCurrentGlobalVirtualTime() -> Double {
                globalVirtualTime
            }
        }

        var queue: Deque<(job: QueuedJob, options: JobOptions)>
        var pendingJobs: [JobID: (buffer: ByteBuffer, fairnessKey: String?)]
        var metadata: [String: (data: ByteBuffer, expires: Date)]
        var isStopped: Bool
        var strideScheduler = StrideScheduler()

        init() {
            self.queue = .init()
            self.isStopped = false
            self.pendingJobs = .init()
            self.metadata = .init()
        }

        func push(_ jobBuffer: ByteBuffer, options: JobOptions) throws -> JobID {
            let id = JobID()
            let passValue = calculatePassValue(for: options)
            self.queue.append((job: QueuedJob(id: id, jobBuffer: jobBuffer, passValue: passValue), options: options))
            return id
        }

        func retry(_ id: JobID, buffer: ByteBuffer, options: JobOptions) throws {
            self.clearPendingJob(jobID: id)
            let passValue = calculatePassValue(for: options)
            let _ = self.queue.append((job: QueuedJob(id: id, jobBuffer: buffer, passValue: passValue), options: options))
        }

        func clearPendingJob(jobID: JobID) {
            if let pendingJob = self.pendingJobs.removeValue(forKey: jobID) {
                // Track job completion for statistics
                strideScheduler.recordJobCompletion(jobId: jobID, fairnessKey: pendingJob.fairnessKey)
            }
        }

        func recordJobExecution(fairnessKey: String?, executionTime: TimeInterval, weight: Double = 1.0) {
            strideScheduler.recordExecution(fairnessKey: fairnessKey, executionTime: executionTime, weight: weight)
        }

        func getFairnessStats() -> [String: FairnessStats] {
            strideScheduler.getFairnessStats()
        }

        func setFairnessWeightOverride(key: String, weight: Double) {
            strideScheduler.setFairnessWeightOverride(key: key, weight: weight)
        }

        func removeFairnessWeightOverride(key: String) {
            strideScheduler.removeFairnessWeightOverride(key: key)
        }

        func advanceVirtualTime(for fairnessKey: String?, weight: Double) {
            strideScheduler.advanceVirtualTime(for: fairnessKey, weight: weight)
        }

        func cancelJob(jobID: JobID) {
            self.queue.removeAll(where: { $0.job.id == jobID })
        }

        func pauseJob(jobID: JobID) {
            let job = self.queue.first(where: { $0.job.id == jobID })
            if let job = job {
                self.pendingJobs[jobID] = (buffer: job.job.jobBuffer, fairnessKey: job.options.fairnessKey)
            }
            self.queue.removeAll(where: { $0.job.id == jobID })
        }

        func resumeJob(jobID: JobID) {
            if let pendingJob = self.pendingJobs[jobID] {
                // Create options for resumed job
                let resumeOptions = JobOptions(fairnessKey: pendingJob.fairnessKey, fairnessWeight: 1.0)
                let passValue = calculatePassValue(for: resumeOptions)
                self.queue.append((job: QueuedJob(id: jobID, jobBuffer: pendingJob.buffer, passValue: passValue), options: .init()))
            } else {
                print("Warning: attempted to resume job \(jobID) which is not pending")
            }
            self.clearPendingJob(jobID: jobID)
        }

        func getPendingJob(jobID: JobID) -> ByteBuffer? {
            let instance = self.pendingJobs[jobID]
            return instance?.buffer
        }

        func next() async throws -> QueuedJob? {
            var maxTimesToLoop = self.queue.count
            while true {
                if self.isStopped {
                    return nil
                }

                // Find the best job based on priority and fairness
                guard !queue.isEmpty else {
                    try await Task.sleep(for: .milliseconds(100))
                    continue
                }

                let bestJobIndex = findBestJobIndex()
                guard let index = bestJobIndex else {
                    // All jobs are delayed, wait and try again
                    try await Task.sleep(for: .milliseconds(100))
                    maxTimesToLoop -= 1
                    if maxTimesToLoop <= 0 {
                        maxTimesToLoop = self.queue.count
                    }
                    continue
                }

                let request = queue.remove(at: index)
                self.pendingJobs[request.job.id] = (
                    buffer: request.job.jobBuffer,
                    fairnessKey: request.options.fairnessKey
                )

                // Update fairness tracking when job starts execution
                if let fairnessKey = request.options.fairnessKey {
                    strideScheduler.updateWeight(fairnessKey: fairnessKey, weight: request.options.fairnessWeight)
                    // Track job start time (pass value was already calculated when job was queued)
                    strideScheduler.recordJobStart(jobId: request.job.id, fairnessKey: fairnessKey)
                } else {
                    strideScheduler.recordJobStart(jobId: request.job.id, fairnessKey: nil)
                }

                return request.job
            }
        }

        /// Find the index of the best job to execute based on priority, fairness, and timing
        /// Priority takes precedence, fairness applies within same priority level
        private func findBestJobIndex() -> Int? {
            let now = Date.now
            var bestIndex: Int? = nil
            var bestPriority = Int.min
            var bestPassValue = Int64.max
            var bestJobID = UInt64.max

            for (index, request) in queue.enumerated() {
                // Skip jobs that aren't ready yet
                guard request.options.delayUntil <= now else { continue }

                let priority = request.options.priority

                // Use stored pass value (calculated when job was queued)
                let passValue = request.job.passValue

                // Use deterministic hashing for consistent tie-breaking
                let jobID = DeterministicHasher.hash(request.job.id.uuidString)

                // Selection criteria: priority DESC, pass ASC, job_id ASC (same as PostgreSQL)
                let isBetter: Bool
                if priority > bestPriority {
                    isBetter = true
                } else if priority < bestPriority {
                    isBetter = false
                } else {
                    // Same priority - use pass value (lower pass = better)
                    if passValue < bestPassValue {
                        isBetter = true
                    } else if passValue > bestPassValue {
                        isBetter = false
                    } else {
                        // Same pass value - use deterministic tie-breaking
                        isBetter = jobID < bestJobID
                    }
                }

                if isBetter {
                    bestPriority = priority
                    bestPassValue = passValue
                    bestJobID = jobID
                    bestIndex = index
                }
            }

            return bestIndex
        }

        /// Calculate pass value for a job using stride scheduling (like PostgreSQL implementation)
        private func calculatePassValue(for options: JobOptions) -> Int64 {
            guard let fairnessKey = options.fairnessKey else {
                // Jobs without fairness key get pass = 0 (highest priority within their priority level)
                return 0
            }

            // Get base pass from minimum pass in queue (like PostgreSQL)
            let basePass = getMinimumPassValue()

            // Update weight and get current virtual time
            strideScheduler.updateWeight(fairnessKey: fairnessKey, weight: options.fairnessWeight)
            let currentVirtualTime = strideScheduler.getVirtualTime(for: fairnessKey, weight: options.fairnessWeight)

            // Calculate stride increment
            let strideFactor: Double = 1000.0
            let minWeight: Double = 0.001
            let effectiveWeight = Swift.max(options.fairnessWeight, minWeight)
            let increment = Swift.max(1, Int64(strideFactor / effectiveWeight))

            // Use max of basePass and calculated virtual time + increment
            let passValue = Swift.max(basePass, Int64(currentVirtualTime * 1000.0) + increment)

            // Advance virtual time for next job
            strideScheduler.advanceVirtualTime(for: fairnessKey, weight: options.fairnessWeight)

            return passValue
        }

        /// Get the minimum pass value from jobs in the queue (like PostgreSQL implementation)
        private func getMinimumPassValue() -> Int64 {
            let now = Date.now
            var minPass: Int64 = 0

            for (_, request) in queue.enumerated() {
                // Only consider jobs that are ready to run
                guard request.options.delayUntil <= now else { continue }

                if minPass == 0 || request.job.passValue < minPass {
                    minPass = request.job.passValue
                }
            }

            return minPass
        }

        func stop() {
            self.isStopped = true
        }

        func shutdown() {
            assert(self.pendingJobs.count == 0)
            self.isStopped = true
        }

        func getMetadata(_ key: String) -> ByteBuffer? {
            self.metadata[key]?.data
        }

        func setMetadata(key: String, value: NIOCore.ByteBuffer) {
            self.metadata[key] = (data: value, expires: .distantFuture)
        }

        func acquireMetadataLock(key: String, id: ByteBuffer, expiresIn: TimeInterval) async -> Bool {
            guard let lock = self.metadata[key] else {
                self.metadata[key] = (data: id, expires: .now + expiresIn)
                return true
            }
            if lock.data == id {
                self.metadata[key]!.expires = .now + expiresIn
                return true
            } else if lock.expires < .now {
                self.metadata[key] = (data: id, expires: .now + expiresIn)
                return true
            } else {
                return false
            }
        }

        func releaseMetadataLock(key: String, id: ByteBuffer) async {
            if self.metadata[key]?.data == id {
                self.metadata[key] = nil
            }
        }
    }
}

extension MemoryQueue: JobMetadataDriver {
    public func getMetadata(_ key: String) async -> ByteBuffer? {
        await self.queue.getMetadata(key)
    }

    public func setMetadata(key: String, value: ByteBuffer) async {
        await self.queue.setMetadata(key: key, value: value)
    }

    public func acquireLock(key: String, id: ByteBuffer, expiresIn: TimeInterval) async -> Bool {
        await self.queue.acquireMetadataLock(key: key, id: id, expiresIn: expiresIn)
    }

    public func releaseLock(key: String, id: ByteBuffer) async {
        await self.queue.releaseMetadataLock(key: key, id: id)
    }
}

extension MemoryQueue {
    public struct AsyncIterator: AsyncIteratorProtocol {
        fileprivate let queue: Internal
        fileprivate let jobRegistry: JobRegistry

        public mutating func next() async throws -> Element? {
            guard let queuedJob = try await self.queue.next() else { return nil }
            do {
                let jobInstance = try self.jobRegistry.decode(queuedJob.jobBuffer)
                return .init(id: queuedJob.id, result: .success(jobInstance))
            } catch let error as JobQueueError {
                return .init(id: queuedJob.id, result: .failure(error))
            }
        }
    }

    public func makeAsyncIterator() -> AsyncIterator {
        .init(queue: self.queue, jobRegistry: self.jobRegistry)
    }
}

extension JobQueueDriver where Self == MemoryQueue {
    /// Return In memory driver for Job Queue
    /// - Parameters:
    ///   - onFailedJob: Closure called when a job fails
    public static var memory: MemoryQueue {
        .init()
    }
}

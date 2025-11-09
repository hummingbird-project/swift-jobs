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
    public struct JobOptions: JobOptionsProtocol {
        /// When to execute the job
        public let delayUntil: Date
        /// Priority level (higher number = higher priority)
        public let priority: Int
        /// Fairness key for resource allocation (optional)
        public let fairnessKey: String?
        /// Fairness weight for this job type (higher = more resources)
        public let fairnessWeight: Double

        /// Requirement from `JobOptionsProtocol`
        public init() {
            self.delayUntil = Date.now
            self.priority = 1
            self.fairnessKey = nil
            self.fairnessWeight = 1.0
        }

        public init(delayUntil: Date?) {
            self.delayUntil = delayUntil ?? Date.now
            self.priority = 1
            self.fairnessKey = nil
            self.fairnessWeight = 1.0
        }

        public init(
            delayUntil: Date? = nil,
            priority: Int = 1,
            fairnessKey: String? = nil,
            fairnessWeight: Double = 1.0
        ) {
            self.delayUntil = delayUntil ?? Date.now
            self.priority = Swift.max(1, priority)
            self.fairnessKey = fairnessKey
            self.fairnessWeight = Swift.max(0.1, fairnessWeight)
        }

        public init(fairnessKey: String?, fairnessWeight: Double) {
            self.fairnessKey = fairnessKey
            self.fairnessWeight = fairnessWeight
            self.priority = 1
            self.delayUntil = Date.now
        }

        /// Create job options with fairness key and weight
        public static func fairness(
            key: String,
            weight: Double = 1.0,
            priority: Int = 1,
            delayUntil: Date? = nil
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
        if await self.queue.clearAndReturnPendingJob(jobID: jobID) != nil {
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

    /// Internal actor managing the job queue
    fileprivate actor Internal {
        struct QueuedJob: Sendable {
            let id: JobID
            let jobBuffer: ByteBuffer
        }

        /// Fairness tracking for job execution
        struct StrideScheduler {
            private static let strideFactor: Double = 1000.0
            private static let minWeight: Double = 0.001

            private var virtualTimeByKey: [String: Double] = [:]
            private var globalVirtualTime: Double = 0
            private var weightOverrides: [String: Double] = [:]
            private var nextJobID: Int = 0

            mutating func recordExecution(fairnessKey: String?, executionTime: TimeInterval, weight: Double = 1.0) {
                // Just update weight tracking - virtual time is updated when job is selected
                let key = fairnessKey ?? "default"
                updateWeight(fairnessKey: key, weight: weight)
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

                // Get current virtual time for this key, or start at global minimum
                return virtualTimeByKey[key, default: globalVirtualTime]
            }

            mutating func advanceVirtualTime(for fairnessKey: String?, weight: Double) {
                let key = fairnessKey ?? "default"

                // Get effective weight with overrides
                let effectiveWeight = getEffectiveWeight(key: key, baseWeight: weight)

                // Calculate stride (higher weight = lower stride = more frequent execution)
                let stride = Self.strideFactor / Swift.max(effectiveWeight, Self.minWeight)

                // Get current virtual time for this key, or start at global minimum
                let currentVirtualTime = virtualTimeByKey[key, default: globalVirtualTime]

                // Advance virtual time for next job from this key
                virtualTimeByKey[key] = currentVirtualTime + stride

                // Update global virtual time to minimum of all keys (prevents infinite drift)
                globalVirtualTime = virtualTimeByKey.values.min() ?? globalVirtualTime
            }

            mutating func getNextJobID() -> Int {
                nextJobID += 1
                return nextJobID
            }

            private func getEffectiveWeight(key: String, baseWeight: Double) -> Double {
                weightOverrides[key] ?? Swift.max(baseWeight, Self.minWeight)
            }

            // Reset state - mainly for testing
            mutating func reset() {
                virtualTimeByKey.removeAll()
                globalVirtualTime = 0
                nextJobID = 0
            }
        }

        var queue: Deque<(job: QueuedJob, options: JobOptions)>
        var pendingJobs: [JobID: ByteBuffer]
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
            self.queue.append((job: QueuedJob(id: id, jobBuffer: jobBuffer), options: options))
            return id
        }

        func retry(_ id: JobID, buffer: ByteBuffer, options: JobOptions) throws {
            self.clearPendingJob(jobID: id)
            let _ = self.queue.append((job: QueuedJob(id: id, jobBuffer: buffer), options: options))
        }

        func clearPendingJob(jobID: JobID) {
            self.pendingJobs[jobID] = nil
        }

        func recordJobExecution(fairnessKey: String?, executionTime: TimeInterval, weight: Double = 1.0) {
            strideScheduler.recordExecution(fairnessKey: fairnessKey, executionTime: executionTime, weight: weight)
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
            self.pendingJobs[jobID] = job?.job.jobBuffer
            self.queue.removeAll(where: { $0.job.id == jobID })
        }

        func resumeJob(jobID: JobID) {
            if let jobBuffer = self.pendingJobs[jobID] {
                self.queue.append((job: QueuedJob(id: jobID, jobBuffer: jobBuffer), options: .init()))
            } else {
                print("Warning: attempted to resume job \(jobID) which is not pending")
            }
            self.clearPendingJob(jobID: jobID)
        }

        func clearAndReturnPendingJob(jobID: JobID) -> ByteBuffer? {
            let instance = self.pendingJobs[jobID]
            self.pendingJobs[jobID] = nil
            return instance
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
                self.pendingJobs[request.job.id] = request.job.jobBuffer

                // Update fairness tracking when job starts execution
                if let fairnessKey = request.options.fairnessKey {
                    strideScheduler.updateWeight(fairnessKey: fairnessKey, weight: request.options.fairnessWeight)
                    // Advance virtual time for this key since we're selecting this job
                    self.advanceVirtualTime(for: fairnessKey, weight: request.options.fairnessWeight)
                }

                return request.job
            }
        }

        /// Find the index of the best job to execute based on priority, fairness, and timing
        private func findBestJobIndex() -> Int? {
            let now = Date.now
            var bestIndex: Int? = nil
            var bestVirtualTime = Double.infinity
            var bestJobID = Int.max

            for (index, request) in queue.enumerated() {
                // Skip jobs that aren't ready yet
                guard request.options.delayUntil <= now else { continue }

                // Priority override: higher priority jobs always go first
                // We use negative priority as virtual time to maintain lexicographic ordering
                let priorityVirtualTime = -Double(request.options.priority) * 10000.0

                // Get virtual time for fairness
                let fairnessVirtualTime = strideScheduler.getVirtualTime(
                    for: request.options.fairnessKey,
                    weight: request.options.fairnessWeight
                )

                // Combined virtual time (priority dominates, then fairness)
                let totalVirtualTime = priorityVirtualTime + fairnessVirtualTime

                // Use deterministic hashing for consistent tie-breaking across app restarts
                let jobID = DeterministicHasher.hash(request.job.id.uuidString)

                // Lexicographic comparison: (virtualTime, jobID)
                let isBetter = totalVirtualTime < bestVirtualTime || (totalVirtualTime == bestVirtualTime && jobID < bestJobID)

                if isBetter {
                    bestVirtualTime = totalVirtualTime
                    bestJobID = jobID
                    bestIndex = index
                }
            }

            return bestIndex
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

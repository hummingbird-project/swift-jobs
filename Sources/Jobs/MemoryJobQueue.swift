//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

import Collections
import NIOCore

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// In memory implementation of job queue driver. Stores job data in a circular buffer
public final class MemoryQueue: JobQueueDriver, CancellableJobQueue, ResumableJobQueue {
    public typealias Element = JobQueueResult<JobID>
    public typealias JobID = UUID
    /// Job options
    public struct JobOptions: JobOptionsProtocol {
        /// When to execute the job
        public let delayUntil: Date
        /// Requirement from `JobOptionsProtocol`
        public init() {
            self.delayUntil = Date.now
        }
        /// Requirement from `JobOptionsProtocol`
        /// - Parameters:
        ///   - delayUntil: When to execute the job
        public init(delayUntil: Date = .now) {
            self.delayUntil = delayUntil
        }
    }

    /// queue of jobs
    fileprivate let queue: Internal
    private let onFailedJob: @Sendable (JobID, any Error) -> Void
    private let jobRegistry: JobRegistry
    public let context: JobQueueContext

    /// Initialise In memory job queue
    public init(queueName: String = "default", onFailedJob: @escaping @Sendable (JobID, any Error) -> Void = { _, _ in }) {
        self.queue = .init()
        self.onFailedJob = onFailedJob
        self.jobRegistry = .init()
        self.context = JobQueueContext(workerID: UUID().uuidString, queueName: queueName, metadata: [:])
    }

    public func waitUntilReady() async throws {
        await self.queue.start()
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
        await self.queue.clearProcessingJob(jobID: jobID)
    }

    public func failed(jobID: JobID, error: any Error) async throws {
        if await self.queue.failJob(jobID: jobID) {
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
        var queue: Deque<(job: QueuedJob, options: JobOptions)>
        var processingJobs: [JobID: ByteBuffer]
        var pausedJobs: [JobID: ByteBuffer]
        var failedJobs: [JobID: ByteBuffer]
        var metadata: [String: (data: ByteBuffer, expires: Date)]
        var isStopped: Bool

        init() {
            self.queue = .init()
            self.isStopped = false
            self.processingJobs = .init()
            self.pausedJobs = .init()
            self.failedJobs = .init()
            self.metadata = .init()
        }

        func push(_ jobBuffer: ByteBuffer, options: JobOptions) throws -> JobID {
            let id = JobID()
            self.queue.append((job: QueuedJob(id: id, jobBuffer: jobBuffer), options: options))
            return id
        }

        func retry(_ id: JobID, buffer: ByteBuffer, options: JobOptions) throws {
            self.clearProcessingJob(jobID: id)
            let _ = self.queue.append((job: QueuedJob(id: id, jobBuffer: buffer), options: options))
        }

        func clearProcessingJob(jobID: JobID) {
            self.processingJobs[jobID] = nil
        }

        func cancelJob(jobID: JobID) {
            self.queue.removeAll(where: { $0.job.id == jobID })
        }

        func pauseJob(jobID: JobID) {
            let job = self.queue.first(where: { $0.job.id == jobID })
            self.pausedJobs[jobID] = job?.job.jobBuffer
            self.queue.removeAll(where: { $0.job.id == jobID })
        }

        func resumeJob(jobID: JobID) {
            if let jobBuffer = self.pausedJobs[jobID] {
                self.pausedJobs[jobID] = nil
                self.queue.append((job: QueuedJob(id: jobID, jobBuffer: jobBuffer), options: .init()))
            } else {
                print("Warning: attempted to resume job \(jobID) which is not pending")
            }
        }

        func failJob(jobID: JobID) -> Bool {
            let instance = self.processingJobs[jobID]
            self.clearProcessingJob(jobID: jobID)
            self.processingJobs[jobID] = nil
            self.failedJobs[jobID] = instance
            return instance != nil
        }

        func next() async throws -> QueuedJob? {
            var maxTimesToLoop = self.queue.count
            while true {
                if self.isStopped {
                    return nil
                }
                if let request = queue.popFirst() {
                    if request.options.delayUntil > Date.now {
                        self.queue.append(request)
                        maxTimesToLoop -= 1
                        if maxTimesToLoop > 0 {
                            continue
                        }
                    } else {
                        self.processingJobs[request.job.id] = request.job.jobBuffer
                        return request.job
                    }
                }
                try await Task.sleep(for: .milliseconds(100))
            }
        }

        func stop() {
            self.isStopped = true
        }

        func start() {
            self.isStopped = false
        }

        func shutdown() {
            assert(self.processingJobs.count == 0)
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

extension MemoryQueue {
    /// how to cleanup a job
    public struct JobCleanup: Sendable, Codable {
        enum RawValue: Codable {
            case doNothing
            case rerun
            case remove
        }
        let rawValue: RawValue

        /// Do nothing to jobs
        public static var doNothing: Self { .init(rawValue: .doNothing) }
        /// Add jobs back onto the pending queue
        public static var rerun: Self { .init(rawValue: .rerun) }
        /// Delete jobs
        public static var remove: Self { .init(rawValue: .remove) }
    }

    /// Cleanup job queues
    ///
    /// This function is used to re-run or delete jobs in a certain state. Failed, completed,
    /// cancelled and paused jobs can be pushed back into the pending queue to be re-run or removed.
    /// When called at startup in theory no job should be set to processing, or set to pending but
    /// not in the queue. but if your job server crashes these states are possible, so we also provide
    /// options to re-queue these jobs so they are run again.
    ///
    /// You can call `cleanup` with `failedJobs`, `completedJobs`, `cancelledJobs` or `pausedJobs` set
    /// to whatever you like at any point to re-queue failed jobs. Moving processing or pending jobs
    /// should only be done if you are certain there is nothing processing the job queue.
    ///
    /// - Parameters:
    ///   - pendingJobs: What to do with jobs tagged as pending
    ///   - processingJobs: What to do with jobs tagged as processing
    ///   - completedJobs: What to do with jobs tagged as completed
    ///   - failedJobs: What to do with jobs tagged as failed
    ///   - cancelledJobs: What to do with jobs tagged as cancelled
    ///   - pausedJobs: What to do with jobs tagged as cancelled
    /// - Throws:
    public func cleanup(
        processingJobs: JobCleanup = .doNothing,
        failedJobs: JobCleanup = .doNothing,
        pausedJobs: MemoryQueue.JobCleanup
    ) async throws {
        await self.queue.cleanup(processingJobs: processingJobs, failedJobs: failedJobs, pausedJobs: pausedJobs)
    }
}

extension MemoryQueue.Internal {
    func cleanup(
        processingJobs: MemoryQueue.JobCleanup,
        failedJobs: MemoryQueue.JobCleanup,
        pausedJobs: MemoryQueue.JobCleanup
    ) {
        cleanupQueue(processingJobs, queue: &self.processingJobs)
        cleanupQueue(failedJobs, queue: &self.failedJobs)
        cleanupQueue(pausedJobs, queue: &self.pausedJobs)
    }

    func cleanupQueue(_ cleanup: MemoryQueue.JobCleanup, queue: inout [MemoryQueue.JobID: ByteBuffer]) {
        switch cleanup.rawValue {
        case .remove:
            queue = [:]
        case .rerun:
            for job in queue {
                self.queue.append((QueuedJob(id: job.key, jobBuffer: job.value), .init()))
            }
        case .doNothing:
            break
        }
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

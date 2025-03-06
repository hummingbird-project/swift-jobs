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

import Collections
import NIOCore

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// In memory implementation of job queue driver. Stores job data in a circular buffer
public final class MemoryQueue: JobQueueDriver {
    public typealias Element = JobQueueResult<JobID>
    public typealias JobID = UUID
    /// Job options
    public struct JobOptions: JobOptionsProtocol, Sendable {
        /// When to execute the job
        public let delayUntil: Date

        public init(delayUntil: Date? = nil) {
            self.delayUntil = delayUntil ?? Date.now
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
    public func registerJob<Parameters: JobParameters>(_ job: JobDefinition<Parameters>) {
        self.jobRegistry.registerJob(job)
    }

    /// Push job onto queue
    /// - Parameters:
    ///   - jobRequest: Job Request
    ///   - options: Job options
    /// - Returns: Job ID
    @discardableResult public func push<Parameters: JobParameters>(_ jobRequest: JobRequest<Parameters>, options: JobOptions) async throws -> JobID {
        let buffer = try self.jobRegistry.encode(jobRequest: jobRequest)
        return try await self.queue.push(buffer, options: options)
    }

    /// Retry an existing job
    /// - Parameters:
    ///   - id: Job ID
    ///   - jobRequest: Job Request
    ///   - options: Job options
    public func retry<Parameters: JobParameters>(_ id: JobID, jobRequest: JobRequest<Parameters>, options: JobOptions) async throws {
        let buffer = try self.jobRegistry.encode(jobRequest: jobRequest)
        try await self.queue.retry(id, buffer: buffer, options: options)
    }

    public func finished(jobID: JobID) async throws {
        await self.queue.clearPendingJob(jobID: jobID)
    }

    public func failed(jobID: JobID, error: any Error) async throws {
        if await self.queue.clearAndReturnPendingJob(jobID: jobID) != nil {
            self.onFailedJob(jobID, error)
        }
    }

    public func getMetadata(_ key: String) async -> ByteBuffer? {
        await self.queue.getMetadata(key)
    }

    public func setMetadata(key: String, value: ByteBuffer) async {
        await self.queue.setMetadata(key: key, value: value)
    }

    /// Internal actor managing the job queue
    fileprivate actor Internal {
        struct QueuedJob: Sendable {
            let id: JobID
            let jobBuffer: ByteBuffer
        }
        var queue: Deque<(job: QueuedJob, options: JobOptions)>
        var pendingJobs: [JobID: ByteBuffer]
        var metadata: [String: ByteBuffer]
        var isStopped: Bool

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
                if let request = queue.popFirst() {
                    if request.options.delayUntil > Date.now {
                        self.queue.append(request)
                        maxTimesToLoop -= 1
                        if maxTimesToLoop > 0 {
                            continue
                        }
                    } else {
                        self.pendingJobs[request.job.id] = request.job.jobBuffer
                        return request.job
                    }
                }
                try await Task.sleep(for: .milliseconds(100))
            }
        }

        func stop() {
            self.isStopped = true
        }

        func shutdown() {
            assert(self.pendingJobs.count == 0)
            self.isStopped = true
        }

        func getMetadata(_ key: String) -> ByteBuffer? {
            self.metadata[key]
        }

        func setMetadata(key: String, value: NIOCore.ByteBuffer) {
            self.metadata[key] = value
        }
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

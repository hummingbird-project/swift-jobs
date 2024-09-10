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
import Foundation
import NIOCore

/// In memory implementation of job queue driver. Stores job data in a circular buffer
public final class MemoryQueue: JobQueueDriver {
    public typealias Element = QueuedJob<JobID>
    public typealias JobID = UUID

    /// queue of jobs
    fileprivate let queue: Internal
    private let onFailedJob: @Sendable (QueuedJob<JobID>, any Error) -> Void
    private let onRetryJob: @Sendable (QueuedJob<JobID>) -> Void

    /// Initialise In memory job queue
    public init(onFailedJob: @escaping @Sendable (QueuedJob<JobID>, any Error) -> Void = { _, _ in }) {
        self.queue = .init()
        self.onFailedJob = onFailedJob
        self.onRetryJob = { _ in }
    }

    /// Stop queue serving more jobs
    public func stop() async {
        await self.queue.stop()
    }

    /// Shutdown queue
    public func shutdownGracefully() async {
        await self.queue.shutdown()
    }

    /// Push job onto queue
    /// - Parameters:
    ///   - job: Job
    ///   - eventLoop: Eventloop to run process on (ignored in this case)
    /// - Returns: Queued job
    @discardableResult public func push(_ buffer: ByteBuffer, options: JobOptions) async throws -> JobID {
        return try await self.queue.push(buffer, options: options)
    }

    public func retry(jobId: JobID, buffer: ByteBuffer, options: JobOptions) async throws {
        await self.queue.updateJob(jobId, buffer: buffer, options: options)
        self.onRetryJob(.init(id: jobId, jobBuffer: buffer))
    }

    public func finished(jobId: JobID) async throws {
        await self.queue.clearPendingJob(jobId: jobId)
    }

    public func failed(jobId: JobID, error: any Error) async throws {
        if let job = await self.queue.clearAndReturnPendingJob(jobId: jobId) {
            self.onFailedJob(.init(id: jobId, jobBuffer: job), error)
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
        var queue: Deque<(job: QueuedJob<JobID>, options: JobOptions)>
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

        func updateJob(_ jobId: JobID, buffer: ByteBuffer, options: JobOptions) {
            self.clearPendingJob(jobId: jobId)
            self.queue.append((job: QueuedJob(id: jobId, jobBuffer: buffer), options: options))
        }

        func clearPendingJob(jobId: JobID) {
            self.pendingJobs[jobId] = nil
        }

        func clearAndReturnPendingJob(jobId: JobID) -> ByteBuffer? {
            let instance = self.pendingJobs[jobId]
            self.pendingJobs[jobId] = nil
            return instance
        }

        func next() async throws -> QueuedJob<JobID>? {
            while true {
                if self.isStopped {
                    return nil
                }
                if let request = queue.popFirst() {
                    if let date = request.options.delayUntil {
                        guard date <= Date.now else {
                            self.queue.append(request)
                            continue
                        }
                    }

                    self.pendingJobs[request.job.id] = request.job.jobBuffer
                    return request.job
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

        public mutating func next() async throws -> Element? {
            try await self.queue.next()
        }
    }

    public func makeAsyncIterator() -> AsyncIterator {
        .init(queue: self.queue)
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

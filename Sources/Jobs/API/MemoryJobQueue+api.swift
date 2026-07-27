//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

import DequeModule
import NIOCore

#if canImport(FoundationEssentials)
public import FoundationEssentials
#else
public import Foundation
#endif

@_spi(JobsAPI) extension MemoryQueue: JobsAPI {
    public func getJobs(maxNumber: Int, paginationToken: String?) async throws -> GetJobsResponse {
        let index = paginationToken.flatMap { Int($0) } ?? 0
        let jobs = await self.queue.getJobs(maxNumber: maxNumber, index: index)
        return try .init(
            paginationToken: jobs.count != maxNumber ? nil : "\(index + maxNumber)",
            jobs: jobs.map {
                let jobInstance = try self.jobRegistry.decode($0.buffer)
                return JobAPIMetadata(id: $0.id, name: jobInstance.name, createdAt: jobInstance.queuedAt, completedAt: nil, status: $0.status)
            }
        )
    }

    public func getJob(id: UUID) async throws -> GetJobResponse? {
        guard let job = await self.queue.getJob(id: id) else { return nil }
        let jobInstance = try self.jobRegistry.decode(job.buffer)
        return GetJobResponse(
            jobMetadata: .init(id: id, name: jobInstance.name, createdAt: jobInstance.queuedAt, completedAt: nil, status: job.status),
            jobParameters: job.buffer
        )
    }
}

extension MemoryQueue.Internal {
    func getJobs(maxNumber: Int, index: Int) async -> [(id: UUID, status: JobAPIMetadata.Status, buffer: ByteBuffer)] {
        var maxNumber = maxNumber
        var index = index
        var jobs: [(id: UUID, status: JobAPIMetadata.Status, buffer: ByteBuffer)] = []

        jobs.append(
            contentsOf: getJobs(from: self.queue.lazy.reversed().map { $0.job }, index: &index, max: &maxNumber).map {
                (id: $0.id, status: .pending, buffer: $0.jobBuffer)
            }
        )
        jobs.append(
            contentsOf: getJobs(
                from: self.processingJobs.lazy.reversed().map { .init(id: $0.key, jobBuffer: $0.value) },
                index: &index,
                max: &maxNumber
            ).map {
                (id: $0.id, status: .processing, buffer: $0.jobBuffer)
            }
        )
        jobs.append(
            contentsOf: getJobs(from: self.pausedJobs.lazy.reversed().map { .init(id: $0.key, jobBuffer: $0.value) }, index: &index, max: &maxNumber)
                .map {
                    (id: $0.id, status: .paused, buffer: $0.jobBuffer)
                }
        )
        jobs.append(
            contentsOf: getJobs(from: self.failedJobs.lazy.reversed().map { .init(id: $0.key, jobBuffer: $0.value) }, index: &index, max: &maxNumber)
                .map {
                    (id: $0.id, status: .failed, buffer: $0.jobBuffer)
                }
        )
        return jobs
    }

    func getJobs(from collection: some Collection<QueuedJob>, index: inout Int, max: inout Int) -> [QueuedJob] {
        if max == 0 {
            return []
        } else if index < collection.count {
            let count = min(max, collection.count - index)
            let startIndex = collection.index(collection.startIndex, offsetBy: index)
            let endIndex = collection.index(startIndex, offsetBy: count)
            let jobs = collection[startIndex..<endIndex].map { $0 }
            index = 0
            max -= count
            return jobs
        } else {
            index -= collection.count
            return []
        }
    }

    func getJob(id: UUID) async -> (status: JobAPIMetadata.Status, buffer: ByteBuffer)? {
        if let job = self.queue.first(where: { $0.job.id == id }) {
            return (.pending, job.job.jobBuffer)
        } else if let jobBuffer = self.processingJobs[id] {
            return (.processing, jobBuffer)
        } else if let jobBuffer = self.failedJobs[id] {
            return (.failed, jobBuffer)
        } else if let jobBuffer = self.pausedJobs[id] {
            return (.paused, jobBuffer)
        }
        return nil
    }
}

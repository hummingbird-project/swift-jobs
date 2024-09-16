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

import Foundation
import Logging
import Metrics
import NIOCore
import NIOFoundationCompat
import ServiceLifecycle

/// Job queue
///
/// Wrapper type to bring together a job queue implementation and a job queue
/// handler. Before you can push jobs onto a queue you should register it
/// with the queue via either ``registerJob(id:maxRetryCount:execute:)`` or
/// ``registerJob(_:)``.
public struct JobQueue<Queue: JobQueueDriver>: Service {
    /// underlying driver for queue
    public let queue: Queue
    let handler: JobQueueHandler<Queue>
    let initializationComplete: Trigger

    public init(_ queue: Queue, numWorkers: Int = 1, logger: Logger, options: JobQueueOptions = .init()) {
        self.queue = queue
        self.handler = .init(queue: queue, numWorkers: numWorkers, logger: logger, options: options)
        self.initializationComplete = .init()
    }

    ///  Push Job onto queue
    /// - Parameters:
    ///   - id: Job identifier
    ///   - parameters: parameters for the job
    /// - Returns: Identifier of queued job
    @discardableResult public func push<Parameters: Codable & Sendable>(
        id: JobIdentifier<Parameters>,
        parameters: Parameters,
        options: JobOptions = .init()
    ) async throws -> Queue.JobID {
        let buffer = try self.queue.encode(id: id, parameters: parameters)
        let jobName = id.name
        let id = try await self.queue.push(buffer, options: options)
        Meter(label: JobMetricsHelper.meterLabel, dimensions: [
            ("status", JobMetricsHelper.JobStatus.queued.rawValue),
            ("jobID", id.description),
        ]).increment()
        self.logger.debug(
            "Pushed Job",
            metadata: ["JobID": .stringConvertible(id), "JobName": .string(jobName)]
        )
        return id
    }

    ///  Register job type
    /// - Parameters:
    ///   - id: Job Identifier
    ///   - maxRetryCount: Maximum number of times job is retried before being flagged as failed
    ///   - execute: Job code
    public func registerJob<Parameters: Codable & Sendable>(
        id: JobIdentifier<Parameters>,
        maxRetryCount: Int = 0,
        execute: @escaping @Sendable (
            Parameters,
            JobContext
        ) async throws -> Void
    ) {
        self.handler.logger.info("Registered Job", metadata: ["JobName": .string(id.name)])
        let job = JobDefinition<Parameters>(id: id, maxRetryCount: maxRetryCount, execute: execute)
        self.registerJob(job)
    }

    ///  Register job type
    /// - Parameters:
    ///   - job: Job definition
    public func registerJob(_ job: JobDefinition<some Codable & Sendable>) {
        self.handler.registerJob(job)
    }

    ///  Run queue handler
    public func run() async throws {
        do {
            try await self.queue.onInit()
            self.initializationComplete.trigger()
        } catch {
            self.initializationComplete.failed(error)
        }
        try await self.handler.run()
    }

    var logger: Logger { self.handler.logger }
}

extension JobQueue {
    /// Get JobQueue metadata
    func getMetadata<Value: Codable>(_ key: JobMetadataKey<Value>) async throws -> Value? {
        guard let buffer = try await self.queue.getMetadata(key.name) else { return nil }
        return try JSONDecoder().decode(Value.self, from: buffer)
    }

    /// Set JobQueue metadata
    func setMetadata<Value: Codable>(key: JobMetadataKey<Value>, value: Value) async throws {
        let buffer = try JSONEncoder().encodeAsByteBuffer(value, allocator: ByteBufferAllocator())
        try await self.queue.setMetadata(key: key.name, value: buffer)
    }
}

extension JobQueue: CustomStringConvertible {
    public var description: String { "JobQueue<\(String(describing: Queue.self))>" }
}

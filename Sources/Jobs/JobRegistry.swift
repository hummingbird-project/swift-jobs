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

import NIOConcurrencyHelpers
import NIOCore

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Registry for job types
public final class JobRegistry: Sendable {
    public init() {
        self.builderTypeMap = .init([:])
    }
    ///  Encode job request to ByteBuffer
    /// - Parameter jobRequest: Job request
    /// - Returns: ByteBuffer containing job request JSON
    public func encode<Parameters: Codable & Sendable>(jobRequest: JobRequest<Parameters>) throws -> ByteBuffer {
        try JSONEncoder().encodeAsByteBuffer(jobRequest, allocator: ByteBufferAllocator())
    }

    ///  Decode job instance from ByteBuffer
    /// - Parameter buffer: Buffer containing job instance JSON
    /// - Throws: JobQueueError
    /// - Returns: Job instance
    public func decode(_ buffer: ByteBuffer) throws -> any JobInstanceProtocol {
        do {
            return try JSONDecoder().decode(AnyDecodableJob.self, from: buffer, userInfoConfiguration: self).job
        } catch let error as JobQueueError {
            throw error
        } catch {
            throw JobQueueError(code: .decodeJobFailed, jobName: nil, details: "\(error)")
        }
    }

    ///  Register job
    /// - Parameters:
    ///   - job: Job Definition
    public func registerJob<Parameters: JobParameters>(_ job: JobDefinition<Parameters>) {
        let builder: @Sendable (Decoder) throws -> any JobInstanceProtocol = { decoder in
            let data = try JobInstanceData<Parameters>(from: decoder)
            return try JobInstance<Parameters>(job: job, data: data)
        }
        self.builderTypeMap.withLockedValue {
            precondition($0[Parameters.jobName] == nil, "There is a job already registered under name \"\(Parameters.jobName)\"")
            $0[Parameters.jobName] = builder
        }
    }

    func decode(jobName: String, from decoder: Decoder) throws -> any JobInstanceProtocol {
        let jobDefinitionBuilder = try self.builderTypeMap.withLockedValue {
            guard let job = $0[jobName] else { throw JobQueueError(code: .unrecognisedJobId, jobName: jobName) }
            return job
        }
        do {
            return try jobDefinitionBuilder(decoder)
        } catch {
            throw JobQueueError(code: .decodeJobFailed, jobName: jobName, details: "\(error)")
        }
    }

    let builderTypeMap: NIOLockedValueBox<[String: @Sendable (Decoder) throws -> any JobInstanceProtocol]>
}

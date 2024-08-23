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

import Foundation
import NIOConcurrencyHelpers
import NIOCore

/// Registry for job types
struct JobRegistry: Sendable {
    ///  Register job
    /// - Parameters:
    ///   - id: Job Identifier
    ///   - maxRetryCount: Maximum number of times job is retried before being flagged as failed
    ///   - execute: Job code
    public func registerJob<Parameters: Codable & Sendable>(
        job: JobDefinition<Parameters>
    ) {
        let builder: @Sendable (Decoder) throws -> any JobInstanceProtocol = { decoder in
            let data = try JobInstanceData<Parameters>(from: decoder)
            return try JobInstance<Parameters>(job: job, data: data)
        }
        self.builderTypeMap.withLockedValue {
            precondition($0[job.id.name] == nil, "There is a job already registered under id \"\(job.id.name)\"")
            $0[job.id.name] = builder
        }
    }

    func decode(_ buffer: ByteBuffer) throws -> any JobInstanceProtocol {
        return try JSONDecoder().decode(AnyDecodableJob.self, from: buffer, userInfoConfiguration: self).job
    }

    func decode(jobName: String, from decoder: Decoder) throws -> any JobInstanceProtocol {
        let jobDefinitionBuilder = try self.builderTypeMap.withLockedValue {
            guard let job = $0[jobName] else { throw JobQueueError.unrecognisedJobId }
            return job
        }
        return try jobDefinitionBuilder(decoder)
    }

    let builderTypeMap: NIOLockedValueBox < [String: @Sendable (Decoder) throws -> any JobInstanceProtocol]> = .init([:])
}

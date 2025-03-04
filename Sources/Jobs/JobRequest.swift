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

import Foundation

/// Request to run job, pushed to job queue
public struct JobRequest<Parameters: JobParameters>: Encodable {
    /// Job details
    public let data: JobInstanceData<Parameters>

    init(
        parameters: Parameters,
        queuedAt: Date,
        attempts: Int,
        nextScheduledAt: Date? = nil
    ) {
        self.data = .init(
            parameters: parameters,
            queuedAt: queuedAt,
            attempts: attempts,
            nextScheduledAt: nextScheduledAt
        )
    }

    init(jobInstance: JobInstance<Parameters>, attempts: Int) {
        self.data = .init(
            parameters: jobInstance.parameters,
            queuedAt: jobInstance.queuedAt,
            attempts: attempts
        )
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: _JobCodingKey.self)
        let childEncoder = container.superEncoder(
            forKey: .init(stringValue: Parameters.jobName, intValue: nil)
        )
        try self.data.encode(to: childEncoder)
    }
}

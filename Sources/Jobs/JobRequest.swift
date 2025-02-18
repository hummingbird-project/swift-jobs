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

public struct JobRequest<Parameters: Codable & Sendable>: Encodable {
    public let id: JobIdentifier<Parameters>
    public let data: JobInstanceData<Parameters>

    init(
        id: JobIdentifier<Parameters>,
        parameters: Parameters,
        queuedAt: Date,
        attempts: Int
    ) {

        self.id = id
        self.data = .init(parameters: parameters, queuedAt: queuedAt, attempts: attempts)
    }

    init(jobInstance: JobInstance<Parameters>, attempts: Int) {
        self.id = jobInstance.id
        self.data = .init(parameters: jobInstance.parameters, queuedAt: jobInstance.queuedAt, attempts: attempts)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: _JobCodingKey.self)
        let childEncoder = container.superEncoder(
            forKey: .init(stringValue: self.id.name, intValue: nil)
        )
        try self.data.encode(to: childEncoder)
    }
}

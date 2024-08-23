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

/// Type used to encode a job
struct EncodabeJob<Parameters: Codable & Sendable>: Encodable, Sendable {
    let id: JobIdentifier<Parameters>
    let data: JobInstanceData<Parameters>

    public init(id: JobIdentifier<Parameters>, parameters: Parameters, queuedAt: Date) {
        self.id = id
        self.data = .init(parameters: parameters, queuedAt: queuedAt)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: _JobCodingKey.self)
        let childEncoder = container.superEncoder(
            forKey: .init(stringValue: self.id.name, intValue: nil))
        try self.data.encode(to: childEncoder)
    }
}

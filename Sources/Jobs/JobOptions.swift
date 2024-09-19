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

import struct Foundation.Date

/// Job options
public struct JobOptions: Sendable {
    /// When to execute the job
    public let delayUntil: Date?
    /// Job Priority, execute from lowest to highest
    /// 0 - 10
    public let priority: Int

    public init(delayUntil: Date? = nil, priority: Int = 10) {
        self.delayUntil = delayUntil
        self.priority = priority
    }
}

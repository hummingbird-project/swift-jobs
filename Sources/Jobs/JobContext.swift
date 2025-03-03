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

import Logging

import struct Foundation.Date

/// Context of running job
public struct JobContext {
    /// Job instance identifier
    public let jobInstanceID: String
    /// Logger
    public let logger: Logger
    /// Time when the job was scheduled
    /// to run or queued at
    public let queuedAt: Date
    /// Next time job is scheduled to run
    public let nextScheduledAt: Date?
}

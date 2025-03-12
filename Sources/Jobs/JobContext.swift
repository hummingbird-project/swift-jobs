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

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Context of running job
public struct JobExecutionContext: Sendable {
    /// Job instance identifier
    public let jobID: String
    /// Logger
    public let logger: Logger
    /// Time when the job was scheduled
    /// to run or queued at
    public let queuedAt: Date
    /// Next time job is scheduled to run
    public let nextScheduledAt: Date?
}

/// context for job being adding/removed from queue
public struct JobQueueContext {
    /// Job instance identifier
    public let jobID: String
}

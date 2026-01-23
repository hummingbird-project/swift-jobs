//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

import Jobs
import Logging

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Context of running workflow job
public struct WorkflowExecutionContext: Sendable {
    /// Workflow ID
    public let workflowID: String
    /// Job ID
    public let jobID: String
    /// Logger
    public let logger: Logger
    /// Attempt number for this job.
    /// Starts at 1 and increments for every retry until max
    public var attempt: Int
    /// Time when the job was scheduled
    /// to run or queued at
    public let queuedAt: Date
}

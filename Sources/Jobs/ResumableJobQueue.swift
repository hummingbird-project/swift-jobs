//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

/// Protocol for Job queue driver which implements resumable jobs
public protocol ResumableJobQueue {
    associatedtype JobID: CustomStringConvertible & Sendable
    /// Pauses a job
    /// - Parameters:
    ///  - jobID: Job id
    func pause(jobID: JobID) async throws
    /// Resumes a job
    /// - Parameters:
    ///  - jobID: Job id
    func resume(jobID: JobID) async throws
}

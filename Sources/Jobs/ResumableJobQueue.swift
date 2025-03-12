//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2025 the Hummingbird authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See hummingbird/CONTRIBUTORS.txt for the list of Hummingbird authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

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

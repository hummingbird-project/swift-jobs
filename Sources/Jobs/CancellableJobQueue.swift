//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

/// Protocol for Job queue driver which implement cancellable jobs
public protocol CancellableJobQueue {
    associatedtype JobID: CustomStringConvertible & Sendable

    /// Cancels a job
    /// - Parameters:
    ///  - jobID: Job id
    func cancel(jobID: JobID) async throws
}

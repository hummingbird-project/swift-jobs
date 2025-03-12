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

/// Protocol for Job queue driver which implement cancellable jobs
public protocol CancellableJobQueue {
    associatedtype JobID: CustomStringConvertible & Sendable

    /// Cancels a job
    /// - Parameters:
    ///  - jobID: Job id
    /// - Throws
    func cancel(jobID: JobID) async throws
}

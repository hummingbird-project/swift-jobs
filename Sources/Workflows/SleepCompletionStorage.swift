//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2026 the Hummingbird authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See hummingbird/CONTRIBUTORS.txt for the list of Hummingbird authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Jobs
import NIOCore
import Synchronization

/// Shared storage for sleep completions
final class SleepCompletionStorage<JobID: Sendable & CustomStringConvertible>: Sendable {
    private let completions: Mutex<[String: CheckedContinuation<Void, Error>]> = .init([:])
    private let scheduledJobs: Mutex<[WorkflowID: [JobID]]> = .init([:])

    func waitForSleep(sleepKey: String) async throws {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            completions.withLock { sleeps in
                sleeps[sleepKey] = continuation
            }
        }
    }

    func completeSleep(sleepKey: String, result: Result<Void, any Error>) {
        completions.withLock { sleeps in
            if let continuation = sleeps.removeValue(forKey: sleepKey) {
                switch result {
                case .success:
                    continuation.resume()
                case .failure(let error):
                    continuation.resume(throwing: error)
                }
            }
        }
    }

    func trackScheduledJob(workflowId: WorkflowID, jobId: JobID) {
        scheduledJobs.withLock { jobs in
            jobs[workflowId, default: []].append(jobId)
        }
    }

    func cancelPendingSleeps(workflowId: WorkflowID) {
        let workflowPrefix = "workflow_sleep:\(workflowId.value):"
        completions.withLock { sleeps in
            let keysToCancel = sleeps.keys.filter { $0.hasPrefix(workflowPrefix) }
            for key in keysToCancel {
                if let continuation = sleeps.removeValue(forKey: key) {
                    continuation.resume(throwing: WorkflowError.workflowCancelled(workflowId))
                }
            }
        }
    }

    func getScheduledJobs(workflowId: WorkflowID) -> [JobID] {
        scheduledJobs.withLock { jobs in
            jobs[workflowId] ?? []
        }
    }

    func clearScheduledJobs(workflowId: WorkflowID) {
        scheduledJobs.withLock { jobs in
            _ = jobs.removeValue(forKey: workflowId)
        }
    }
}

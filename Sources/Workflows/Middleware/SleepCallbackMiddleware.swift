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

/// Middleware to handle WorkflowDelayJob completions with callback
final class SleepCallbackMiddleware<JobID: Sendable & CustomStringConvertible>: JobMiddleware {
    let storage: SleepCompletionStorage<JobID>

    init(storage: SleepCompletionStorage<JobID>) {
        self.storage = storage
    }

    func onCompletedJob(job: any JobInstanceProtocol, result: Result<Void, any Error>, context: JobCompletedQueueContext) async {
        // Handle WorkflowDelayJob completions
        guard job.parameters is WorkflowDelayJob else { return }
        let delayJob = job.parameters as! WorkflowDelayJob
        storage.completeSleep(sleepKey: delayJob.sleepKey, result: result)
    }
}

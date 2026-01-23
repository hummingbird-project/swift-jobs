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

/// Middleware to handle ActivityExecutionJob completions with callback
final class ActivityCallbackMiddleware: JobMiddleware {
    let storage: ActivityCompletionStorage

    init(storage: ActivityCompletionStorage) {
        self.storage = storage
    }

    func onCompletedJob(job: any JobInstanceProtocol, result: Result<Void, any Error>, context: JobCompletedQueueContext) async {
        // Handle ActivityExecution completions
        guard job.parameters is ActivityExecutionJob else { return }
        let activityJob = job.parameters as! ActivityExecutionJob

        switch result {
        case .success:
            // Activity completed successfully - retrieve the actual result from storage
            storage.completeActivity(activityId: activityJob.activityId.value)
        case .failure(let error):
            storage.failActivity(activityId: activityJob.activityId.value, error: error)
        }
    }
}

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

import Dispatch

public struct JobEventMiddleware<Driver: JobEventDriver>: JobMiddleware {
    let eventQueue: JobEventQueue<Driver>
    let queueName: String

    init(_ eventQueue: JobEventQueue<Driver>, queueName: String) {
        self.eventQueue = eventQueue
        self.queueName = queueName
    }

    public func onPushJob<Parameters>(name: String, parameters: Parameters, context: JobPushQueueContext) async {
        if context.attempt > 1 {
            self.eventQueue.publishEvent(.retryJob(name: name, id: context.jobID, queueName: queueName))
        } else {
            self.eventQueue.publishEvent(.pushJob(name: name, id: context.jobID, queueName: queueName))
        }
    }

    public func onPopJob(result: Result<any JobInstanceProtocol, JobQueueError>, context: JobPopQueueContext) async {
        switch result {
        case .success(let job):
            self.eventQueue.publishEvent(.popJob(name: job.name, id: context.jobID, queueName: queueName))
        case .failure(_):
            // no event for failing to decode
            break
        }
    }

    public func handleJob(
        job: any JobInstanceProtocol,
        context: JobExecutionContext,
        next: (any JobInstanceProtocol, JobExecutionContext) async throws -> Void
    ) async throws {
        let startTime = DispatchTime.now().uptimeNanoseconds
        try await next(job, context)
        self.eventQueue.publishEvent(
            .successfulJob(
                name: job.name,
                id: context.jobID,
                queueName: self.queueName,
                processingTimeNanoseconds: DispatchTime.now().uptimeNanoseconds - startTime
            )
        )
    }

    public func onCompletedJob(job: any JobInstanceProtocol, result: Result<Void, any Error>, context: JobCompletedQueueContext) async {
        switch result {
        case .failure(let error):
            self.eventQueue.publishEvent(.failedJob(name: job.name, id: context.jobID, queueName: self.queueName, error: error))
        default:
            // success if dealt with in handle
            break
        }
    }
}

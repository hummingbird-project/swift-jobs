//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

public struct EventsJobMiddleware<Queue: JobEventsDriver>: JobMiddleware {
    let queue: Queue

    public init(jobQueue: Queue) {
        self.queue = jobQueue
    }

    /// Job has been pushed onto the queue
    ///
    /// - Parameters:
    ///   - name: Job name
    ///   - parameters: Job parameters
    ///   - context: Job queue context
    public func onPushJob<Parameters>(name: String, parameters: Parameters, context: JobPushQueueContext) async {
        await self.queue.publish(event: .init(type: .jobPushed, id: context.jobID, time: .now))
    }
    /// Handle job and pass it onto next handler
    ///
    /// - Parameters:
    ///   - job: Job instance
    ///   - context: Job execution context
    ///   - next: Next handler
    /// - Throws:
    public func handleJob(
        job: any JobInstanceProtocol,
        context: JobExecutionContext,
        next: (any JobInstanceProtocol, JobExecutionContext) async throws -> Void
    ) async throws {
        await self.queue.publish(event: .init(type: .jobStarted, id: context.jobID, time: .now))
        try await next(job, context)
    }
    /// Job has completed or failed and if it failed will not be retried
    ///
    /// - Parameters:
    ///   - job: Job instance
    ///   - result: Result of completing job
    ///   - context: Job queue context
    public func onCompletedJob(job: any JobInstanceProtocol, result: Result<Void, any Error>, context: JobCompletedQueueContext) async {
        switch result {
        case .success: await self.queue.publish(event: .init(type: .jobCompleted, id: context.jobID, time: .now))
        case .failure: await self.queue.publish(event: .init(type: .jobFailed, id: context.jobID, time: .now))
        }
    }

}

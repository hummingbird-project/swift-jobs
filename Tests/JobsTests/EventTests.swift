//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

import Foundation
import Logging
import ServiceLifecycle
import Synchronization
import Testing

@testable import Jobs

struct JobEventTests {
    @Test func testPushEvent() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testBasic"
            let value: Int
        }
        let logger = Logger(label: "JobsTests")
        let expectation = TestExpectation()
        let jobQueue = JobQueue(.memory, logger: logger) { queue in
            EventsJobMiddleware(jobQueue: queue)
        }
        let job = JobDefinition { (parameters: TestParameters, context) in
            context.logger.info("Parameters=\(parameters.value)")
            try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
            expectation.trigger()
        }
        jobQueue.registerJob(job)
        try await testJobQueue(JobQueueProcessor(queue: jobQueue, logger: logger)) {
            for i in 0..<10 {
                try await jobQueue.push(TestParameters(value: i))
            }

            try await jobQueue.queue.subscribe(eventType: .jobPushed, from: .now - 3600) { events in
                var iterator = events.makeAsyncIterator()
                for _ in 0..<10 {
                    _ = await iterator.next()
                }
            }
        }
    }
}

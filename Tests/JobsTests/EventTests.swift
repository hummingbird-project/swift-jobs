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
        let jobQueue = JobQueue(.memory, logger: logger) { queue in
            EventsJobMiddleware(jobQueue: queue)
        }
        let job = JobDefinition { (parameters: TestParameters, context) in
            context.logger.info("Parameters=\(parameters.value)")
            try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
        }
        jobQueue.registerJob(job)
        try await testJobQueue(jobQueue.processor()) {
            var ids: Set<String> = []
            for i in 0..<10 {
                let jobID = try await jobQueue.push(TestParameters(value: i))
                ids.insert(jobID.uuidString)
            }

            try await jobQueue.queue.subscribe(eventType: .jobPushed, from: .now - 3600) { events in
                var iterator = events.makeAsyncIterator()
                for _ in 0..<10 {
                    let event = try #require(await iterator.next())
                    #expect(event.type == .jobPushed)
                    let removedValue = ids.remove(event.id)
                    #expect(removedValue != nil)
                }
            }
        }
    }

    @Test func testCompletedEvent() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testFailedEvent"
            let value: Int
        }
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
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
            var ids: Set<String> = []
            for i in 0..<10 {
                let jobID = try await jobQueue.push(TestParameters(value: i))
                ids.insert(jobID.uuidString)
            }

            try await jobQueue.queue.subscribe(eventType: .jobCompleted, from: .now - 3600) { events in
                var iterator = events.makeAsyncIterator()
                for _ in 0..<10 {
                    let event = try #require(await iterator.next())
                    #expect(event.type == .jobCompleted)
                    let removedValue = ids.remove(event.id)
                    #expect(removedValue != nil)
                }
            }
        }
    }

    @Test func testFailedEvent() async throws {
        struct RandomError: Error {}
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let jobQueue = JobQueue(.memory, logger: logger) { queue in
            EventsJobMiddleware(jobQueue: queue)
        }
        let job = JobDefinition(
            name: "testCompletedEvent",
            retryStrategy: .exponentialJitter(maxAttempts: 3, maxBackoff: .milliseconds(50))
        ) { (parameters: Int, context) in
            context.logger.info("Parameters=\(parameters)")
            throw RandomError()
        }
        jobQueue.registerJob(job)
        try await testJobQueue(JobQueueProcessor(queue: jobQueue, logger: logger)) {
            let jobID = try await jobQueue.push("testCompletedEvent", parameters: 1)

            try await jobQueue.queue.subscribe(eventType: .jobAttemptFailed, from: .now - 3600) { events in
                var iterator = events.makeAsyncIterator()
                var event = try #require(await iterator.next())
                #expect(event.id == jobID.description)
                #expect(event.type == .jobAttemptFailed)
                #expect(event.parameters[.attempt] == .int(1))
                event = try #require(await iterator.next())
                #expect(event.id == jobID.description)
                #expect(event.type == .jobAttemptFailed)
                #expect(event.parameters[.attempt] == .int(2))
                event = try #require(await iterator.next())
                #expect(event.id == jobID.description)
                #expect(event.type == .jobAttemptFailed)
                #expect(event.parameters[.attempt] == .int(3))
            }
            try await jobQueue.queue.subscribe(eventType: .jobFailed, from: .now - 3600) { events in
                var iterator = events.makeAsyncIterator()
                let event = try #require(await iterator.next())
                #expect(event.id == jobID.description)
                #expect(event.type == .jobFailed)
            }
        }
    }
}

@Test func testEventsAssociatedWithID() async throws {
    struct RandomError: Error {}
    struct TestParameters: JobParameters {
        static let jobName = "testEventsAssociatedWithID"
        let value: Int
    }
    let logger = Logger(label: "JobsTests")
    let jobQueue = JobQueue(.memory, logger: logger) { queue in
        EventsJobMiddleware(jobQueue: queue)
    }
    let job = JobDefinition(
        retryStrategy: .exponentialJitter(maxAttempts: 3, maxBackoff: .milliseconds(50))
    ) { (parameters: TestParameters, context) in
        context.logger.info("Parameters=\(parameters.value)")
        if context.attempt == 1 {
            throw RandomError()
        }
        try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
    }
    jobQueue.registerJob(job)
    try await testJobQueue(jobQueue.processor()) {
        let jobID = try await jobQueue.push(TestParameters(value: 1)).description

        try await jobQueue.queue.subscribe(id: jobID, from: .now - 3600) { events in
            var iterator = events.makeAsyncIterator()
            var event = try #require(await iterator.next())
            #expect(event.id == jobID)
            #expect(event.type == .jobPushed)
            event = try #require(await iterator.next())
            #expect(event.id == jobID)
            #expect(event.type == .jobStarted)
            event = try #require(await iterator.next())
            #expect(event.id == jobID)
            #expect(event.type == .jobAttemptFailed)
            #expect(event.parameters[.attempt] == .int(1))
            event = try #require(await iterator.next())
            #expect(event.id == jobID)
            #expect(event.type == .jobPushed)
            event = try #require(await iterator.next())
            #expect(event.id == jobID)
            #expect(event.type == .jobStarted)
            event = try #require(await iterator.next())
            #expect(event.id == jobID)
            #expect(event.type == .jobCompleted)
        }
    }
}

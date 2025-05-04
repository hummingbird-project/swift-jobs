//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2024 the Hummingbird authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See hummingbird/CONTRIBUTORS.txt for the list of Hummingbird authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Logging
import ServiceLifecycle
import XCTest

@testable import Jobs

final class LeadershipElectorServiceTests: XCTestCase {
    func testMultipleSchedulerServicesWithJustOneAbleToScheduled() async throws {
        let (stream, source) = AsyncStream.makeStream(of: Int.self)

        struct LeadershipParameters: JobParameters {
            static let jobName = "LeadershipJob"
        }

        var logger = Logger(label: "jobs")
        logger.logLevel = .debug

        let jobQueue = JobQueue(MemoryQueue(), logger: logger)
        let jobQueue2 = JobQueue(MemoryQueue(), logger: logger)
        jobQueue.registerJob(parameters: LeadershipParameters.self) { _, context in
            XCTAssertNotNil(context.nextScheduledAt)
            XCTAssertGreaterThan(context.nextScheduledAt!, context.queuedAt)

            source.yield(1)
        }
        // job queue 2 will never run because it's not the leader
        // what's the best way to test this?
        jobQueue2.registerJob(parameters: LeadershipParameters.self) { _, context in
            XCTAssertNotNil(context.nextScheduledAt)
            XCTAssertGreaterThan(context.nextScheduledAt!, context.queuedAt)
            context.logger.debug("Hello from second queue")
        }
        // create schedule that ensures a job will be run in the next second
        let dateComponents = Calendar.current.dateComponents([.hour, .minute, .second], from: Date.now + 1)
        var jobSchedule = JobSchedule()
        jobSchedule.addJob(LeadershipParameters(), schedule: .everyMinute(second: dateComponents.second!))

        let elector = LeadershipElectorService(jobQueue: jobQueue, interval: .milliseconds(100))

        await withThrowingTaskGroup(of: Void.self) { group in
            let serviceGroup = await ServiceGroup(
                configuration: .init(
                    services: [
                        jobQueue,
                        jobQueue2,
                        jobSchedule.scheduler(on: jobQueue),
                        jobSchedule.scheduler(on: jobQueue2),
                        elector,
                    ],
                    logger: logger
                )
            )
            group.addTask {
                try await serviceGroup.run()
                let firstNodeIsLeader = await jobQueue.queue.isLeader()
                XCTAssertTrue(firstNodeIsLeader)
                let isLeader = await jobQueue2.queue.isLeader()
                XCTAssertFalse(isLeader)
            }
            let one = await stream.first { _ in true }
            XCTAssertEqual(one, 1)
            await serviceGroup.triggerGracefulShutdown()
        }
    }
}

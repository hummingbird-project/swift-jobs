//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

import Foundation
import Jobs
import Logging
import ServiceLifecycle
import Synchronization
import Testing

struct JobServiceTests {
    @Test func runJob() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "runJob"
            let value: Int
        }
        var logger = Logger(label: "runJob")
        logger.logLevel = .trace
        let jobService = JobService(.memory, logger: logger)
        async let _ = jobService.run()

        let (stream, cont) = AsyncStream.makeStream(of: Int.self)
        jobService.registerJob(parameters: TestParameters.self) { parameter, _ in
            cont.yield(parameter.value)
        }
        try await jobService.push(TestParameters(value: 4))
        let value = await stream.first { _ in true }
        #expect(value == 4)
    }

    @Test func runScheduledJob() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "runScheduledJob"
            let value: Int
        }
        var logger = Logger(label: "runScheduledJob")
        logger.logLevel = .trace
        var jobService = JobService(.memory, logger: logger)

        let (stream, cont) = AsyncStream.makeStream(of: Int.self)
        jobService.registerJob(parameters: TestParameters.self) { parameter, _ in
            cont.yield(parameter.value)
        }
        // create schedule that ensures a job will be run in the next second
        let dateComponents = Calendar.current.dateComponents([.hour, .minute, .second], from: Date.now + 1)
        jobService.addScheduledJob(TestParameters(value: 8), schedule: .everyMinute(second: dateComponents.second!))

        async let _ = jobService.run()
        let value = await stream.first { _ in true }
        #expect(value == 8)
    }
}

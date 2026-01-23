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

import Jobs
import Logging
import Testing
import Workflows

struct WorkflowTests {
    @Test func testBasicWorkflow() async throws {
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let (stream, cont) = AsyncStream.makeStream(of: Double.self)
        let jobQueue = JobQueue(.memory, logger: logger)
        let workflowName = jobQueue.registerWorkflow(name: "basic", input: String.self) {
            WorkflowJob(name: "convert-to-int") { (input: String, context) in
                print(input)
                return Int(input)!
            }
            WorkflowJob(name: "divide-by-two") { (input: Int, context) in
                print(input)
                return Double(input) / 2.0
            }
            WorkflowJob(name: "yield") { (input: Double, context) in
                cont.yield(input)
                return
            }
        }

        try await testJobQueue(JobQueueProcessor(queue: jobQueue, logger: logger)) {
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "25")
            let value = await stream.first { _ in true }
            #expect(value == 12.5)
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "3")
            let value2 = await stream.first { _ in true }
            #expect(value2 == 1.5)
        }
    }

    @Test func testWorkflowJobsDefinedOutsideWorkflow() async throws {
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let (stream, cont) = AsyncStream.makeStream(of: Double.self)
        let jobQueue = JobQueue(.memory, logger: logger)
        let convertToIntJob = WorkflowJob(name: "convert-to-int") { (input: String, context) in
            print(input)
            return Int(input)!
        }
        let yieldJob = WorkflowJob(name: "yield") { (input: Double, context) in
            cont.yield(input)
            return
        }
        let workflowName = jobQueue.registerWorkflow(name: "basic", input: String.self) {
            convertToIntJob
            WorkflowJob(name: "divide-by-two") { (input: Int, context) in
                print(input)
                return Double(input) / 2.0
            }
            yieldJob
        }

        try await testJobQueue(JobQueueProcessor(queue: jobQueue, logger: logger)) {
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "25")
            let value = await stream.first { _ in true }
            #expect(value == 12.5)
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "3")
            let value2 = await stream.first { _ in true }
            #expect(value2 == 1.5)
        }
    }

    @Test func testSingleJobWorkflow() async throws {
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let (stream, cont) = AsyncStream.makeStream(of: Int.self)
        let jobQueue = JobQueue(.memory, logger: logger)
        let workflowName = jobQueue.registerWorkflow(name: "basic", input: String.self) {
            WorkflowJob(name: "convert-to-int") { (input: String, context) in
                cont.yield(Int(input)!)
                return
            }
        }

        try await testJobQueue(JobQueueProcessor(queue: jobQueue, logger: logger)) {
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "25")
            let value = await stream.first { _ in true }
            #expect(value == 25)
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "3")
            let value2 = await stream.first { _ in true }
            #expect(value2 == 3)
        }
    }
}

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
        let workflow = WorkflowBuilder()
            .addStep(name: "convert-to-int", input: String.self) { input, context in
                print(input)
                return Int(input)!
            }
            .addStep(name: "divide-by-two") { (input, context) in
                print(input)
                return Double(input) / 2.0
            }
            .addStep(name: "yield") { input, _ in
                cont.yield(input)
                return
            }
        let workflowName = jobQueue.registerWorkflow(name: "basic", workflow: workflow)
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
        let workflow = WorkflowBuilder()
            .addStep(
                WorkflowStep(name: "convert-to-int") { (input: String, context) in
                    cont.yield(Int(input)!)
                    return
                }
            )
        let workflowName = jobQueue.registerWorkflow(name: "basic", workflow: workflow)

        try await testJobQueue(JobQueueProcessor(queue: jobQueue, logger: logger)) {
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "25")
            let value = await stream.first { _ in true }
            #expect(value == 25)
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "3")
            let value2 = await stream.first { _ in true }
            #expect(value2 == 3)
        }
    }

    @Test func testIfConditionWorkflow() async throws {
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let (stream, cont) = AsyncStream.makeStream(of: Int.self)
        let jobQueue = JobQueue(.memory, logger: logger)
        let workflow = WorkflowBuilder()
            .addStep(
                WorkflowStep(name: "convert-to-int") { (input: String, context) in
                    Int(input)!
                }
            )
            .if {
                $0 > 10
            } then: { builder in
                builder.addStep(
                    WorkflowStep(name: "big") { (input: Int, context) in
                        input + 10
                    }
                )
            }
            .addStep(
                WorkflowStep(name: "yield") { (input: Int, context) in
                    cont.yield(input)
                    return
                }
            )
        let workflowName = jobQueue.registerWorkflow(name: "test", workflow: workflow)
        try await testJobQueue(JobQueueProcessor(queue: jobQueue, logger: logger)) {
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "25")
            let value = await stream.first { _ in true }
            #expect(value == 35)
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "3")
            let value2 = await stream.first { _ in true }
            #expect(value2 == 3)
        }
    }

    @Test func testIfElseConditionWorkflow() async throws {
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let (stream, cont) = AsyncStream.makeStream(of: String.self)
        let jobQueue = JobQueue(.memory, logger: logger)
        let workflow = WorkflowBuilder()
            .addStep(
                WorkflowStep(name: "convert-to-int") { (input: String, context) in
                    Int(input)!
                }
            )
            .if {
                $0 > 10
            } then: {
                $0.addStep(
                    WorkflowStep(name: "big") { (input: Int, context) in
                        "BIG"
                    }
                )
            } else: {
                $0.addStep(
                    WorkflowStep(name: "small") { (input: Int, context) in
                        "SMALL"
                    }
                )
            }
            .addStep(
                WorkflowStep(name: "yield") { (input: String, context) in
                    cont.yield(input)
                    return
                }
            )
        let workflowName = jobQueue.registerWorkflow(name: "testing", workflow: workflow)

        try await testJobQueue(JobQueueProcessor(queue: jobQueue, logger: logger)) {
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "25")
            let value = await stream.first { _ in true }
            #expect(value == "BIG")
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "3")
            let value2 = await stream.first { _ in true }
            #expect(value2 == "SMALL")
        }
    }

    @Test func testGuardConditionWorkflow() async throws {
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let (stream, cont) = AsyncStream.makeStream(of: Int.self)
        let jobQueue = JobQueue(.memory, logger: logger)
        let workflow = WorkflowBuilder()
            .addStep(
                WorkflowStep(name: "convert-to-int") { (input: String, context) in
                    Int(input)!
                }
            )
            .guard {
                $0 <= 10
            } then: {
                $0.addStep(
                    WorkflowStep(name: "big") { (input: Int, context) in
                        cont.yield(input + 10)
                        return
                    }
                )
            }
            .addStep(
                WorkflowStep(name: "yield") { (input: Int, context) in
                    cont.yield(input)
                    return
                }
            )

        let workflowName = jobQueue.registerWorkflow(name: "testing", workflow: workflow)
        try await testJobQueue(JobQueueProcessor(queue: jobQueue, logger: logger)) {
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "25")
            let value = await stream.first { _ in true }
            #expect(value == 35)
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "3")
            let value2 = await stream.first { _ in true }
            #expect(value2 == 3)
        }
    }

    @Test func testChildWorkflow() async throws {
        struct RandomError: Error {}
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let (stream, cont) = AsyncStream.makeStream(of: Int.self)
        let jobQueue = JobQueue(.memory, logger: logger)

        let childWorkflow = WorkflowBuilder()
            .addStep(
                WorkflowStep(name: "Set bit 0") { (input: Int, context) in
                    input | 1
                }
            ).if {
                $0 & 2 != 0
            } then: {
                $0.addStep(
                    WorkflowStep(name: "set bit 2") { (input: Int, _) in
                        input | 4
                    }
                )
            }
        let workflow = WorkflowBuilder()
            .addStep(
                WorkflowStep(name: "convert-to-int") { (input: String, context) -> Int in
                    Int(input)!
                }
            ).addChildWorkflow(childWorkflow)
            .addStep(
                WorkflowStep(name: "Set bit 5") { (input: Int, context) in
                    input | 32
                }
            ).addStep(
                WorkflowStep(name: "yield") { (input: Int, context) in
                    cont.yield(input)
                    return
                }
            )

        let workflowName = jobQueue.registerWorkflow(name: "testing", workflow: workflow)
        try await testJobQueue(JobQueueProcessor(queue: jobQueue, logger: logger)) {
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "0")
            let value = await stream.first { _ in true }
            #expect(value == 33)
        }
    }

    @Test func testWorkflowWithGroupName() async throws {
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let (stream, cont) = AsyncStream.makeStream(of: Double.self)
        let jobQueue = JobQueue(.memory, logger: logger)
        let workflow = WorkflowBuilder()
            .addStep(name: "convert-to-int", input: String.self) { input, context in
                print(input)
                return Int(input)!
            }
            .group(name: "group") { builder in
                builder.addStep(name: "divide-by-two") { (input, context) in
                    print(input)
                    return Double(input) / 2.0
                }
            }
            .addStep(name: "divide-by-two") { (input, context) in
                print(input)
                return Double(input) / 2.0
            }
            .addStep(name: "yield") { input, _ in
                cont.yield(input)
                return
            }
        let workflowName = jobQueue.registerWorkflow(name: "basic", workflow: workflow)
        try await testJobQueue(JobQueueProcessor(queue: jobQueue, logger: logger)) {
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "25")
            let value = await stream.first { _ in true }
            #expect(value == 6.25)
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "3")
            let value2 = await stream.first { _ in true }
            #expect(value2 == 0.75)
        }
    }

    @Test func testDropResult() async throws {
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let (stream, cont) = AsyncStream.makeStream(of: Double.self)
        let jobQueue = JobQueue(.memory, logger: logger)
        let workflow = WorkflowBuilder()
            .addStep(name: "convert-to-int", input: String.self) { input, context in
                print(input)
                return Int(input)!
            }
            .addStep(name: "divide-by-two") { (input, context) in
                print(input)
                return Double(input) / 2.0
            }
            .addStep(name: "yield") { input, _ in
                cont.yield(input)
                return input
            }.dropResult()
        let workflowName = jobQueue.registerWorkflow(name: "basic", workflow: workflow)
        try await testJobQueue(JobQueueProcessor(queue: jobQueue, logger: logger)) {
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "25")
            let value = await stream.first { _ in true }
            #expect(value == 12.5)
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "3")
            let value2 = await stream.first { _ in true }
            #expect(value2 == 1.5)
        }
    }
}

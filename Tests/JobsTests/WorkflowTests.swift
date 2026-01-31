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

    @Test func testIfConditionWorkflow() async throws {
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let (stream, cont) = AsyncStream.makeStream(of: Int.self)
        let jobQueue = JobQueue(.memory, logger: logger)
        let workflowName = jobQueue.registerWorkflow(name: "testing", input: String.self) {
            WorkflowJob(name: "convert-to-int") { (input: String, context) in
                Int(input)!
            }
            IfThen(output: Int.self) { (output: Int) in
                output > 10
            } then: {
                WorkflowJob(name: "big") { (input: Int, context) in
                    input + 10
                }
            }
            WorkflowJob(name: "yield") { (input: Int, context) in
                cont.yield(input)
                return
            }
        }

        try await testJobQueue(JobQueueProcessor(queue: jobQueue, logger: logger)) {
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "25")
            let value = await stream.first { _ in true }
            #expect(value == 35)
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "3")
            let value2 = await stream.first { _ in true }
            #expect(value2 == 3)
        }
    }

    @Test func testIfEndingConditionWorkflow() async throws {
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let (stream, cont) = AsyncStream.makeStream(of: Int.self)
        let jobQueue = JobQueue(.memory, logger: logger)
        let workflowName = jobQueue.registerWorkflow(name: "testing", input: String.self) {
            WorkflowJob(name: "convert-to-int") { (input: String, context) in
                Int(input)!
            }
            IfThen(output: Void.self) { (output: Int) in
                output > 10
            } then: {
                WorkflowJob(name: "big") { (input: Int, context) in
                    cont.yield(input + 10)
                    return
                }
            }
            WorkflowJob(name: "yield") { (input: Int, context) in
                cont.yield(input)
                return
            }
        }

        try await testJobQueue(JobQueueProcessor(queue: jobQueue, logger: logger)) {
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "25")
            let value = await stream.first { _ in true }
            #expect(value == 35)
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "3")
            let value2 = await stream.first { _ in true }
            #expect(value2 == 3)
        }
    }

    @Test func testIfElseConditionAtEndWorkflow() async throws {
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let (stream, cont) = AsyncStream.makeStream(of: String.self)
        let jobQueue = JobQueue(.memory, logger: logger)
        let workflowName = jobQueue.registerWorkflow(name: "testing", input: String.self) {
            WorkflowJob(name: "convert-to-int") { (input: String, context) in
                Int(input)!
            }
            IfThenElse(output: Void.self) { (output: Int) in
                output > 10
            } then: {
                WorkflowJob(name: "big") { (input: Int, context) in
                    cont.yield("BIG")
                    return
                }
            } else: {
                WorkflowJob(name: "small") { (input: Int, context) in
                    cont.yield("SMALL")
                    return
                }
            }
        }

        try await testJobQueue(JobQueueProcessor(queue: jobQueue, logger: logger)) {
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "25")
            let value = await stream.first { _ in true }
            #expect(value == "BIG")
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "3")
            let value2 = await stream.first { _ in true }
            #expect(value2 == "SMALL")
        }
    }

    @Test func testIfElseConditionWorkflow() async throws {
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let (stream, cont) = AsyncStream.makeStream(of: String.self)
        let jobQueue = JobQueue(.memory, logger: logger)
        let workflowName = jobQueue.registerWorkflow(name: "testing", input: String.self) {
            WorkflowJob(name: "convert-to-int") { (input: String, context) in
                Int(input)!
            }
            IfThenElse(output: String.self) { (output: Int) in
                output > 10
            } then: {
                WorkflowJob(name: "big") { (input: Int, context) in
                    "BIG"
                }
            } else: {
                WorkflowJob(name: "small") { (input: Int, context) in
                    "SMALL"
                }
            }
            WorkflowJob(name: "yield") { (input: String, context) in
                cont.yield(input)
                return
            }
        }

        try await testJobQueue(JobQueueProcessor(queue: jobQueue, logger: logger)) {
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "25")
            let value = await stream.first { _ in true }
            #expect(value == "BIG")
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "3")
            let value2 = await stream.first { _ in true }
            #expect(value2 == "SMALL")
        }
    }

    @Test func testPartialWorkflowInCondition() async throws {
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let (stream, cont) = AsyncStream.makeStream(of: String.self)
        let jobQueue = JobQueue(.memory, logger: logger)
        let workflowName = jobQueue.registerWorkflow(name: "testing", input: String.self) {
            WorkflowJob(name: "convert-to-int") { (input: String, context) in
                Int(input)!
            }
            IfThenElse(output: String.self) { (output: Int) in
                output > 10
            } then: {
                WorkflowJob(name: "big") { (input: Int, context) in
                    "BIG"
                }
                WorkflowJob(name: "really-big") { (input: String, context) in
                    "REALLY \(input)"
                }
            } else: {
                WorkflowJob(name: "small") { (input: Int, context) in
                    "SMALL"
                }
            }
            WorkflowJob(name: "yield") { (input: String, context) in
                cont.yield(input)
                return
            }
        }

        try await testJobQueue(JobQueueProcessor(queue: jobQueue, logger: logger)) {
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "25")
            let value = await stream.first { _ in true }
            #expect(value == "REALLY BIG")
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "3")
            let value2 = await stream.first { _ in true }
            #expect(value2 == "SMALL")
        }
    }

    @Test func testIfElseConditionInPartialWorkflow() async throws {
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let (stream, cont) = AsyncStream.makeStream(of: String.self)
        let jobQueue = JobQueue(.memory, logger: logger)
        let workflowName = jobQueue.registerWorkflow(name: "testing", input: String.self) {
            WorkflowJob(name: "convert-to-int") { (input: String, context) in
                Int(input)!
            }
            IfThenElse(output: String.self) { (output: Int) in
                output > 10
            } then: {
                WorkflowJob(name: "divide") { (input: Int, context) in
                    input / 2
                }
                IfThenElse(output: String.self) { (output: Int) in
                    output > 100
                } then: {
                    WorkflowJob(name: "really-big") { (input: Int, context) in
                        "REALLY BIG"
                    }
                } else: {
                    WorkflowJob(name: "big") { (input: Int, context) in
                        "BIG"
                    }
                }
            } else: {
                WorkflowJob(name: "small") { (input: Int, context) in
                    "SMALL"
                }
            }
            WorkflowJob(name: "yield") { (input: String, context) in
                cont.yield(input)
                return
            }
        }

        try await testJobQueue(JobQueueProcessor(queue: jobQueue, logger: logger)) {
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "25")
            let value = await stream.first { _ in true }
            #expect(value == "BIG")
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "250")
            let value2 = await stream.first { _ in true }
            #expect(value2 == "REALLY BIG")
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "3")
            let value3 = await stream.first { _ in true }
            #expect(value3 == "SMALL")
        }
    }

    @Test func testIfElseFollowingIfWorkflow() async throws {
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let (stream, cont) = AsyncStream.makeStream(of: String.self)
        let jobQueue = JobQueue(.memory, logger: logger)
        let workflowName = jobQueue.registerWorkflow(name: "testing", input: String.self) {
            WorkflowJob(name: "convert-to-int") { (input: String, context) in
                Int(input)!
            }
            IfThen(output: Int.self) { (output: Int) in
                output > 10
            } then: {
                WorkflowJob(name: "grow") { (input: Int, context) in
                    input + 10
                }
            }
            IfThenElse(output: String.self) {
                $0 > 100
            } then: {
                WorkflowJob(name: "big") { (input: Int, context) in
                    "BIG \(input)"
                }
            } else: {
                WorkflowJob(name: "small") { (input: Int, context) in
                    "SMALL \(input)"
                }
            }
            WorkflowJob(name: "yield") { (input: String, context) in
                cont.yield(input)
                return
            }
        }

        try await testJobQueue(JobQueueProcessor(queue: jobQueue, logger: logger)) {
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "25")
            var value = await stream.first { _ in true }
            #expect(value == "SMALL 35")
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "95")
            value = await stream.first { _ in true }
            #expect(value == "BIG 105")
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "3")
            value = await stream.first { _ in true }
            #expect(value == "SMALL 3")
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "200")
            value = await stream.first { _ in true }
            #expect(value == "BIG 210")
        }
    }

    @Test func testChildWorkflow() async throws {
        struct RandomError: Error {}
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let (stream, cont) = AsyncStream.makeStream(of: Int.self)
        let jobQueue = JobQueue(.memory, logger: logger)

        let childWorkflow = jobQueue.createChildWorkflow(name: "child", input: Int.self, output: Int.self) {
            WorkflowJob(name: "Set bit 0") { (input: Int, context) in
                input | 1
            }
            IfThen(output: Int.self) {
                $0 & 2 != 0
            } then: {
                WorkflowJob(name: "set bit 2") { (input: Int, _) in
                    input | 4
                }
            }
        }

        let workflowName = jobQueue.registerWorkflow(name: "basic", input: String.self) {
            WorkflowJob(name: "convert-to-int") { (input: String, context) -> Int in
                Int(input)!
            }
            childWorkflow
            WorkflowJob(name: "Set bit 5") { (input: Int, context) in
                input | 32
            }
            WorkflowJob(name: "yield") { (input: Int, context) in
                cont.yield(input)
                return
            }
        }

        try await testJobQueue(JobQueueProcessor(queue: jobQueue, logger: logger)) {
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "0")
            let value = await stream.first { _ in true }
            #expect(value == 33)
        }
    }

    @Test func testChildWorkflowInCondition() async throws {
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let (stream, cont) = AsyncStream.makeStream(of: String.self)
        let jobQueue = JobQueue(.memory, logger: logger)
        let bigChildWorkflow = jobQueue.createChildWorkflow(name: "big", output: String.self) {
            WorkflowJob(name: "big") { (input: Int, context) in
                "BIG"
            }
            WorkflowJob(name: "really-big") { (input: String, context) in
                "REALLY \(input)"
            }
        }
        let workflowName = jobQueue.registerWorkflow(name: "testing", input: String.self) {
            WorkflowJob(name: "convert-to-int") { (input: String, context) in
                Int(input)!
            }
            IfThenElse(output: String.self) { (output: Int) in
                output > 10
            } then: {
                bigChildWorkflow
            } else: {
                WorkflowJob(name: "small") { (input: Int, context) in
                    "SMALL"
                }
            }
            WorkflowJob(name: "yield") { (input: String, context) in
                cont.yield(input)
                return
            }
        }

        try await testJobQueue(JobQueueProcessor(queue: jobQueue, logger: logger)) {
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "25")
            let value = await stream.first { _ in true }
            #expect(value == "REALLY BIG")
            _ = try await jobQueue.pushWorkflow(workflowName, parameters: "3")
            let value2 = await stream.first { _ in true }
            #expect(value2 == "SMALL")
        }
    }
}

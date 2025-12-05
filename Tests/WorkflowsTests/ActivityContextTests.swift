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

import Foundation
import Logging
import NIOCore
import ServiceLifecycle
import Testing

@testable import Jobs
@testable import Workflows

@Suite("ActivityContextTests")
struct ActivityContextTests {

    // MARK: - Helper Methods

    private func setupWorkflowSystem() async throws -> (WorkflowEngine<MemoryWorkflowQueue>, any Service) {
        let logger = Logger(label: "ActivityContextTests")

        let activities = TestActivityContainer()
        let workflowEngine = WorkflowEngine(
            queue: .memoryWorkflow,
            logger: logger,
            activities: [activities]
        )

        let processor = workflowEngine.processor(options: .init(numWorkers: 10))

        // Register test workflows
        workflowEngine.registerWorkflow(ContextAwareWorkflow.self)
        workflowEngine.registerWorkflow(TaskLocalWorkflow.self)
        workflowEngine.registerWorkflow(VoidActivityWorkflow.self)
        workflowEngine.registerWorkflow(ContextVoidWorkflow.self)
        workflowEngine.registerWorkflow(RetryAwareWorkflow.self)

        return (workflowEngine, processor)
    }

    // MARK: - Test Workflows

    struct ContextAwareWorkflow: WorkflowProtocol {
        struct Input: Codable, Sendable {
            let message: String
        }

        struct Output: Codable, Sendable {
            let processedMessage: String
            let activityInfo: String
        }

        func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
            let result = try await context.executeActivity(
                ContextActivity.self,
                input: ContextInput(message: input.message)
            )

            return Output(
                processedMessage: result.processedMessage,
                activityInfo: result.contextInfo
            )
        }
    }

    struct TaskLocalWorkflow: WorkflowProtocol {
        struct Input: Codable, Sendable {
            let value: Int
        }

        struct Output: Codable, Sendable {
            let result: Int
            let hadContext: Bool
        }

        func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
            let result = try await context.executeActivity(
                TaskLocalActivity.self,
                input: TaskLocalInput(value: input.value)
            )

            return Output(
                result: result.doubledValue,
                hadContext: result.hadContext
            )
        }
    }

    struct VoidActivityWorkflow: WorkflowProtocol {
        struct Input: Codable, Sendable {
            let message: String
        }

        struct Output: Codable, Sendable {
            let completed: Bool
        }

        func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
            let _ = try await context.executeActivity(
                VoidActivity.self,
                input: VoidInput(message: input.message)
            )

            return Output(completed: true)
        }
    }

    struct ContextVoidWorkflow: WorkflowProtocol {
        struct Input: Codable, Sendable {
            let data: String
        }

        struct Output: Codable, Sendable {
            let success: Bool
        }

        func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
            let _ = try await context.executeActivity(
                ContextVoidActivity.self,
                input: ContextVoidInput(data: input.data)
            )

            return Output(success: true)
        }
    }

    struct RetryAwareWorkflow: WorkflowProtocol {
        struct Input: Codable, Sendable {
            let shouldSucceed: Bool
        }

        struct Output: Codable, Sendable {
            let attempts: Int
        }

        func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
            let result = try await context.executeActivity(
                RetryActivity.self,
                input: RetryInput(shouldSucceed: input.shouldSucceed)
            )

            return Output(attempts: result.attemptNumber)
        }
    }

    // MARK: - Tests

    @Test("Activity receives execution context")
    func activityReceivesExecutionContext() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        let result = try await testWorkflow(processor) {
            try await workflowEngine.runWorkflow(
                ContextAwareWorkflow.self,
                input: ContextAwareWorkflow.Input(message: "test message")
            )
        }

        #expect(result.processedMessage == "PROCESSED: test message")
        #expect(result.activityInfo.contains("activityId:"))
        #expect(result.activityInfo.contains("attempt: 1"))
    }

    @Test("Task local context access works")
    func taskLocalContextAccess() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        let result = try await testWorkflow(processor) {
            try await workflowEngine.runWorkflow(
                TaskLocalWorkflow.self,
                input: TaskLocalWorkflow.Input(value: 42)
            )
        }

        #expect(result.result == 84)
        #expect(result.hadContext == true)
    }

    @Test("Void activities execute successfully")
    func voidActivitiesExecute() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        let result = try await testWorkflow(processor) {
            try await workflowEngine.runWorkflow(
                VoidActivityWorkflow.self,
                input: VoidActivityWorkflow.Input(message: "test void")
            )
        }

        #expect(result.completed == true)
    }

    @Test("Context aware void activities work")
    func contextAwareVoidActivities() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        let result = try await testWorkflow(processor) {
            try await workflowEngine.runWorkflow(
                ContextVoidWorkflow.self,
                input: ContextVoidWorkflow.Input(data: "context test")
            )
        }

        #expect(result.success == true)
    }

    @Test("Activities are retry aware")
    func activitiesAreRetryAware() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        let result = try await testWorkflow(processor) {
            try await workflowEngine.runWorkflow(
                RetryAwareWorkflow.self,
                input: RetryAwareWorkflow.Input(shouldSucceed: true)
            )
        }

        #expect(result.attempts == 1)
    }
}

// MARK: - Activity Definitions

struct ContextInput: Codable, Sendable {
    let message: String
}

struct ContextOutput: Codable, Sendable {
    let processedMessage: String
    let contextInfo: String
}

struct ContextActivity: ActivityParameters {
    typealias Input = ContextInput
    typealias Output = ContextOutput
}

struct TaskLocalInput: Codable, Sendable {
    let value: Int
}

struct TaskLocalOutput: Codable, Sendable {
    let doubledValue: Int
    let hadContext: Bool
}

struct TaskLocalActivity: ActivityParameters {
    typealias Input = TaskLocalInput
    typealias Output = TaskLocalOutput
}

struct VoidInput: Codable, Sendable {
    let message: String
}

struct VoidActivity: ActivityParameters {
    typealias Input = VoidInput
}

struct ContextVoidInput: Codable, Sendable {
    let data: String
}

struct ContextVoidActivity: ActivityParameters {
    typealias Input = ContextVoidInput
}

struct RetryInput: Codable, Sendable {
    let shouldSucceed: Bool
}

struct RetryOutput: Codable, Sendable {
    let attemptNumber: Int
}

struct RetryActivity: ActivityParameters {
    typealias Input = RetryInput
    typealias Output = RetryOutput
}

// MARK: - Test Activity Container

struct TestActivityContainer: ActivityContainer {
    func registerActivities(with registry: ActivityRegistry) {
        // Context-aware activity with return value
        registry.registerActivity(ContextActivity.self) {
            (input: ContextInput, context: ActivityExecutionContext) async throws -> ContextOutput in

            let contextInfo = "activityId: \(context.info.activityId), attempt: \(context.attempt)"

            context.logger.info(
                "Processing with context",
                metadata: [
                    "inputMessage": .string(input.message),
                    "activityId": .string(context.info.activityId),
                ]
            )

            return ContextOutput(
                processedMessage: "PROCESSED: \(input.message)",
                contextInfo: contextInfo
            )
        }

        // Task-local context activity (no explicit context parameter)
        registry.registerActivity(TaskLocalActivity.self) { input, context in
            // Access context via task-local storage
            let currentContext = ActivityExecutionContext.current
            let hadContext = currentContext != nil

            if let context = currentContext {
                context.logger.info(
                    "Task-local context available",
                    metadata: [
                        "value": .stringConvertible(input.value)
                    ]
                )
            }

            return TaskLocalOutput(
                doubledValue: input.value * 2,
                hadContext: hadContext
            )
        }

        // Simple void activity
        registry.registerActivity(VoidActivity.self) { input, context in
            // Simulate side effect
            print("Void activity executed: \(input.message)")
        }

        // Context-aware void activity
        registry.registerActivity(ContextVoidActivity.self, returning: Void.self) { input, context in
            context.logger.info(
                "Context void activity",
                metadata: [
                    "data": .string(input.data),
                    "attempt": .stringConvertible(context.attempt),
                ]
            )

            context.logger.debug(
                "Processing context void activity data",
                metadata: [
                    "dataLength": .stringConvertible(input.data.count)
                ]
            )
        }

        // Retry-aware activity
        registry.registerActivity(RetryActivity.self) {
            (input: RetryInput, context: ActivityExecutionContext) async throws -> RetryOutput in

            context.logger.info(
                "Retry activity",
                metadata: [
                    "attempt": .stringConvertible(context.attempt),
                    "isRetry": .string(String(context.isRetry)),
                ]
            )

            return RetryOutput(attemptNumber: context.attempt)
        }
    }
}

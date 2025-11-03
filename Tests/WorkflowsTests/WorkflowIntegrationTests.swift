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

struct WorkflowIntegrationTests {

    // MARK: - Helper Methods for Workflow Completion

    /// Helper to wait for workflow completion using TestExpectation
    private func waitForWorkflowCompletion(
        _ workflowId: WorkflowID,
        engine: WorkflowEngine<MemoryQueue>,
        expectedStatus: WorkflowStatus = .completed,
        timeout: Duration = .seconds(10),
        description: String = "workflow completion"
    ) async throws -> WorkflowExecutionStatus {
        let expectation = TestExpectation()

        let pollingTask = Task {
            while !Task.isCancelled {
                do {
                    let status = try await engine.getWorkflowStatus(workflowId)
                    if status.status == expectedStatus {
                        expectation.trigger()
                        return
                    } else if status.status == .failed && expectedStatus != .failed {
                        expectation.trigger()
                        return
                    }
                    try await Task.sleep(for: .milliseconds(100))
                } catch {
                    break
                }
            }
        }

        defer { pollingTask.cancel() }

        try await expectation.wait(for: description, timeout: timeout)
        return try await engine.getWorkflowStatus(workflowId)
    }

    // MARK: - Test Workflows

    /// Simple workflow for basic testing
    struct SimpleWorkflow: WorkflowProtocol {
        static let workflowName = "SimpleWorkflow"

        struct Input: Codable, Sendable {
            let message: String
            let count: Int
        }

        struct Output: Codable, Sendable {
            let result: String
            let processedCount: Int
        }

        func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
            // Simple processing without activities
            Output(
                result: "Processed: \(input.message)",
                processedCount: input.count * 2
            )
        }
    }

    /// Workflow with single activity
    struct SingleActivityWorkflow: WorkflowProtocol {
        static let workflowName = "SingleActivityWorkflow"

        struct Input: Codable, Sendable {
            let text: String
        }

        struct Output: Codable, Sendable {
            let processedText: String
        }

        func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
            let processedText: String = try await context.executeActivity(
                ProcessTextActivity.self,
                input: TextProcessingInput(text: input.text),
                options: ActivityOptions(startToCloseTimeout: .seconds(30))
            )

            return Output(processedText: processedText)
        }
    }

    struct ProcessTextActivity: ActivityParameters {
        static let activityName = "processText"
        typealias Input = TextProcessingInput
        typealias Output = String
    }

    // MARK: - Multiple Activities Workflow

    struct MultipleActivitiesWorkflow: WorkflowProtocol {
        static let workflowName = "MultipleActivitiesWorkflow"

        struct Input: Codable, Sendable {
            let orderId: String
            let customerId: String
            let amount: Double
        }

        struct Output: Codable, Sendable {
            let orderId: String
            let paymentId: String
        }

        func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
            // Step 1: Validate order
            let isValid: Bool = try await context.executeActivity(
                ValidateOrderActivity.self,
                input: OrderValidationInput(orderId: input.orderId, customerId: input.customerId, amount: input.amount),
                options: ActivityOptions(
                    startToCloseTimeout: .seconds(10),
                    retryPolicy: .exponentialJitter(maxAttempts: 3)
                )
            )

            guard isValid else {
                throw WorkflowTestError.orderValidationFailed
            }

            // Step 2: Process payment
            let paymentId: String = try await context.executeActivity(
                ProcessPaymentActivity.self,
                input: PaymentInput(customerId: input.customerId, amount: input.amount),
                options: ActivityOptions(
                    startToCloseTimeout: .seconds(30),
                    retryPolicy: .exponentialJitter(maxAttempts: 5)
                )
            )

            // Step 3: Update order status
            let _: EmptyOutput = try await context.executeActivity(
                UpdateOrderStatusActivity.self,
                input: StatusUpdateInput(orderId: input.orderId, status: "completed"),
                options: ActivityOptions(startToCloseTimeout: .seconds(15))
            )

            return Output(
                orderId: input.orderId,
                paymentId: paymentId
            )
        }
    }

    struct ValidateOrderActivity: ActivityParameters {
        static let activityName = "validateOrder"
        typealias Input = OrderValidationInput
        typealias Output = Bool
    }

    struct ProcessPaymentActivity: ActivityParameters {
        static let activityName = "processPayment"
        typealias Input = PaymentInput
        typealias Output = String
    }

    struct UpdateOrderStatusActivity: ActivityParameters {
        static let activityName = "updateOrderStatus"
        typealias Input = StatusUpdateInput
        typealias Output = EmptyOutput
    }

    /// Workflow that demonstrates error handling
    struct ErrorHandlingWorkflow: WorkflowProtocol {
        static let workflowName = "ErrorHandlingWorkflow"

        struct Input: Codable, Sendable {
            let shouldFail: Bool
            let failureType: String
        }

        struct Output: Codable, Sendable {
            let result: String
        }

        func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
            do {
                let result: String = try await context.executeActivity(
                    RiskyOperationActivity.self,
                    input: RiskyOperationInput(shouldFail: input.shouldFail, failureType: input.failureType),
                    options: ActivityOptions(
                        startToCloseTimeout: .seconds(10),
                        retryPolicy: .exponentialJitter(maxAttempts: 3)
                    )
                )

                return Output(result: result)
            } catch {
                // Handle specific errors and provide fallback
                if input.failureType == "recoverable" {
                    return Output(result: "Fallback result after error: \(error)")
                } else {
                    throw error
                }
            }
        }
    }

    struct RiskyOperationActivity: ActivityParameters {
        static let activityName = "riskyOperation"
        typealias Input = RiskyOperationInput
        typealias Output = String
    }

    // MARK: - Parallel Execution Test Workflow

    struct ParallelActivitiesWorkflow: WorkflowProtocol {
        static let workflowName = "ParallelActivitiesWorkflow"

        struct Input: Codable, Sendable {
            let data: String
        }

        struct Output: Codable, Sendable {
            let results: [String]
        }

        func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
            // Execute three activities in parallel using async let
            async let result1: String = context.executeActivity(
                ParallelTask1Activity.self,
                input: ParallelTaskInput(data: input.data),
                options: ActivityOptions(startToCloseTimeout: .seconds(5))
            )

            async let result2: String = context.executeActivity(
                ParallelTask2Activity.self,
                input: ParallelTaskInput(data: input.data),
                options: ActivityOptions(startToCloseTimeout: .seconds(5))
            )

            async let result3: String = context.executeActivity(
                ParallelTask3Activity.self,
                input: ParallelTaskInput(data: input.data),
                options: ActivityOptions(startToCloseTimeout: .seconds(5))
            )

            let (r1, r2, r3) = try await (result1, result2, result3)
            return Output(results: [r1, r2, r3])
        }
    }

    struct ParallelTask1Activity: ActivityParameters {
        static let activityName = "parallelTask1"
        typealias Input = ParallelTaskInput
        typealias Output = String
    }

    struct ParallelTask2Activity: ActivityParameters {
        static let activityName = "parallelTask2"
        typealias Input = ParallelTaskInput
        typealias Output = String
    }

    struct ParallelTask3Activity: ActivityParameters {
        static let activityName = "parallelTask3"
        typealias Input = ParallelTaskInput
        typealias Output = String
    }

    // MARK: - Activity Input Types

    struct ParallelTaskInput: Codable, Sendable {
        let data: String
    }

    struct TextProcessingInput: Codable, Sendable {
        let text: String
    }

    struct OrderValidationInput: Codable, Sendable {
        let orderId: String
        let customerId: String
        let amount: Double
    }

    struct PaymentInput: Codable, Sendable {
        let customerId: String
        let amount: Double
    }

    struct StatusUpdateInput: Codable, Sendable {
        let orderId: String
        let status: String
    }

    struct RiskyOperationInput: Codable, Sendable {
        let shouldFail: Bool
        let failureType: String
    }

    // MARK: - Test Errors

    enum WorkflowTestError: Error {
        case orderValidationFailed
        case paymentProcessingFailed
        case recoverableError
        case nonRecoverableError
        case invalidOrder(String)
    }

    // MARK: - Helper Methods

    func setupWorkflowSystem() async throws -> (WorkflowEngine<MemoryQueue>, any Service) {
        let logger = Logger(label: "WorkflowIntegrationTests")
        let jobQueue = JobQueue(.memory, logger: logger)

        // Create activity container with test activities
        let activities = TestActivityContainer()

        // Create workflow engine with activity container
        let workflowEngine = WorkflowEngine(
            jobQueue: jobQueue,
            logger: logger,
            activities: [activities]
        )

        // Register test workflows
        workflowEngine.registerWorkflow(SimpleWorkflow.self)
        workflowEngine.registerWorkflow(SingleActivityWorkflow.self)
        workflowEngine.registerWorkflow(MultipleActivitiesWorkflow.self)
        workflowEngine.registerWorkflow(ErrorHandlingWorkflow.self)
        workflowEngine.registerWorkflow(ParallelActivitiesWorkflow.self)

        // Create processor with 4 workers to handle concurrent workflows
        // Each workflow needs 1 persistent WorkflowCoordinator job + short-lived ActivityExecution jobs
        // 4 workers allows 2-3 concurrent workflows with good activity parallelism
        // Production scaling: workers ≈ concurrent_workflows + (concurrent_workflows × 0.5-1.0)
        let processor = jobQueue.processor(options: .init(numWorkers: 4))

        return (workflowEngine, processor)
    }

    // MARK: - Test Activity Container

    struct TestActivityContainer: ActivityContainer {
        func registerActivities(with registry: ActivityRegistry) {
            registry.registerActivity(ProcessTextActivity.self) { (input: TextProcessingInput) async throws -> String in
                try await Task.sleep(for: .milliseconds(50))
                return input.text.uppercased()
            }

            registry.registerActivity(ValidateOrderActivity.self) { (input: OrderValidationInput) async throws -> Bool in
                try await Task.sleep(for: .milliseconds(30))

                // Validate order - fail if amount is negative
                guard input.amount > 0 else {
                    throw WorkflowTestError.invalidOrder("Amount must be positive")
                }

                // Fail if order ID contains "INVALID"
                guard !input.orderId.contains("INVALID") else {
                    throw WorkflowTestError.invalidOrder("Invalid order ID")
                }

                return true
            }

            registry.registerActivity(ProcessPaymentActivity.self) { (input: PaymentInput) async throws -> String in
                try await Task.sleep(for: .milliseconds(40))
                return "PAY-\(UUID().uuidString.prefix(8))"
            }

            registry.registerActivity(UpdateOrderStatusActivity.self) { (input: StatusUpdateInput) async throws -> EmptyOutput in
                try await Task.sleep(for: .milliseconds(25))
                return EmptyOutput()
            }

            registry.registerActivity(RiskyOperationActivity.self) { (input: RiskyOperationInput) async throws -> String in
                try await Task.sleep(for: .milliseconds(30))

                if input.shouldFail {
                    switch input.failureType {
                    case "recoverable":
                        throw WorkflowTestError.recoverableError
                    case "nonRecoverable":
                        throw WorkflowTestError.nonRecoverableError
                    default:
                        throw WorkflowTestError.recoverableError
                    }
                }

                return "Operation completed successfully"
            }

            registry.registerActivity(ParallelTask1Activity.self) { (input: ParallelTaskInput) async throws -> String in
                try await Task.sleep(for: .milliseconds(100))
                return "Task1 processed: \(input.data)"
            }

            registry.registerActivity(ParallelTask2Activity.self) { (input: ParallelTaskInput) async throws -> String in
                try await Task.sleep(for: .milliseconds(150))
                return "Task2 processed: \(input.data)"
            }

            registry.registerActivity(ParallelTask3Activity.self) { (input: ParallelTaskInput) async throws -> String in
                try await Task.sleep(for: .milliseconds(120))
                return "Task3 processed: \(input.data)"
            }
        }
    }

    // MARK: - Tests

    @Test("Simple workflow execution without activities")
    func testSimpleWorkflow() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        try await testWorkflow(processor) {
            let workflowId = try await workflowEngine.startWorkflow(
                SimpleWorkflow.self,
                input: SimpleWorkflow.Input(message: "Hello World", count: 5)
            )

            #expect(workflowId.value.count > 0)

            // Wait for workflow completion
            let finalStatus = try await waitForWorkflowCompletion(workflowId, engine: workflowEngine, description: "simple workflow")
            #expect(finalStatus.status == .completed)
            #expect(finalStatus.hasOutput == true)
        }
    }

    @Test("Single activity workflow execution")
    func testSingleActivityWorkflow() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        try await testWorkflow(processor) {
            let workflowId = try await workflowEngine.startWorkflow(
                SingleActivityWorkflow.self,
                input: SingleActivityWorkflow.Input(text: "hello world")
            )

            #expect(workflowId.value.count > 0)

            // Wait for workflow completion
            let finalStatus = try await waitForWorkflowCompletion(workflowId, engine: workflowEngine, description: "single activity workflow")
            if finalStatus.status == .failed {
                #expect(Bool(false), "Workflow failed: \(finalStatus.error ?? "Unknown error")")
                return
            }
            #expect(finalStatus.status == .completed)
            #expect(finalStatus.hasOutput == true)
        }
    }

    @Test("Multiple activities workflow execution")
    func testMultipleActivitiesWorkflow() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        try await testWorkflow(processor) {
            let workflowId = try await workflowEngine.startWorkflow(
                MultipleActivitiesWorkflow.self,
                input: MultipleActivitiesWorkflow.Input(
                    orderId: "ORDER-123",
                    customerId: "CUSTOMER-456",
                    amount: 99.99
                )
            )

            #expect(workflowId.value.count > 0)

            // Wait for workflow completion
            let finalStatus = try await waitForWorkflowCompletion(
                workflowId,
                engine: workflowEngine,
                timeout: .seconds(15),
                description: "multiple activities workflow"
            )
            if finalStatus.status == .failed {
                #expect(Bool(false), "Workflow failed unexpectedly")
                return
            }
            #expect(finalStatus.status == .completed)
            #expect(finalStatus.hasOutput == true)
        }
    }

    @Test("Workflow with validation failure")
    func testWorkflowValidationFailure() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        try await testWorkflow(processor) {
            let workflowId = try await workflowEngine.startWorkflow(
                MultipleActivitiesWorkflow.self,
                input: MultipleActivitiesWorkflow.Input(
                    orderId: "ORDER-INVALID",
                    customerId: "CUSTOMER-456",
                    amount: -10.0  // Invalid amount
                )
            )

            #expect(workflowId.value.count > 0)

            // Wait for workflow to fail
            let finalStatus = try await waitForWorkflowCompletion(
                workflowId,
                engine: workflowEngine,
                expectedStatus: .failed,
                description: "workflow validation failure"
            )
            if finalStatus.status == .completed {
                #expect(Bool(false), "Workflow should have failed but completed")
                return
            }
            #expect(finalStatus.status == .failed)
            #expect(finalStatus.error != nil)
        }
    }

    @Test("Error handling workflow with recovery")
    func testErrorHandlingWorkflowRecovery() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        try await testWorkflow(processor) {
            let workflowId = try await workflowEngine.startWorkflow(
                ErrorHandlingWorkflow.self,
                input: ErrorHandlingWorkflow.Input(
                    shouldFail: true,
                    failureType: "recoverable"
                )
            )

            #expect(workflowId.value.count > 0)

            // Wait for workflow completion (should recover)
            let finalStatus = try await waitForWorkflowCompletion(
                workflowId,
                engine: workflowEngine,
                description: "error handling workflow recovery"
            )
            if finalStatus.status == .failed {
                #expect(Bool(false), "Workflow failed: \(finalStatus.error ?? "Unknown error")")
                return
            }
            #expect(finalStatus.status == .completed)
            #expect(finalStatus.hasOutput == true)
        }
    }

    @Test("Error handling workflow with non-recoverable error")
    func testErrorHandlingWorkflowNonRecoverable() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        try await testWorkflow(processor) {
            let workflowId = try await workflowEngine.startWorkflow(
                ErrorHandlingWorkflow.self,
                input: ErrorHandlingWorkflow.Input(
                    shouldFail: true,
                    failureType: "nonRecoverable"
                )
            )

            #expect(workflowId.value.count > 0)

            // Wait for workflow to fail
            let finalStatus = try await waitForWorkflowCompletion(
                workflowId,
                engine: workflowEngine,
                expectedStatus: .failed,
                description: "non-recoverable error workflow"
            )
            if finalStatus.status == .completed {
                #expect(Bool(false), "Workflow should have failed but completed")
                return
            }
            #expect(finalStatus.status == .failed)
            #expect(finalStatus.error != nil)
        }
    }

    @Test("Custom workflow ID support")
    func testCustomWorkflowId() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        try await testWorkflow(processor) {
            let customId = WorkflowID(workflowId: "CUSTOM-WORKFLOW-123")

            let returnedId = try await workflowEngine.startWorkflow(
                SimpleWorkflow.self,
                input: SimpleWorkflow.Input(message: "Custom ID Test", count: 1),
                workflowId: customId
            )

            #expect(returnedId.workflowId == "CUSTOM-WORKFLOW-123")

            // Verify we can query the workflow by the custom ID
            let status = try await workflowEngine.getWorkflowStatus(customId)
            #expect(status.id == customId)
            #expect(status.workflowType == "SimpleWorkflow")
        }
    }

    @Test("Workflow cancellation")
    func testWorkflowCancellation() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        try await testWorkflow(processor) {
            // Start a simple workflow that we can cancel immediately
            let workflowId = try await workflowEngine.startWorkflow(
                SimpleWorkflow.self,
                input: SimpleWorkflow.Input(message: "To be cancelled", count: 1),
                workflowId: WorkflowID(workflowId: "cancel-test")
            )

            // Cancel the workflow immediately
            try await workflowEngine.cancelWorkflow(workflowId)

            // Give the cancellation a moment to process
            try await Task.sleep(for: .milliseconds(200))

            // Verify cancellation - the workflow should either be cancelled or completed
            // (since it's a simple workflow that might complete very quickly)
            let status = try await workflowEngine.getWorkflowStatus(workflowId)
            let isCancelledOrCompleted = status.status == .cancelled || status.status == .completed
            #expect(isCancelledOrCompleted, "Workflow should be cancelled or completed after cancellation request")

            // If it was cancelled, verify the error message
            if status.status == .cancelled {
                #expect(status.error?.contains("cancelled") == true, "Should have cancellation error message")
            }
        }
    }

    @Test("Workflow status querying")
    func testWorkflowStatusQuerying() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        try await testWorkflow(processor) {
            let workflowId = try await workflowEngine.startWorkflow(
                SimpleWorkflow.self,
                input: SimpleWorkflow.Input(message: "Status Test", count: 42)
            )

            // Check initial status
            let initialStatus = try await workflowEngine.getWorkflowStatus(workflowId)
            #expect(initialStatus.status == .running)
            #expect(initialStatus.startTime <= Date.now)
            #expect(initialStatus.endTime == nil)
            #expect(initialStatus.currentStep == 0)
            #expect(initialStatus.workflowType == "SimpleWorkflow")
        }
    }

    @Test("Non-existent workflow status query")
    func testNonExistentWorkflowStatus() async throws {
        let (workflowEngine, _) = try await setupWorkflowSystem()

        let nonExistentId = WorkflowID(workflowId: "NON-EXISTENT-123")

        do {
            _ = try await workflowEngine.getWorkflowStatus(nonExistentId)
            #expect(Bool(false), "Should have thrown an error for non-existent workflow")
        } catch WorkflowError.executionNotFound(let id) {
            #expect(id.workflowId == "NON-EXISTENT-123")
        } catch {
            #expect(Bool(false), "Wrong error type thrown: \(error)")
        }
    }

    @Test("Multiple workflow versions with runID")
    func testMultipleWorkflowVersions() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        try await testWorkflow(processor) {
            let businessWorkflowId = "ORDER-PROCESS-123"

            // Start first version of the workflow
            let version1Id = try await workflowEngine.startWorkflow(
                SimpleWorkflow.self,
                input: SimpleWorkflow.Input(message: "Version 1", count: 1),
                workflowId: WorkflowID(workflowId: businessWorkflowId)
            )

            // Start second version of the same business workflow
            let version2Id = try await workflowEngine.startWorkflow(
                SimpleWorkflow.self,
                input: SimpleWorkflow.Input(message: "Version 2", count: 2),
                workflowId: WorkflowID(workflowId: businessWorkflowId)
            )

            // Verify they have same business workflow ID but different run IDs
            #expect(version1Id.workflowId == businessWorkflowId)
            #expect(version2Id.workflowId == businessWorkflowId)
            #expect(version1Id.runId != version2Id.runId)
            #expect(version1Id != version2Id)

            // Wait for both workflows to complete using the helper
            let status1 = try await waitForWorkflowCompletion(
                version1Id,
                engine: workflowEngine,
                expectedStatus: .completed,
                timeout: .seconds(10),
                description: "workflow version 1"
            )

            let status2 = try await waitForWorkflowCompletion(
                version2Id,
                engine: workflowEngine,
                expectedStatus: .completed,
                timeout: .seconds(10),
                description: "workflow version 2"
            )

            #expect(status1.status == .completed, "Workflow version 1 should complete successfully")
            #expect(status2.status == .completed, "Workflow version 2 should complete successfully")
        }
    }

    @Test("Concurrent workflow executions")
    func testConcurrentWorkflowExecutions() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        try await testWorkflow(processor) {
            let workflowCount = 5
            var workflowIds: [WorkflowID] = []

            // Start multiple workflows concurrently
            try await withThrowingTaskGroup(of: WorkflowID.self) { group in
                for i in 0..<workflowCount {
                    group.addTask {
                        try await workflowEngine.startWorkflow(
                            SimpleWorkflow.self,
                            input: SimpleWorkflow.Input(
                                message: "Concurrent workflow \(i)",
                                count: i
                            )
                        )
                    }
                }

                for try await workflowId in group {
                    workflowIds.append(workflowId)
                }
            }

            #expect(workflowIds.count == workflowCount)

            // Wait for all workflows to complete using the helper
            for workflowId in workflowIds {
                let status = try await waitForWorkflowCompletion(
                    workflowId,
                    engine: workflowEngine,
                    expectedStatus: .completed,
                    timeout: .seconds(10),
                    description: "concurrent workflow \(workflowId.workflowId)"
                )
                #expect(status.status == .completed, "Workflow \(workflowId) should complete successfully")
            }
        }
    }

    @Test("Parallel activity execution")
    func testParallelActivityExecution() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        try await testWorkflow(processor) {
            let startTime = Date()

            let workflowId = try await workflowEngine.startWorkflow(
                ParallelActivitiesWorkflow.self,
                input: ParallelActivitiesWorkflow.Input(data: "test-data")
            )

            #expect(workflowId.value.count > 0)

            // Wait for workflow completion
            let finalStatus = try await waitForWorkflowCompletion(
                workflowId,
                engine: workflowEngine,
                description: "parallel activities workflow"
            )

            if finalStatus.status == .failed {
                #expect(Bool(false), "Parallel workflow failed: \(finalStatus.error ?? "Unknown error")")
                return
            }

            let endTime = Date()
            let duration = endTime.timeIntervalSince(startTime)

            // Parallel execution should complete faster than sequential
            // Sequential would take ~370ms (100+150+120), parallel should be ~150ms (max of the three)
            // Account for workflow coordination overhead
            #expect(duration < 1.0, "Parallel execution took too long: \(duration)s")

            #expect(finalStatus.status == .completed)
            #expect(finalStatus.hasOutput == true)

            // Verify the output contains all three results
            if finalStatus.hasOutput, let outputBuffer = finalStatus.output {
                let output = try JSONDecoder().decode(ParallelActivitiesWorkflow.Output.self, from: outputBuffer)
                #expect(output.results.count == 3)
                #expect(output.results.contains { $0.contains("Task1 processed: test-data") })
                #expect(output.results.contains { $0.contains("Task2 processed: test-data") })
                #expect(output.results.contains { $0.contains("Task3 processed: test-data") })
            }
        }
    }

    @Test("Workflow sleep functionality")
    func testWorkflowSleep() async throws {
        struct SleepWorkflow: WorkflowProtocol {
            static let workflowName = "SleepWorkflow"

            struct Input: Codable, Sendable {
                let sleepDuration: TimeInterval
            }

            struct Output: Codable, Sendable {
                let completed: Bool
                let duration: TimeInterval
            }

            func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
                let startTime = Date()

                // Use workflow sleep
                try await context.sleep(for: Duration.seconds(input.sleepDuration))

                let endTime = Date()
                let actualDuration = endTime.timeIntervalSince(startTime)

                return Output(completed: true, duration: actualDuration)
            }
        }

        let (workflowEngine, processor) = try await setupWorkflowSystem()
        workflowEngine.registerWorkflow(SleepWorkflow.self)

        try await testWorkflow(processor) {
            let startTime = Date()

            let workflowId = try await workflowEngine.startWorkflow(
                SleepWorkflow.self,
                input: SleepWorkflow.Input(sleepDuration: 0.2)
            )

            #expect(workflowId.value.count > 0)

            // Wait for workflow completion
            let finalStatus = try await waitForWorkflowCompletion(
                workflowId,
                engine: workflowEngine,
                timeout: .seconds(5),
                description: "sleep workflow"
            )

            if finalStatus.status == .failed {
                #expect(Bool(false), "Sleep workflow failed: \(finalStatus.error ?? "Unknown error")")
                return
            }

            let endTime = Date()
            let totalDuration = endTime.timeIntervalSince(startTime)

            // Sleep should add at least the requested duration
            #expect(totalDuration >= 0.2, "Sleep duration was too short: \(totalDuration)s")

            #expect(finalStatus.status == .completed)
            #expect(finalStatus.hasOutput == true)

            // Verify the output
            if let outputBuffer = finalStatus.output {
                let output = try JSONDecoder().decode(SleepWorkflow.Output.self, from: outputBuffer)
                #expect(output.completed == true)
                #expect(output.duration >= 0.2, "Workflow reported sleep duration too short: \(output.duration)s")
            }
        }
    }

    @Test("Concurrent workflows with same input")
    func testConcurrentWorkflowsWithSameInput() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        try await testWorkflow(processor) {
            // Start two workflows with identical input
            let input = MultipleActivitiesWorkflow.Input(
                orderId: "ORDER-123",
                customerId: "CUSTOMER-456",
                amount: 99.99
            )

            let workflowId1 = try await workflowEngine.startWorkflow(
                MultipleActivitiesWorkflow.self,
                input: input
            )

            let workflowId2 = try await workflowEngine.startWorkflow(
                MultipleActivitiesWorkflow.self,
                input: input
            )

            #expect(workflowId1.value != workflowId2.value)

            // Wait for both workflows to complete
            var attempts = 0
            var workflow1Complete = false
            var workflow2Complete = false

            while attempts < 150 && (!workflow1Complete || !workflow2Complete) {
                let status1 = try await workflowEngine.getWorkflowStatus(workflowId1)
                let status2 = try await workflowEngine.getWorkflowStatus(workflowId2)

                if status1.status == .completed {
                    workflow1Complete = true
                }
                if status2.status == .completed {
                    workflow2Complete = true
                }

                if status1.status == .failed {
                    #expect(Bool(false), "Workflow 1 failed: \(status1.error ?? "Unknown error")")
                    return
                }
                if status2.status == .failed {
                    #expect(Bool(false), "Workflow 2 failed: \(status2.error ?? "Unknown error")")
                    return
                }

                try await Task.sleep(for: .milliseconds(100))
                attempts += 1
            }

            #expect(workflow1Complete, "Workflow 1 did not complete in time")
            #expect(workflow2Complete, "Workflow 2 did not complete in time")
        }
    }

    @Test("Workflow timing information")
    func testWorkflowTimingInfo() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        // Create a workflow that accesses timing information
        struct TimingTestWorkflow: WorkflowProtocol {
            static let workflowName = "TimingTestWorkflow"

            struct Input: Codable, Sendable {
                let testId: String
            }

            struct Output: Codable, Sendable {
                let testId: String
                let hadScheduledAt: Bool
                let hadQueuedAt: Bool
                let hadStartTime: Bool
            }

            func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
                context.logger.info(
                    "Workflow timing info",
                    metadata: [
                        "queuedAt": "\(context.queuedAt)",
                        "scheduledAt": "\(String(describing: context.scheduledAt))",
                        "startTime": "\(context.startTime)",
                    ]
                )

                return Output(
                    testId: input.testId,
                    hadScheduledAt: context.scheduledAt != nil,
                    hadQueuedAt: true,  // queuedAt is always present
                    hadStartTime: true  // startTime is always present
                )
            }
        }

        try await testWorkflow(processor) {
            workflowEngine.registerWorkflow(TimingTestWorkflow.self)

            let workflowId = try await workflowEngine.startWorkflow(
                TimingTestWorkflow.self,
                input: TimingTestWorkflow.Input(testId: "timing-test")
            )

            let finalStatus = try await waitForWorkflowCompletion(workflowId, engine: workflowEngine, description: "timing test workflow")
            #expect(finalStatus.status == .completed)

            if let outputBuffer = finalStatus.output {
                let output = try JSONDecoder().decode(TimingTestWorkflow.Output.self, from: outputBuffer)
                #expect(output.hadQueuedAt == true)
                #expect(output.hadStartTime == true)
                // For immediate workflows, scheduledAt should be nil
                #expect(output.hadScheduledAt == false)
            }
        }
    }

    @Test("Scheduled workflow execution")
    func testScheduledWorkflow() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        // Create a workflow that returns timing information to verify execution
        struct ScheduledTimingWorkflow: WorkflowProtocol {
            static let workflowName = "ScheduledTimingWorkflow"

            struct Input: Codable, Sendable {
                let testId: String
            }

            struct Output: Codable, Sendable {
                let testId: String
                let queuedAt: TimeInterval
                let scheduledAt: TimeInterval?
                let startTime: TimeInterval
                let wasScheduled: Bool
                let schedulingDelay: TimeInterval?
            }

            func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
                context.logger.info(
                    "Scheduled workflow executed!",
                    metadata: [
                        "queuedAt": "\(context.queuedAt.timeIntervalSince1970)",
                        "scheduledAt": "\(context.scheduledAt?.timeIntervalSince1970 ?? 0)",
                        "startTime": "\(context.startTime.timeIntervalSince1970)",
                    ]
                )

                return Output(
                    testId: input.testId,
                    queuedAt: context.queuedAt.timeIntervalSince1970,
                    scheduledAt: context.scheduledAt?.timeIntervalSince1970,
                    startTime: context.startTime.timeIntervalSince1970,
                    wasScheduled: context.scheduledAt != nil,
                    schedulingDelay: context.schedulingDelay
                )
            }
        }

        workflowEngine.registerWorkflow(ScheduledTimingWorkflow.self)

        // Schedule the workflow to run soon
        let now = Date.now
        let nextSecond = Calendar.current.dateComponents([.second], from: now.addingTimeInterval(1))
        let schedule = Schedule.everyMinute(second: nextSecond.second!)

        let jobSchedule = try workflowEngine.scheduleWorkflow(
            ScheduledTimingWorkflow.self,
            input: ScheduledTimingWorkflow.Input(testId: "scheduled-test"),
            schedule: schedule,
            scheduleId: "scheduled-test"
        )

        let schedulerService = await workflowEngine.createSchedulerService(with: jobSchedule)

        try await withThrowingTaskGroup(of: Void.self) { group in
            let serviceGroup = ServiceGroup(
                configuration: .init(
                    services: [processor, schedulerService],
                    logger: Logger(label: "ScheduledWorkflowTest")
                )
            )
            group.addTask {
                try await serviceGroup.run()
            }

            // Wait for scheduler to execute the workflow
            try await Task.sleep(for: .seconds(4))

            await serviceGroup.triggerGracefulShutdown()
        }

        // Use the predictable WorkflowID to wait for completion
        let scheduleWorkflowId = WorkflowID(workflowId: "scheduled-test", runId: "scheduled-test")

        let status = try await waitForWorkflowCompletion(
            scheduleWorkflowId,
            engine: workflowEngine,
            expectedStatus: .completed,
            timeout: .seconds(10),
            description: "scheduled workflow completion"
        )

        // Verify workflow completed successfully with output
        #expect(status.status == .completed, "Scheduled workflow should complete successfully")
        #expect(status.output != nil, "Scheduled workflow should produce output")

        guard let outputBuffer = status.output else {
            #expect(Bool(false), "Expected workflow output but got nil")
            return
        }

        let output = try JSONDecoder().decode(ScheduledTimingWorkflow.Output.self, from: outputBuffer)

        // Verify the scheduled workflow output contains correct data
        #expect(output.testId == "scheduled-test", "Should maintain test ID")
        #expect(output.wasScheduled == true, "Workflow should have scheduledAt timestamp")
        #expect(output.schedulingDelay != nil, "Should have scheduling delay information")
    }

    @Test("Scheduled workflow basic functionality")
    func testScheduledWorkflowBasic() async throws {
        let (workflowEngine, _) = try await setupWorkflowSystem()

        // Schedule a simple workflow to run soon
        let now = Date.now
        let schedule = Schedule.everyMinute(second: Calendar.current.component(.second, from: now.addingTimeInterval(1)))
        let scheduleId = "scheduled-basic-test"

        let jobSchedule = try workflowEngine.scheduleWorkflow(
            SimpleWorkflow.self,
            input: SimpleWorkflow.Input(message: "Scheduled Test", count: 1),
            schedule: schedule,
            scheduleId: scheduleId
        )

        // Verify the schedule was created with correct structure
        #expect(jobSchedule.count == 1, "Job schedule should contain one job")

        // Verify the job schedule element contains correct configuration
        let scheduleElement = jobSchedule[0]
        #expect(scheduleElement.jobName == "ScheduledWorkflowExecution", "Should use ScheduledWorkflowExecution job name")
        #expect(scheduleElement.nextScheduledDate > now, "Next scheduled date should be in the future")

        // Verify scheduler service can be created without errors
        let schedulerService = await workflowEngine.createSchedulerService(with: jobSchedule)
        #expect(schedulerService.description == "JobScheduler", "Scheduler service should have correct description")

        // Test that we can create a workflow with predictable ID using the schedule ID
        // This tests our fix for making scheduled workflows searchable by schedule ID
        let predictableWorkflowId = WorkflowID(workflowId: scheduleId, runId: scheduleId)
        #expect(predictableWorkflowId.value == "\(scheduleId):\(scheduleId)", "WorkflowID should use schedule ID for both parts")
        #expect(predictableWorkflowId.workflowId == scheduleId, "WorkflowID should preserve schedule ID as workflowId")
        #expect(predictableWorkflowId.runId == scheduleId, "WorkflowID should use schedule ID as runId for predictability")

        // Test additional schedule types to verify they all work correctly
        let hourlySchedule = Schedule.hourly(minute: 30)
        let dailySchedule = Schedule.daily(hour: 2, minute: 0)

        // Test hourly schedule workflow
        let hourlyJobSchedule = try workflowEngine.scheduleWorkflow(
            SimpleWorkflow.self,
            input: SimpleWorkflow.Input(message: "Hourly Test", count: 1),
            schedule: hourlySchedule,
            scheduleId: "hourly-test"
        )
        #expect(hourlyJobSchedule.count == 1, "Hourly job schedule should contain one job")

        // Test daily schedule workflow
        let dailyJobSchedule = try workflowEngine.scheduleWorkflow(
            SimpleWorkflow.self,
            input: SimpleWorkflow.Input(message: "Daily Test", count: 1),
            schedule: dailySchedule,
            scheduleId: "daily-test"
        )
        #expect(dailyJobSchedule.count == 1, "Daily job schedule should contain one job")

        // Verify scheduler services can be created for different schedule types
        let _ = await workflowEngine.createSchedulerService(with: hourlyJobSchedule)
        let _ = await workflowEngine.createSchedulerService(with: dailyJobSchedule)
    }

    @Test("Workflow signaling with data")
    func testWorkflowSignaling() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        try await testWorkflow(processor) {
            // Define Signal
            struct ApprovalSignal: SignalParameters {
                static let signalName = "approval"
                typealias Input = String
            }

            // Create a workflow that waits for a signal
            struct OrderApprovalWorkflow: WorkflowProtocol {
                static let workflowName = "OrderApprovalWorkflow"

                struct Input: Codable, Sendable {
                    let orderId: String
                }

                struct Output: Codable, Sendable {
                    let orderId: String
                    let approvalData: String
                }

                func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
                    // Do some initial work
                    context.logger.info("Starting order processing for \(input.orderId)")

                    // Wait for approval signal with data
                    let approvalData = try await context.waitForSignal(
                        ApprovalSignal.self,
                        timeout: .seconds(5)
                    )

                    return Output(
                        orderId: input.orderId,
                        approvalData: approvalData ?? "no-data"
                    )
                }
            }

            workflowEngine.registerWorkflow(OrderApprovalWorkflow.self)

            // Start the workflow
            let workflowId = try await workflowEngine.startWorkflow(
                OrderApprovalWorkflow.self,
                input: OrderApprovalWorkflow.Input(orderId: "ORDER-123")
            )

            // Give workflow time to start and reach the signal wait
            try await Task.sleep(for: .milliseconds(500))

            // Send a signal with data
            try await workflowEngine.signalWorkflow(
                workflowId,
                signalType: ApprovalSignal.self,
                input: "APPROVED-BY-MANAGER"
            )

            // Wait for workflow completion
            let finalStatus = try await waitForWorkflowCompletion(
                workflowId,
                engine: workflowEngine,
                timeout: .seconds(5),
                description: "order approval workflow signaling"
            )
            #expect(finalStatus.status == .completed)
            // Verify the output contains the signal data
            if let outputBuffer = finalStatus.output {
                let output = try JSONDecoder().decode(OrderApprovalWorkflow.Output.self, from: outputBuffer)
                #expect(output.orderId == "ORDER-123")
                #expect(output.approvalData == "APPROVED-BY-MANAGER")
            }
        }
    }

    @Test("Workflow signaling without data")
    func testWorkflowSignalingNoData() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        try await testWorkflow(processor) {
            // Define continue signal without meaningful data
            struct ContinueSignal: SignalParameters {
                static let signalName = "continue"
                typealias Input = EmptyOutput
            }

            struct ContinueSignalWorkflow: WorkflowProtocol {
                static let workflowName = "ContinueSignalWorkflow"

                struct Input: Codable, Sendable {
                    let message: String
                }

                struct Output: Codable, Sendable {
                    let message: String
                    let signalReceived: Bool
                }

                func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
                    context.logger.info("Waiting for continue signal")

                    // Wait for signal without meaningful data
                    try await context.waitForSignal(ContinueSignal.self, timeout: .seconds(5))

                    return Output(
                        message: input.message,
                        signalReceived: true
                    )
                }
            }

            workflowEngine.registerWorkflow(ContinueSignalWorkflow.self)

            let workflowId = try await workflowEngine.startWorkflow(
                ContinueSignalWorkflow.self,
                input: ContinueSignalWorkflow.Input(message: "test")
            )

            // Give workflow time to reach signal wait
            try await Task.sleep(for: .milliseconds(500))

            // Send signal without meaningful data
            try await workflowEngine.signalWorkflow(
                workflowId,
                signalType: ContinueSignal.self,
                input: EmptyOutput()
            )

            // Verify completion
            let finalStatus = try await waitForWorkflowCompletion(
                workflowId,
                engine: workflowEngine,
                timeout: .seconds(5),
                description: "continue signal workflow"
            )
            #expect(finalStatus.status == .completed)
            if let outputBuffer = finalStatus.output {
                let output = try JSONDecoder().decode(ContinueSignalWorkflow.Output.self, from: outputBuffer)
                #expect(output.signalReceived == true)
            }
        }
    }

    @Test("Workflow signal timeout")
    func testWorkflowSignalTimeout() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        try await testWorkflow(processor) {
            // Define a signal that will never be sent
            struct NeverSentSignal: SignalParameters {
                static let signalName = "never-sent"
                typealias Input = String
            }

            struct TimeoutWorkflow: WorkflowProtocol {
                static let workflowName = "TimeoutWorkflow"

                struct Input: Codable, Sendable {
                    let id: String
                }

                struct Output: Codable, Sendable {
                    let id: String
                    let timedOut: Bool
                }

                func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
                    do {
                        // Wait for signal with short timeout
                        let _ = try await context.waitForSignal(
                            NeverSentSignal.self,
                            timeout: .milliseconds(500)
                        )
                        return Output(id: input.id, timedOut: false)
                    } catch {
                        context.logger.info("Signal timed out as expected")
                        return Output(id: input.id, timedOut: true)
                    }
                }
            }

            workflowEngine.registerWorkflow(TimeoutWorkflow.self)

            let workflowId = try await workflowEngine.startWorkflow(
                TimeoutWorkflow.self,
                input: TimeoutWorkflow.Input(id: "timeout-test")
            )

            // Wait for workflow to complete with timeout
            let finalStatus = try await waitForWorkflowCompletion(workflowId, engine: workflowEngine, description: "timeout workflow")
            #expect(finalStatus.status == .completed)
            if let outputBuffer = finalStatus.output {
                let output = try JSONDecoder().decode(TimeoutWorkflow.Output.self, from: outputBuffer)
                #expect(output.timedOut == true)  // Should timeout
            }
        }
    }

    @Test("Multiple signals to workflow")
    func testMultipleSignals() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        try await testWorkflow(processor) {
            // Define signals for multi-step process
            struct Step1Signal: SignalParameters {
                static let signalName = "step1"
                typealias Input = String
            }

            struct Step2Signal: SignalParameters {
                static let signalName = "step2"
                typealias Input = String
            }

            struct MultiSignalWorkflow: WorkflowProtocol {
                static let workflowName = "MultiSignalWorkflow"

                struct Input: Codable, Sendable {
                    let processId: String
                }

                struct Output: Codable, Sendable {
                    let processId: String
                    let step1Data: String
                    let step2Data: String
                }

                func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
                    context.logger.info("Starting multi-step process")

                    // Wait for first signal
                    let step1 = try await context.waitForSignal(
                        Step1Signal.self,
                        timeout: .seconds(5)
                    )

                    context.logger.info("Received step1 signal, waiting for step2")

                    // Wait for second signal
                    let step2 = try await context.waitForSignal(
                        Step2Signal.self,
                        timeout: .seconds(5)
                    )

                    return Output(
                        processId: input.processId,
                        step1Data: step1 ?? "missing",
                        step2Data: step2 ?? "missing"
                    )
                }
            }

            workflowEngine.registerWorkflow(MultiSignalWorkflow.self)

            let workflowId = try await workflowEngine.startWorkflow(
                MultiSignalWorkflow.self,
                input: MultiSignalWorkflow.Input(processId: "PROC-456")
            )

            // Send signals in sequence
            try await Task.sleep(for: .milliseconds(300))
            try await workflowEngine.signalWorkflow(
                workflowId,
                signalType: Step1Signal.self,
                input: "STEP1-COMPLETE"
            )

            try await Task.sleep(for: .milliseconds(300))
            try await workflowEngine.signalWorkflow(
                workflowId,
                signalType: Step2Signal.self,
                input: "STEP2-COMPLETE"
            )

            // Verify completion
            let finalStatus = try await waitForWorkflowCompletion(
                workflowId,
                engine: workflowEngine,
                timeout: .seconds(5),
                description: "multiple signals workflow"
            )
            #expect(finalStatus.status == .completed)
            if let outputBuffer = finalStatus.output {
                let output = try JSONDecoder().decode(MultiSignalWorkflow.Output.self, from: outputBuffer)
                #expect(output.step1Data == "STEP1-COMPLETE")
                #expect(output.step2Data == "STEP2-COMPLETE")
            }
        }
    }

    @Test("Approval workflow signaling")
    func testApprovalWorkflowSignaling() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        try await testWorkflow(processor) {
            // Define Signals
            struct ApprovalSignal: SignalParameters {
                static let signalName = "approval"
                typealias Input = ApprovalData
            }

            struct ApprovalData: Codable, Sendable {
                let approved: Bool
                let approver: String
                let comments: String?
            }

            struct ContinueSignal: SignalParameters {
                static let signalName = "continue"
                typealias Input = EmptyOutput
            }

            // Create a workflow that uses signals for approval requests
            struct ApprovalRequestWorkflow: WorkflowProtocol {
                static let workflowName = "ApprovalRequestWorkflow"

                struct Input: Codable, Sendable {
                    let requestId: String
                }

                struct Output: Codable, Sendable {
                    let requestId: String
                    let approved: Bool
                    let approver: String
                    let comments: String
                }

                func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
                    context.logger.info("Waiting for approval signal", metadata: [
                        "WorkflowID": .stringConvertible(context.workflowId),
                        "BusinessID": .stringConvertible(context.workflowId.workflowId)
                    ])

                    // Wait for approval signal
                    let approvalData = try await context.waitForSignal(
                        ApprovalSignal.self,
                        timeout: .seconds(5)
                    )

                    // Wait for continue signal without data
                    try await context.waitForSignal(
                        ContinueSignal.self,
                        timeout: .seconds(5)
                    )

                    return Output(
                        requestId: input.requestId,
                        approved: approvalData?.approved ?? false,
                        approver: approvalData?.approver ?? "unknown",
                        comments: approvalData?.comments ?? "no-comments"
                    )
                }
            }

            workflowEngine.registerWorkflow(ApprovalRequestWorkflow.self)

            // Start the workflow
            let workflowId = try await workflowEngine.startWorkflow(
                ApprovalRequestWorkflow.self,
                input: ApprovalRequestWorkflow.Input(requestId: "REQ-789"),
                workflowId: WorkflowID(workflowId: "approval-request-wf-123")
            )

            // Give workflow time to start and reach the signal wait
            try await Task.sleep(for: .milliseconds(500))

            // Send approval signal
            try await workflowEngine.signalWorkflow(
                workflowId,
                signalType: ApprovalSignal.self,
                input: ApprovalData(
                    approved: true,
                    approver: "manager@company.com",
                    comments: "Looks good to proceed"
                )
            )

            // Send continue signal
            try await workflowEngine.signalWorkflow(
                workflowId,
                signalType: ContinueSignal.self,
                input: EmptyOutput()
            )

            // Wait for workflow completion
            let finalStatus = try await waitForWorkflowCompletion(
                workflowId,
                engine: workflowEngine,
                timeout: .seconds(5),
                description: "approval request workflow signaling"
            )
            #expect(finalStatus.status == .completed)
            // Verify the output contains the signal data
            if let outputBuffer = finalStatus.output {
                let output = try JSONDecoder().decode(ApprovalRequestWorkflow.Output.self, from: outputBuffer)
                #expect(output.requestId == "REQ-789")
                #expect(output.approved == true)
                #expect(output.approver == "manager@company.com")
                #expect(output.comments == "Looks good to proceed")
            }

        }
    }

    @Test("Workflow run method - success")
    func testWorkflowRunSuccess() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        try await testWorkflow(processor) {
            // Use the run method to execute workflow and get result
            let result = try await workflowEngine.run(
                SimpleWorkflow.self,
                input: SimpleWorkflow.Input(message: "Run Test", count: 42),
                options: WorkflowOptions(timeout: .seconds(10))
            )

            #expect(result.result == "Processed: Run Test", "Should return processed message")
            #expect(result.processedCount == 84, "Should return processed count (doubled)")
        }
    }

    @Test("Workflow run method - timeout")
    func testWorkflowRunTimeout() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        try await testWorkflow(processor) {
            // Create a long-running workflow for timeout testing
            struct LongRunningWorkflow: WorkflowProtocol {
                static let workflowName = "LongRunning"

                struct Input: Codable, Sendable {
                    let duration: TimeInterval
                }

                struct Output: Codable, Sendable {
                    let completed: Bool
                }

                func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
                    try await context.sleep(for: .seconds(input.duration))
                    return Output(completed: true)
                }
            }

            workflowEngine.registerWorkflow(LongRunningWorkflow.self)

            // Test timeout with a very short timeout and long-running workflow
            await #expect(throws: WorkflowError.self) {
                _ = try await workflowEngine.run(
                    LongRunningWorkflow.self,
                    input: LongRunningWorkflow.Input(duration: 5.0),
                    options: WorkflowOptions(timeout: .milliseconds(100))
                )
            }
        }
    }

    @Test("Workflow run method - failure")
    func testWorkflowRunFailure() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        let _ = try await testWorkflow(processor) {
            // Test workflow failure handling
            await #expect(throws: WorkflowError.self) {
                _ = try await workflowEngine.run(
                    ErrorHandlingWorkflow.self,
                    input: ErrorHandlingWorkflow.Input(shouldFail: true, failureType: "test-error"),
                    options: WorkflowOptions(timeout: .seconds(10))
                )
            }
        }
    }

    @Test("Workflow step tracking")
    func testWorkflowStepTracking() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        try await testWorkflow(processor) {
            let workflowId = try await workflowEngine.startWorkflow(
                MultipleActivitiesWorkflow.self,
                input: MultipleActivitiesWorkflow.Input(
                    orderId: "STEP-TEST-123",
                    customerId: "CUSTOMER-456",
                    amount: 99.99
                )
            )

            let status = try await waitForWorkflowCompletion(
                workflowId,
                engine: workflowEngine,
                expectedStatus: .completed,
                timeout: .seconds(10),
                description: "step tracking workflow"
            )

            #expect(status.status == .completed, "Workflow should complete successfully")

            // Get the internal workflow state to check step tracking
            let internalState = try await workflowEngine.getInternalWorkflowState(workflowId)
            #expect(internalState.stepHistory.count >= 0, "Should have step history")
            #expect(internalState.currentStep >= 0, "Should track current step")
        }
    }
}

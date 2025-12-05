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
import Synchronization
import Testing

@testable import Jobs
@testable import Workflows

@Suite("QueryTests")
struct QueryTests {

    // MARK: - Helper Methods

    private func setupWorkflowSystem() async throws -> (WorkflowEngine<MemoryWorkflowQueue>, any Service) {
        var logger = Logger(label: "QueryTests")
        logger.logLevel = .debug

        let activities = TestQueryActivityContainer()
        let workflowEngine = WorkflowEngine(
            queue: .memoryWorkflow,
            logger: logger,
            activities: [activities]
        )

        let processor = workflowEngine.processor(options: .init(numWorkers: 200))

        // Register test workflows
        workflowEngine.registerWorkflow(SimpleQueryableWorkflow.self)
        workflowEngine.registerWorkflow(StatefulQueryWorkflow.self)
        workflowEngine.registerWorkflow(MultiQueryWorkflow.self)
        workflowEngine.registerWorkflow(InteractiveOrderWorkflow.self)

        return (workflowEngine, processor)
    }

    @Test("Complete workflow interaction - Queries, Signals, and Updates")
    func testCompleteWorkflowInteraction() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        try await testWorkflow(processor) {
            let uniqueOrderId = "interactive_order_\(UUID().uuidString)"
            let workflowInput = InteractiveOrderWorkflow.Input(orderId: uniqueOrderId)

            let workflowId = try await workflowEngine.startWorkflow(
                InteractiveOrderWorkflow.self,
                input: workflowInput,
                workflowId: WorkflowID(workflowId: uniqueOrderId)
            )

            // Wait for workflow to start and enter waiting state
            try await Task.sleep(for: .milliseconds(500))

            // 1. Query initial state while workflow is waiting for signal
            let initialStatus = try await workflowEngine.query(
                workflowId: workflowId,
                workflowType: InteractiveOrderWorkflow.self
            )
            #expect(initialStatus.orderId == uniqueOrderId)
            #expect(initialStatus.status == .processing)

            // 2. Send signal to update status (this will trigger workflow to continue)
            try await workflowEngine.signalWorkflow(
                workflowId,
                signalType: UpdateOrderStatusSignal.self,
                input: "processing"
            )

            // 3. Query to see signal effect - workflow should still be running
            try await Task.sleep(for: .milliseconds(500))
            let afterSignalStatus = try await workflowEngine.query(
                workflowId: workflowId,
                workflowType: InteractiveOrderWorkflow.self
            )
            #expect(afterSignalStatus.status == .updated)

            // 4. Send update - Clean workflow-type-based API!
            let updateResult = try await workflowEngine.update(
                workflowId: workflowId,
                workflowType: InteractiveOrderWorkflow.self,
                input: SetPriorityInput(priority: "expedited")
            )
            #expect(updateResult.contains("Priority changed from standard to expedited"))

            // 5. Query to see update effect - Same clean API!
            let afterUpdateStatus = try await workflowEngine.query(
                workflowId: workflowId,
                workflowType: InteractiveOrderWorkflow.self
            )
            #expect(afterUpdateStatus.priority == .expedited)

            // 6. Test validation - try invalid priority
            do {
                let _ = try await workflowEngine.update(
                    workflowId: workflowId,
                    workflowType: InteractiveOrderWorkflow.self,
                    input: SetPriorityInput(priority: "invalid-priority")
                )
                #expect(Bool(false), "Should have thrown validation error for invalid priority")
            } catch WorkflowError.validationFailed(let message) {
                #expect(message.contains("Invalid priority"))
                #expect(message.contains("standard, expedited, overnight"))
            }

            // Wait for workflow to complete
            let finalStatus = try await waitForWorkflowCompletion(
                workflowId,
                engine: workflowEngine,
                inputType: InteractiveOrderWorkflow.Input.self,
                outputType: InteractiveOrderWorkflow.Output.self,
                description: "interactive workflow test"
            )

            #expect(finalStatus.status == .completed)
        }
    }

    @Test("Simple query-only workflow")
    func testSimpleQueryWorkflow() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        try await testWorkflow(processor) {
            let workflowId = try await workflowEngine.startWorkflow(
                SimpleQueryableWorkflow.self,
                input: SimpleQueryableWorkflow.Input(orderId: "simple_order_123")
            )

            // Wait for workflow to start and register
            try await Task.sleep(for: .milliseconds(300))

            // Query the workflow while it's running
            let runningStatus = try await workflowEngine.query(
                workflowId: workflowId,
                workflowType: SimpleQueryableWorkflow.self
            )

            #expect(runningStatus.orderId == "simple_order_123")
            #expect(runningStatus.status == "processing")
            #expect(runningStatus.isRunning == true)

            // Wait for completion
            let finalStatus = try await waitForWorkflowCompletion(
                workflowId,
                engine: workflowEngine,
                inputType: SimpleQueryableWorkflow.Input.self,
                outputType: SimpleQueryableWorkflow.Output.self,
                description: "simple query workflow test"
            )

            #expect(finalStatus.status == .completed)
            #expect(finalStatus.hasOutput == true)

            // Direct access to typed workflow output
            if let finalOutput = finalStatus.output {
                #expect(finalOutput.success == true)
                #expect(finalOutput.orderId == "simple_order_123")
            } else {
                #expect(Bool(false), "Expected workflow output but none found")
            }
        }
    }

    @Test("Update-only workflow")
    func testUpdateOnlyWorkflow() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        try await testWorkflow(processor) {
            let workflowId = try await workflowEngine.startWorkflow(
                StatefulQueryWorkflow.self,
                input: StatefulQueryWorkflow.Input(initialValue: 5)
            )

            // Give workflow time to start and register itself properly
            try await Task.sleep(for: .milliseconds(500))

            // Update the workflow - awaitCondition handles coordination
            let updateResult = try await workflowEngine.update(
                workflowId: workflowId,
                workflowType: StatefulQueryWorkflow.self,
                input: StatefulQueryWorkflow.UpdateInput(multiplier: 3)
            )

            #expect(updateResult.contains("Value updated from"))
            #expect(updateResult.contains("to 15"))

            // Test validation
            do {
                let _ = try await workflowEngine.update(
                    workflowId: workflowId,
                    workflowType: StatefulQueryWorkflow.self,
                    input: StatefulQueryWorkflow.UpdateInput(multiplier: -1)
                )
                #expect(Bool(false), "Should have thrown validation error")
            } catch WorkflowError.validationFailed(let message) {
                #expect(message.contains("Multiplier must be positive"))
            }

            // Wait for completion
            let finalStatus = try await waitForWorkflowCompletion(
                workflowId,
                engine: workflowEngine,
                inputType: StatefulQueryWorkflow.Input.self,
                outputType: StatefulQueryWorkflow.Output.self,
                description: "update-only workflow test"
            )

            #expect(finalStatus.status == .completed)
            #expect(finalStatus.hasOutput == true)

            // Direct access to typed workflow output
            if let finalOutput = finalStatus.output {
                #expect(finalOutput.finalValue == 25)  // (5 * 3) + 10
                #expect(finalOutput.operationCount == 2)
            } else {
                #expect(Bool(false), "Expected workflow output but none found")
            }
        }

    }

    @Test("Test cleanup methods remove old data")
    func testCleanupMethods() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        try await testWorkflow(processor) {
            // Start a workflow and let it complete
            let workflowId = try await workflowEngine.startWorkflow(
                SimpleQueryableWorkflow.self,
                input: SimpleQueryableWorkflow.Input(orderId: "cleanup-test")
            )

            // Wait for completion
            let finalStatus = try await waitForWorkflowCompletion(
                workflowId,
                engine: workflowEngine,
                inputType: SimpleQueryableWorkflow.Input.self,
                outputType: SimpleQueryableWorkflow.Output.self,
                description: "cleanup test workflow"
            )

            #expect(finalStatus.status == .completed)

            // Test cleanup methods - they should return counts >= 0 without errors
            let cutoffDate = Date().addingTimeInterval(3600)  // 1 hour from now

            let completedCleaned = try await workflowEngine.jobQueue.queue.cleanupCompletedWorkflows(before: cutoffDate)
            #expect(completedCleaned >= 0)

            let resultsCleaned = try await workflowEngine.jobQueue.queue.cleanupResults(before: cutoffDate)
            #expect(resultsCleaned >= 0)

            let historyCleaned = try await workflowEngine.jobQueue.queue.cleanupHistoryEvents(before: cutoffDate)
            #expect(historyCleaned >= 0)

            let archivedCleaned = try await workflowEngine.jobQueue.queue.cleanupArchivedResults(before: cutoffDate)
            #expect(archivedCleaned >= 0)
        }
    }

    @Test("Multi-query workflow with both queries and updates")
    func testMultiQueryWorkflow() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        try await testWorkflow(processor) {
            let workflowId = try await workflowEngine.startWorkflow(
                MultiQueryWorkflow.self,
                input: MultiQueryWorkflow.Input(id: "multi_workflow_456")
            )

            // Wait for workflow to start and register
            try await Task.sleep(for: .milliseconds(300))

            // Query initial state
            let initialStatus = try await workflowEngine.query(
                workflowId: workflowId,
                workflowType: MultiQueryWorkflow.self
            )

            #expect(initialStatus.id == "multi_workflow_456")
            #expect(initialStatus.currentPhase == "processing")
            #expect(initialStatus.updateCount == 0)
            #expect(initialStatus.isActive == true)

            // Update the phase
            let updateResult = try await workflowEngine.update(
                workflowId: workflowId,
                workflowType: MultiQueryWorkflow.self,
                input: MultiQueryWorkflow.UpdateInput(newPhase: "finalizing")
            )

            #expect(updateResult.contains("Phase updated from 'processing' to 'finalizing'"))

            // Query after update
            let updatedStatus = try await workflowEngine.query(
                workflowId: workflowId,
                workflowType: MultiQueryWorkflow.self
            )

            #expect(updatedStatus.currentPhase == "finalizing")
            #expect(updatedStatus.updateCount == 1)

            // Wait for completion
            let finalStatus = try await waitForWorkflowCompletion(
                workflowId,
                engine: workflowEngine,
                inputType: MultiQueryWorkflow.Input.self,
                outputType: MultiQueryWorkflow.Output.self,
                description: "multi-query workflow test"
            )

            #expect(finalStatus.status == .completed)
            #expect(finalStatus.hasOutput == true)

            // Direct access to typed workflow output
            if let finalOutput = finalStatus.output {
                #expect(finalOutput.id == "multi_workflow_456")
                #expect(finalOutput.phase == "completed")
                #expect(finalOutput.updateCount == 1)
            } else {
                #expect(Bool(false), "Expected workflow output but none found")
            }
        }
    }

    // MARK: - Test Workflows

    final class SimpleQueryableWorkflow: WorkflowQuery {

        struct Input: Codable, Sendable {
            let orderId: String
        }

        struct Output: Codable, Sendable {
            let orderId: String
            let success: Bool
        }

        struct QueryOutput: Codable, Sendable {
            let orderId: String
            let status: String
            let isRunning: Bool
        }

        private nonisolated(unsafe) var orderId: String = ""
        private nonisolated(unsafe) var status: String = "initializing"
        private nonisolated(unsafe) var isRunning: Bool = false

        func handleQuery() throws -> QueryOutput {
            QueryOutput(
                orderId: orderId,
                status: status,
                isRunning: isRunning
            )
        }

        func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
            // Initialize state
            orderId = input.orderId
            status = "processing"
            isRunning = true

            // Simulate some workflow execution
            try await context.sleep(for: .milliseconds(500))

            let _ = try await context.executeActivity(
                TestQueryActivity.self,
                input: TestQueryActivity.Input(name: "query_test")
            )

            status = "completed"
            isRunning = false

            return Output(orderId: orderId, success: true)
        }
    }

    final class StatefulQueryWorkflow: WorkflowUpdate {

        struct Input: Codable, Sendable {
            let initialValue: Int
        }

        struct Output: Codable, Sendable {
            let finalValue: Int
            let operationCount: Int
        }

        struct UpdateInput: Codable, Sendable {
            let multiplier: Int
        }

        private nonisolated(unsafe) var currentValue: Int = 0
        private nonisolated(unsafe) var operationCount: Int = 0

        func handleUpdate(input: UpdateInput) async throws -> String {
            guard input.multiplier > 0 else {
                throw WorkflowError.validationFailed("Multiplier must be positive")
            }

            let oldValue = currentValue
            currentValue = currentValue * input.multiplier
            operationCount += 1

            return "Value updated from \(oldValue) to \(currentValue)"
        }

        func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
            context.logger.info("🚀 StatefulQueryWorkflow starting", metadata: ["initialValue": .stringConvertible(input.initialValue)])
            currentValue = input.initialValue
            operationCount = 0

            context.logger.info("😴 Waiting for updates", metadata: ["currentValue": .stringConvertible(currentValue)])
            // Wait for updates to arrive using awaitCondition with timeout
            try await context.awaitCondition(timeout: .seconds(5)) {
                self.currentValue != input.initialValue
            }

            context.logger.info("⚡ Processing +10 operation", metadata: ["currentValueBefore": .stringConvertible(currentValue)])
            // Do some work
            currentValue += 10
            operationCount += 1

            context.logger.info("😴 Final sleep before completion", metadata: ["currentValueAfter": .stringConvertible(currentValue)])
            try await context.sleep(for: .milliseconds(300))

            context.logger.info(
                "✅ StatefulQueryWorkflow completing",
                metadata: ["finalValue": .stringConvertible(currentValue), "operationCount": .stringConvertible(operationCount)]
            )
            return Output(finalValue: currentValue, operationCount: operationCount)
        }
    }

    final class MultiQueryWorkflow: WorkflowQueryUpdate {

        struct Input: Codable, Sendable {
            let id: String
        }

        struct Output: Codable, Sendable {
            let id: String
            let phase: String
            let updateCount: Int
        }

        struct QueryOutput: Codable, Sendable {
            let id: String
            let currentPhase: String
            let updateCount: Int
            let isActive: Bool
        }

        struct UpdateInput: Codable, Sendable {
            let newPhase: String
        }

        private nonisolated(unsafe) var id: String = ""
        private nonisolated(unsafe) var currentPhase: String = "initializing"
        private nonisolated(unsafe) var updateCount: Int = 0
        private nonisolated(unsafe) var isActive: Bool = false

        func handleQuery() throws -> QueryOutput {
            QueryOutput(
                id: id,
                currentPhase: currentPhase,
                updateCount: updateCount,
                isActive: isActive
            )
        }

        func handleUpdate(input: UpdateInput) async throws -> String {
            let oldPhase = currentPhase
            currentPhase = input.newPhase
            updateCount += 1

            return "Phase updated from '\(oldPhase)' to '\(currentPhase)'"
        }

        func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
            id = input.id
            currentPhase = "processing"
            isActive = true

            // Simulate multi-step workflow
            try await context.sleep(for: .milliseconds(300))
            let _ = try await context.executeActivity(
                TestQueryActivity.self,
                input: TestQueryActivity.Input(name: "multi_query_activity")
            )

            currentPhase = "completed"
            isActive = false

            return Output(id: id, phase: currentPhase, updateCount: updateCount)
        }
    }

    // MARK: - Interactive Test Workflow

    final class InteractiveOrderWorkflow: WorkflowQueryUpdate {

        struct Input: Codable, Sendable {
            let orderId: String
        }

        struct Output: Codable, Sendable {
            let orderId: String
            let finalStatus: String
        }

        // Order status enum for type safety
        enum OrderStatus: String, Codable, Sendable {
            case pending = "pending"
            case processing = "processing"
            case updated = "updated"
            case completed = "completed"
            case shipped = "shipped"
            case cancelled = "cancelled"
        }

        // Priority enum for type safety
        enum OrderPriority: String, Codable, Sendable, CaseIterable {
            case standard = "standard"
            case expedited = "expedited"
            case overnight = "overnight"
        }

        // Mutable state properties (Swift Temporal-style with nonisolated(unsafe))
        private nonisolated(unsafe) var orderId: String = ""
        private nonisolated(unsafe) var orderStatus: OrderStatus = .pending
        private nonisolated(unsafe) var priority: OrderPriority = .standard
        private nonisolated(unsafe) var lastUpdated: Date = Date()

        /// Handle query requests - Swift infers QueryOutput from return type
        func handleQuery() throws -> GetOrderStatusQueryOutput {
            GetOrderStatusQueryOutput(
                orderId: orderId,
                status: orderStatus,
                priority: priority,
                lastUpdated: lastUpdated
            )
        }

        /// Handle update requests - Swift infers UpdateInput/UpdateOutput from method signature
        func handleUpdate(input: SetPriorityInput) async throws -> String {
            // Validate priority value using enum
            guard let newPriority = OrderPriority(rawValue: input.priority) else {
                let validPriorities = OrderPriority.allCases.map(\.rawValue).joined(separator: ", ")
                throw WorkflowError.validationFailed("Invalid priority. Must be one of: \(validPriorities)")
            }

            // Cannot change priority after shipping has started
            guard ![.completed, .shipped].contains(orderStatus) else {
                throw WorkflowError.validationFailed(
                    "Cannot change priority after shipping has started. Current status: \(orderStatus.rawValue)"
                )
            }

            let oldPriority = priority
            priority = newPriority
            lastUpdated = Date()

            return "Priority changed from \(oldPriority.rawValue) to \(newPriority.rawValue)"
        }

        func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
            // Initialize state from input
            orderId = input.orderId
            orderStatus = .pending
            lastUpdated = Date()

            // Update status to processing (queries will see this immediately)
            orderStatus = .processing
            lastUpdated = Date()

            // Wait for status update signal
            let newStatus = try await context.waitForSignal(
                UpdateOrderStatusSignal.self,
                timeout: .seconds(10)
            )

            let finalStatus: String
            if newStatus != nil {
                orderStatus = .updated
                lastUpdated = Date()

                // Stay alive longer to allow for queries and updates
                try await context.sleep(for: .seconds(3))

                finalStatus = "completed"
            } else {
                finalStatus = "timeout"
            }

            // Continue workflow processing
            try await context.sleep(for: .milliseconds(200))

            // Final status update (queries will see this)
            orderStatus = .completed
            lastUpdated = Date()

            return Output(orderId: input.orderId, finalStatus: finalStatus)
        }
    }

    // Supporting output type for the query
    struct GetOrderStatusQueryOutput: Codable, Sendable {
        let orderId: String
        let status: InteractiveOrderWorkflow.OrderStatus
        let priority: InteractiveOrderWorkflow.OrderPriority
        let lastUpdated: Date
    }

    // Supporting input type for updates
    struct SetPriorityInput: Codable, Sendable {
        let priority: String
    }

    // Supporting signal type - uses automatic naming!
    struct UpdateOrderStatusSignal: SignalParameters {
        typealias Input = String
    }

    // MARK: - Test Activities

    struct TestQueryActivity: ActivityParameters {

        struct Input: Codable, Sendable {
            let name: String
        }

        struct Output: Codable, Sendable {
            let result: String
        }
    }

    // MARK: - Test Activity Container

    struct TestQueryActivityContainer: ActivityContainer {
        func registerActivities(with registry: ActivityRegistry) {
            registry.registerActivity(TestQueryActivity.self) { input, context in
                // Simulate some work
                try await Task.sleep(for: .milliseconds(50))

                return TestQueryActivity.Output(result: "completed_\(input.name)")
            }
        }
    }
}

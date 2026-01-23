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

@Suite("StepRetryPolicyTests")
struct StepRetryPolicyTests {

    @Test("StepRetryPolicy.exponentialJitter encodes and decodes correctly")
    func testExponentialJitterCodeable() async throws {
        let policy = StepRetryPolicy.exponentialJitter(
            maxAttempts: 15,
            maxBackoff: .seconds(300),
            minJitter: -0.3,
            maxJitter: 0.3
        )

        let encoded = try JSONEncoder().encode(policy)
        let decoded = try JSONDecoder().decode(StepRetryPolicy.self, from: encoded)

        guard case let .exponentialJitter(maxAttempts, maxBackoff, minJitter, maxJitter) = decoded else {
            Issue.record("Expected .exponentialJitter, got \(decoded)")
            return
        }

        #expect(maxAttempts == 15)
        #expect(maxBackoff == .seconds(300))
        #expect(minJitter == -0.3)
        #expect(maxJitter == 0.3)
    }

    @Test("StepRetryPolicy.fixedAttempts encodes and decodes correctly")
    func testFixedAttemptsCodeable() async throws {
        let policy = StepRetryPolicy.fixedAttempts(maxAttempts: 5)

        let encoded = try JSONEncoder().encode(policy)
        let decoded = try JSONDecoder().decode(StepRetryPolicy.self, from: encoded)

        guard case let .fixedAttempts(maxAttempts) = decoded else {
            Issue.record("Expected .fixedAttempts, got \(decoded)")
            return
        }

        #expect(maxAttempts == 5)
    }

    // MARK: - Strategy Conversion Tests

    @Test("StepRetryPolicy.dontRetry creates NeverRetryStrategy")
    func testDontRetryStrategy() async throws {
        let policy = StepRetryPolicy.dontRetry
        let strategy = policy.strategy

        // Should never retry regardless of attempt or error
        let error = RuntimeError("Test error")
        #expect(strategy.shouldRetry(attempt: 0, error: error) == false)
        #expect(strategy.shouldRetry(attempt: 1, error: error) == false)
        #expect(strategy.shouldRetry(attempt: 10, error: error) == false)

        // Should have zero backoff
        #expect(strategy.calculateBackoff(attempt: 1) == .seconds(0))
    }

    @Test("StepRetryPolicy.exponentialJitter creates WorkflowAwareJobRetryStrategy")
    func testExponentialJitterStrategy() async throws {
        let policy = StepRetryPolicy.exponentialJitter(maxAttempts: 3)
        let strategy = policy.strategy

        let retryableError = RuntimeError("Network timeout")

        // Should retry within limits for retryable errors
        #expect(strategy.shouldRetry(attempt: 0, error: retryableError) == true)
        #expect(strategy.shouldRetry(attempt: 1, error: retryableError) == true)
        #expect(strategy.shouldRetry(attempt: 2, error: retryableError) == true)
        #expect(strategy.shouldRetry(attempt: 3, error: retryableError) == false)

        // Should have increasing base backoff (test multiple times to account for jitter)
        var increasingBackoffCount = 0
        for _ in 0..<10 {
            let backoff1 = strategy.calculateBackoff(attempt: 1)
            let backoff2 = strategy.calculateBackoff(attempt: 2)
            if backoff2 > backoff1 {
                increasingBackoffCount += 1
            }
        }
        // Due to random jitter, expect at least 70% to show increase
        #expect(increasingBackoffCount >= 7)
    }

    // MARK: - WorkflowAwareJobRetryStrategy Tests

    @Test("WorkflowAwareJobRetryStrategy respects ApplicationError.isNonRetryable")
    func testWorkflowAwareRetryStrategyWithApplicationError() async throws {
        let policy = StepRetryPolicy.exponentialJitter(maxAttempts: 10)
        let strategy = policy.strategy

        // Non-retryable ApplicationError should never retry
        let nonRetryableError = ApplicationError.newNonRetryableFailure(
            "Invalid account",
            type: "InvalidAccount"
        )
        #expect(strategy.shouldRetry(attempt: 0, error: nonRetryableError) == false)
        #expect(strategy.shouldRetry(attempt: 5, error: nonRetryableError) == false)

        // Retryable ApplicationError should follow user's retry count
        let retryableError = ApplicationError(
            message: "Database timeout",
            type: "DatabaseError",
            isNonRetryable: false
        )
        #expect(strategy.shouldRetry(attempt: 0, error: retryableError) == true)
        #expect(strategy.shouldRetry(attempt: 5, error: retryableError) == true)
        #expect(strategy.shouldRetry(attempt: 10, error: retryableError) == false)  // Exceeds maxAttempts
    }

    @Test("WorkflowAwareJobRetryStrategy handles WorkflowError cases")
    func testWorkflowAwareRetryStrategyWithWorkflowError() async throws {
        let policy = StepRetryPolicy.exponentialJitter(maxAttempts: 5)
        let strategy = policy.strategy

        // Non-retryable WorkflowError cases
        let validationError = WorkflowError.validationFailed("Invalid input")
        #expect(strategy.shouldRetry(attempt: 0, error: validationError) == false)

        let unknownTypeError = WorkflowError.unknownWorkflowType("MissingWorkflow")
        #expect(strategy.shouldRetry(attempt: 0, error: unknownTypeError) == false)

        let cancelledError = WorkflowError.workflowCancelled(WorkflowID("test"))
        #expect(strategy.shouldRetry(attempt: 0, error: cancelledError) == false)

        // Retryable WorkflowError cases
        let timeoutError = WorkflowError.activityTimeout("ActivityTimeout")
        #expect(strategy.shouldRetry(attempt: 0, error: timeoutError) == true)
        #expect(strategy.shouldRetry(attempt: 3, error: timeoutError) == true)
        #expect(strategy.shouldRetry(attempt: 5, error: timeoutError) == false)  // Exceeds maxAttempts
    }

    // MARK: - Convenience Static Properties Tests

    @Test("StepRetryPolicy convenience properties work correctly")
    func testConvenienceProperties() async throws {
        // Test .never
        let neverPolicy = StepRetryPolicy.never
        guard case .dontRetry = neverPolicy else {
            Issue.record("Expected .dontRetry for .never")
            return
        }

        // Test .default
        let defaultPolicy = StepRetryPolicy.default
        guard case let .exponentialJitter(maxAttempts, _, _, _) = defaultPolicy else {
            Issue.record("Expected .exponentialJitter for .default")
            return
        }
        #expect(maxAttempts == 4)

        // Test .aggressive
        let aggressivePolicy = StepRetryPolicy.aggressive
        guard case let .exponentialJitter(aggressiveAttempts, _, _, _) = aggressivePolicy else {
            Issue.record("Expected .exponentialJitter for .aggressive")
            return
        }
        #expect(aggressiveAttempts == 10)

        // Test .conservative
        let conservativePolicy = StepRetryPolicy.conservative
        guard case let .exponentialJitter(conservativeAttempts, _, _, _) = conservativePolicy else {
            Issue.record("Expected .exponentialJitter for .conservative")
            return
        }
        #expect(conservativeAttempts == 2)
    }

    // MARK: - ActivityExecutionJob Integration Tests

    @Test("ActivityExecutionJob stores StepRetryPolicy correctly")
    func testActivityExecutionJobWithRetryPolicy() async throws {
        let retryPolicy = StepRetryPolicy.exponentialJitter(maxAttempts: 15)

        let activityJob = try ActivityExecutionJob(
            activityId: ActivityID("test-activity"),
            workflowId: WorkflowID("test-workflow"),
            activityName: "TestActivity",
            input: ["test": "data"],
            timeout: .seconds(30),
            retryPolicy: retryPolicy,
            workflowType: "TestWorkflow",
            stepIndex: 1
        )

        // Verify the retry policy is stored
        guard case let .exponentialJitter(maxAttempts, _, _, _) = activityJob.retryPolicy else {
            Issue.record("Expected exponentialJitter policy")
            return
        }
        #expect(maxAttempts == 15)
    }

    @Test("ActivityExecutionJob is Codable with StepRetryPolicy")
    func testActivityExecutionJobCodeable() async throws {
        let retryPolicy = StepRetryPolicy.fixedAttempts(maxAttempts: 8)

        let originalJob = try ActivityExecutionJob(
            activityId: ActivityID("activity-123"),
            workflowId: WorkflowID("workflow-456"),
            activityName: "ProcessOrder",
            input: ["orderId": "order-789"],
            timeout: .seconds(60),
            retryPolicy: retryPolicy,
            workflowType: "OrderWorkflow",
            stepIndex: 2
        )

        // Test encoding/decoding
        let encoded = try JSONEncoder().encode(originalJob)
        let decodedJob = try JSONDecoder().decode(ActivityExecutionJob.self, from: encoded)

        // Verify all fields match
        #expect(decodedJob.activityId == originalJob.activityId)
        #expect(decodedJob.workflowId == originalJob.workflowId)
        #expect(decodedJob.activityName == originalJob.activityName)
        #expect(decodedJob.timeout == originalJob.timeout)
        #expect(decodedJob.workflowType == originalJob.workflowType)
        #expect(decodedJob.stepIndex == originalJob.stepIndex)

        // Verify retry policy
        guard case let .fixedAttempts(maxAttempts) = decodedJob.retryPolicy else {
            Issue.record("Expected fixedAttempts policy")
            return
        }
        #expect(maxAttempts == 8)
    }

    // MARK: - Real-World Scenario Tests

    @Test("High retry count for critical operations")
    func testCriticalOperationRetryBehavior() async throws {
        let criticalPolicy = StepRetryPolicy.exponentialJitter(maxAttempts: 25)
        let strategy = criticalPolicy.strategy

        // Business error should never retry (even with 25 attempts configured)
        let businessError = ApplicationError.newNonRetryableFailure(
            "Customer account is blocked",
            type: "AccountBlocked"
        )
        #expect(strategy.shouldRetry(attempt: 0, error: businessError) == false)
        #expect(strategy.shouldRetry(attempt: 10, error: businessError) == false)

        // System error should retry up to 25 times
        let systemError = RuntimeError("Service temporarily unavailable")
        for attempt in 0..<25 {
            #expect(strategy.shouldRetry(attempt: attempt, error: systemError) == true)
        }
        #expect(strategy.shouldRetry(attempt: 25, error: systemError) == false)
    }

    @Test("No retry policy for validation operations")
    func testValidationOperationBehavior() async throws {
        let validationPolicy = StepRetryPolicy.never
        let strategy = validationPolicy.strategy

        // Should never retry any error type
        let validationError = RuntimeError("Invalid email format")
        let systemError = RuntimeError("Database connection failed")
        let businessError = ApplicationError.newNonRetryableFailure("Invalid input", type: "ValidationError")

        #expect(strategy.shouldRetry(attempt: 0, error: validationError) == false)
        #expect(strategy.shouldRetry(attempt: 0, error: systemError) == false)
        #expect(strategy.shouldRetry(attempt: 0, error: businessError) == false)

        // Should have zero backoff
        #expect(strategy.calculateBackoff(attempt: 1) == .seconds(0))
    }

    @Test("Custom backoff parameters work correctly")
    func testCustomBackoffParameters() async throws {
        let customPolicy = StepRetryPolicy.fixedAttempts(maxAttempts: 5)
        let strategy = customPolicy.strategy

        _ = RuntimeError("Test error")

        // Test that retry attempts are respected
        let error = RuntimeError("Test error")
        #expect(strategy.shouldRetry(attempt: 0, error: error) == true)
        #expect(strategy.shouldRetry(attempt: 4, error: error) == true)
        #expect(strategy.shouldRetry(attempt: 5, error: error) == false)

        // Test exponential backoff pattern (FixedAttempts uses ExponentialJitterJobRetryStrategy internally)
        let backoff1 = strategy.calculateBackoff(attempt: 1)
        let backoff2 = strategy.calculateBackoff(attempt: 2)

        // Should have some backoff delay
        #expect(backoff1 > .seconds(0))
        #expect(backoff2 > backoff1)
    }
}

// MARK: - Test Helper Types

private struct RuntimeError: Error {
    let message: String

    init(_ message: String) {
        self.message = message
    }
}

extension RuntimeError: LocalizedError {
    var errorDescription: String? { message }
}

// MARK: - Integration Tests with Real Workflows

extension StepRetryPolicyTests {

    // MARK: - Helper Methods

    private func setupWorkflowSystem() async throws -> (WorkflowEngine<MemoryWorkflowQueue>, any Service) {
        let logger = Logger(label: "StepRetryPolicyTests")

        let activities = TestRetryActivityContainer()
        let workflowEngine = WorkflowEngine(
            queue: .memoryWorkflow,
            logger: logger,
            activities: [activities]
        )

        let processor = workflowEngine.processor(options: .init(numWorkers: 10))

        // Register test workflows
        workflowEngine.registerWorkflow(RetryTestWorkflow.self)
        workflowEngine.registerWorkflow(HighRetryWorkflow.self)

        return (workflowEngine, processor)
    }

    @Test("Workflow with non-retryable activity error fails immediately")
    func testWorkflowWithNonRetryableActivityError() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        // Execute workflow - should fail immediately without retries
        do {
            _ = try await testWorkflow(processor) {
                try await workflowEngine.runWorkflow(
                    RetryTestWorkflow.self,
                    input: RetryTestWorkflow.Input(
                        shouldFailWithNonRetryable: true,
                        maxExpectedAttempts: 5
                    )
                )
            }
            Issue.record("Expected workflow to fail")
        } catch {
            // Verify it failed with our non-retryable error
            let errorMessage = error.localizedDescription
            #expect(errorMessage.contains("Workflow failed"))
        }
    }

    @Test("Workflow with retryable activity error respects user retry count")
    func testWorkflowWithRetryableActivityError() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        // Execute workflow - should retry up to configured amount
        do {
            _ = try await testWorkflow(processor) {
                try await workflowEngine.runWorkflow(
                    RetryTestWorkflow.self,
                    input: RetryTestWorkflow.Input(
                        shouldFailWithNonRetryable: false,
                        maxExpectedAttempts: 3
                    )
                )
            }
            Issue.record("Expected workflow to fail after retries")
        } catch {
            // Should fail after attempting retries
            let errorMessage = error.localizedDescription
            #expect(errorMessage.contains("Workflow failed"))
        }
    }

    @Test("Workflow with custom high retry count for critical activity")
    func testWorkflowWithHighRetryCount() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        // Execute workflow - should succeed after several retries
        let result = try await testWorkflow(processor) {
            try await workflowEngine.runWorkflow(
                HighRetryWorkflow.self,
                input: HighRetryWorkflow.Input(failureCount: 3)
            )
        }

        #expect(result.success == true)
        #expect(result.attemptsUsed > 1)
    }
}

// MARK: - Test Workflows and Activities

/// Test workflow that demonstrates retry behavior
struct RetryTestWorkflow: WorkflowProtocol {
    struct Input: Codable, Sendable {
        let shouldFailWithNonRetryable: Bool
        let maxExpectedAttempts: Int
    }

    struct Output: Codable, Sendable {
        let success: Bool
        let attemptsUsed: Int
    }

    func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
        let options = ActivityOptions(
            startToCloseTimeout: .seconds(10),
            retryPolicy: StepRetryPolicy.exponentialJitter(maxAttempts: input.maxExpectedAttempts)
        )

        if input.shouldFailWithNonRetryable {
            // This should fail immediately without retries
            _ = try await context.executeActivity(
                TestNonRetryableActivity.self,
                input: TestNonRetryableInput(),
                options: options
            )
        } else {
            // This should retry according to the policy
            _ = try await context.executeActivity(
                TestRetryableActivity.self,
                input: TestRetryableInput(),
                options: options
            )
        }

        return Output(success: true, attemptsUsed: 1)
    }
}

/// Workflow with high retry count for critical operations
struct HighRetryWorkflow: WorkflowProtocol {
    struct Input: Codable, Sendable {
        let failureCount: Int
    }

    struct Output: Codable, Sendable {
        let success: Bool
        let attemptsUsed: Int
    }

    func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
        let options = ActivityOptions(
            startToCloseTimeout: .seconds(5),
            retryPolicy: StepRetryPolicy.exponentialJitter(maxAttempts: 25)
        )

        let result = try await context.executeActivity(
            TestEventualSuccessActivity.self,
            input: TestEventualSuccessInput(failureCount: input.failureCount),
            options: options
        )

        return Output(success: result.success, attemptsUsed: result.attemptsUsed)
    }
}

// MARK: - Activity Definitions

struct TestNonRetryableInput: Codable, Sendable {}

struct TestNonRetryableOutput: Codable, Sendable {
    let success: Bool
}

struct TestNonRetryableActivity: ActivityParameters {
    typealias Input = TestNonRetryableInput
    typealias Output = TestNonRetryableOutput
}

struct TestRetryableInput: Codable, Sendable {}

struct TestRetryableOutput: Codable, Sendable {
    let success: Bool
}

struct TestRetryableActivity: ActivityParameters {
    typealias Input = TestRetryableInput
    typealias Output = TestRetryableOutput
}

struct TestEventualSuccessInput: Codable, Sendable {
    let failureCount: Int
}

struct TestEventualSuccessOutput: Codable, Sendable {
    let success: Bool
    let attemptsUsed: Int
}

struct TestEventualSuccessActivity: ActivityParameters {
    typealias Input = TestEventualSuccessInput
    typealias Output = TestEventualSuccessOutput
}

// MARK: - Test Activity Container

actor AttemptCounter {
    private var counters: [String: Int] = [:]

    func incrementAndGet(for key: String) -> Int {
        let current = counters[key, default: 0] + 1
        counters[key] = current
        return current
    }

    func reset() {
        counters.removeAll()
    }
}

struct TestRetryActivityContainer: ActivityContainer {
    static let attemptCounter = AttemptCounter()

    func registerActivities(with registry: ActivityRegistry) {
        // Activity that always throws non-retryable error
        registry.registerActivity(TestNonRetryableActivity.self) {
            (input: TestNonRetryableInput, context: ActivityExecutionContext) async throws -> TestNonRetryableOutput in

            throw ApplicationError.newNonRetryableFailure(
                "Account is blocked",
                type: "AccountBlocked"
            )
        }

        // Activity that throws retryable errors
        registry.registerActivity(TestRetryableActivity.self) {
            (input: TestRetryableInput, context: ActivityExecutionContext) async throws -> TestRetryableOutput in

            throw ApplicationError(
                message: "Network timeout",
                type: "NetworkError",
                isNonRetryable: false
            )
        }

        // Activity that succeeds after a certain number of failures
        registry.registerActivity(TestEventualSuccessActivity.self) {
            (input: TestEventualSuccessInput, context: ActivityExecutionContext) async throws -> TestEventualSuccessOutput in

            let activityKey = context.info.activityId

            // Track attempts for this specific activity instance
            let currentAttempt = await TestRetryActivityContainer.attemptCounter.incrementAndGet(for: activityKey)

            if currentAttempt <= input.failureCount {
                // Fail with retryable error
                throw ApplicationError(
                    message: "Temporary service failure (attempt \(currentAttempt))",
                    type: "TemporaryFailure",
                    isNonRetryable: false
                )
            }

            // Success after enough attempts
            return TestEventualSuccessOutput(success: true, attemptsUsed: currentAttempt)
        }
    }
}

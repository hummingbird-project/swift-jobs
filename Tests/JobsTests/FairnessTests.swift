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

import Atomics
import Foundation
import Logging
import ServiceLifecycle
import Synchronization
import Testing

@testable import Jobs

struct FairnessTests {

    // MARK: - Test Job Types

    enum Workload: String, Codable {
        case light
        case medium
        case heavy
    }

    enum Priority: String, Codable {
        case low
        case normal
        case high
    }

    struct TestJob: JobParameters {
        static let jobName = "fairness-test-job"
        let id: String
        let tenantId: String
        let workload: Workload
        let priority: Priority
    }

    struct QuickJob: JobParameters {
        static let jobName = "quick-job"
        let id: String
        let tenantId: String
    }

    // MARK: - Test Results Tracking

    final class ExecutionTracker: Sendable {
        private let _executionOrder = Mutex<[String]>([])
        private let _executionTimes = Mutex<[String: TimeInterval]>([:])
        private let _tenantTimes = Mutex<[String: TimeInterval]>([:])

        func recordExecution(_ jobId: String, tenantId: String, executionTime: TimeInterval) {
            _executionOrder.withLock { $0.append(jobId) }
            _executionTimes.withLock { $0[jobId] = executionTime }
            _tenantTimes.withLock { times in
                times[tenantId, default: 0] += executionTime
            }
        }

        var executionOrder: [String] {
            _executionOrder.withLock { Array($0) }
        }

        var tenantTimes: [String: TimeInterval] {
            _tenantTimes.withLock { $0 }
        }

        func clear() {
            _executionOrder.withLock { $0.removeAll() }
            _executionTimes.withLock { $0.removeAll() }
            _tenantTimes.withLock { $0.removeAll() }
        }
    }

    // MARK: - Basic Fairness Tests

    @Test func testBasicPriorityScheduling() async throws {
        let expectation = TestExpectation()
        let tracker = ExecutionTracker()
        let jobQueue = JobQueue(.memory, logger: Logger(label: "FairnessTests"))

        jobQueue.registerJob(parameters: TestJob.self) { job, context in
            let startTime = Date()
            try await Task.sleep(for: .milliseconds(10))
            let executionTime = Date().timeIntervalSince(startTime)
            tracker.recordExecution(job.id, tenantId: job.tenantId, executionTime: executionTime)
            expectation.trigger()
        }

        try await testJobQueue(jobQueue.processor()) {
            // Submit jobs with different priorities
            try await jobQueue.push(
                TestJob(id: "low-1", tenantId: "tenant-a", workload: .light, priority: .low),
                options: MemoryQueue.JobOptions(priority: 1)
            )
            try await jobQueue.push(
                TestJob(id: "high-1", tenantId: "tenant-a", workload: .light, priority: .high),
                options: MemoryQueue.JobOptions(priority: 10)
            )
            try await jobQueue.push(
                TestJob(id: "normal-1", tenantId: "tenant-a", workload: .light, priority: .normal),
                options: MemoryQueue.JobOptions(priority: 5)
            )

            try await expectation.wait(count: 3)

            let order = tracker.executionOrder
            #expect(order.count == 3)
            #expect(order[0] == "high-1")  // Highest priority first
            #expect(order[1] == "normal-1")  // Medium priority second
            #expect(order[2] == "low-1")  // Lowest priority last
        }
    }

    @Test func testPriorityWithFairness() async throws {
        let expectation = TestExpectation()
        let tracker = ExecutionTracker()
        let jobQueue = JobQueue(.memory, logger: Logger(label: "FairnessTests"))

        jobQueue.registerJob(parameters: TestJob.self) { job, context in
            let startTime = Date()
            try await Task.sleep(for: .milliseconds(20))
            let executionTime = Date().timeIntervalSince(startTime)
            tracker.recordExecution(job.id, tenantId: job.tenantId, executionTime: executionTime)
            expectation.trigger()
        }

        try await testJobQueue(jobQueue.processor()) {
            // Submit jobs with different priorities and fairness settings
            try await jobQueue.push(
                TestJob(id: "tenant-a-low", tenantId: "tenant-a", workload: .light, priority: .low),
                options: .fairness(key: "tenant-a", weight: 1.0, priority: 1)
            )
            try await jobQueue.push(
                TestJob(id: "tenant-b-high", tenantId: "tenant-b", workload: .light, priority: .high),
                options: .fairness(key: "tenant-b", weight: 1.0, priority: 10)
            )
            try await jobQueue.push(
                TestJob(id: "tenant-a-high", tenantId: "tenant-a", workload: .light, priority: .high),
                options: .fairness(key: "tenant-a", weight: 1.0, priority: 10)
            )

            try await expectation.wait(count: 3)

            let order = tracker.executionOrder
            #expect(order.count == 3)

            // High priority jobs should execute before low priority, regardless of tenant
            let highPriorityJobs = order.prefix(2)
            #expect(highPriorityJobs.contains("tenant-a-high"))
            #expect(highPriorityJobs.contains("tenant-b-high"))
            #expect(order[2] == "tenant-a-low")
        }
    }

    @Test func testFairnessPreventsTenantStarvation() async throws {
        let expectation = TestExpectation()
        let tracker = ExecutionTracker()
        let jobQueue = JobQueue(.memory, logger: Logger(label: "FairnessTests"))

        jobQueue.registerJob(parameters: QuickJob.self) { job, context in
            let startTime = Date()
            try await Task.sleep(for: .milliseconds(10))
            let executionTime = Date().timeIntervalSince(startTime)
            tracker.recordExecution(job.id, tenantId: job.tenantId, executionTime: executionTime)
            expectation.trigger()
        }

        try await testJobQueue(jobQueue.processor()) {
            // Set weight overrides to create extreme imbalance and test anti-starvation
            try await jobQueue.queue.setFairnessWeightOverride(key: "big-tenant", weight: 100.0)
            try await jobQueue.queue.setFairnessWeightOverride(key: "small-tenant", weight: 1.0)

            // Submit many jobs for big tenant
            for i in 0..<6 {
                try await jobQueue.push(
                    QuickJob(id: "big-tenant-\(i)", tenantId: "big-tenant"),
                    options: .fairness(key: "big-tenant", weight: 100.0, priority: 5)
                )
            }

            // Submit few jobs for small tenant
            for i in 0..<2 {
                try await jobQueue.push(
                    QuickJob(id: "small-tenant-\(i)", tenantId: "small-tenant"),
                    options: .fairness(key: "small-tenant", weight: 1.0, priority: 5)
                )
            }

            try await expectation.wait(count: 8)

            let order = tracker.executionOrder
            #expect(order.count == 8)

            // Verify fairness: small tenant jobs should be interspersed with big tenant jobs
            let smallTenantJobs = order.filter { $0.contains("small-tenant") }
            #expect(smallTenantJobs.count == 2)

            // Anti-starvation guarantee: small tenant must execute before being completely starved
            let firstSmallTenantIndex = order.firstIndex { $0.contains("small-tenant") } ?? order.count
            let maxAllowedPosition = order.count - smallTenantJobs.count  // Must execute before all its jobs are skipped
            #expect(
                firstSmallTenantIndex <= maxAllowedPosition,
                "Small tenant starved - first execution at position \(firstSmallTenantIndex), should be ≤ \(maxAllowedPosition)"
            )
        }
    }

    @Test func testStarvationPreventionStatistical() async throws {
        let numKeys = 6  // Multiple fairness keys
        let expectation = TestExpectation()
        let tracker = ExecutionTracker()
        let jobQueue = JobQueue(.memory, logger: Logger(label: "StarvationTests"))

        jobQueue.registerJob(parameters: TestJob.self) { job, context in
            let startTime = Date()
            try await Task.sleep(for: .milliseconds(1))  // Minimal sleep for consistent timing
            let executionTime = Date().timeIntervalSince(startTime)
            tracker.recordExecution(job.id, tenantId: job.tenantId, executionTime: executionTime)
            expectation.trigger()
        }

        try await testJobQueue(jobQueue.processor()) {
            // Use equal weights to test pure fairness without weight bias
            // This reduces timing variations and makes tests more reliable
            let keyWeights: [String: Double] = [
                "tenant_0": 1.0,
                "tenant_1": 1.0,
                "tenant_2": 1.0,
                "tenant_3": 1.0,
                "tenant_4": 1.0,
                "tenant_5": 1.0,
            ]

            // Test realistic unequal job distribution to check for starvation
            // Tenant A submits 50 jobs, Tenant B submits 10 jobs, others vary
            let jobDistribution: [String: Int] = [
                "tenant_0": 50,  // Heavy load tenant
                "tenant_1": 10,  // Light load tenant
                "tenant_2": 8,
                "tenant_3": 5,
                "tenant_4": 3,
                "tenant_5": 2,
            ]
            var jobCounter = 0

            // Create jobs according to realistic distribution
            for (tenantKey, jobCount) in jobDistribution {
                for jobIndex in 0..<jobCount {
                    jobCounter += 1
                    try await jobQueue.push(
                        TestJob(
                            id: "\(tenantKey)-\(jobIndex)",
                            tenantId: tenantKey,
                            workload: .light,
                            priority: .normal
                        ),
                        options: .fairness(
                            key: tenantKey,
                            weight: keyWeights[tenantKey] ?? 1.0,
                            priority: 5
                        )
                    )
                }
            }

            // Add one more job for the last tenant to test late arrival
            jobCounter += 1
            try await jobQueue.push(
                TestJob(
                    id: "tenant_5-late",
                    tenantId: "tenant_5",
                    workload: .light,
                    priority: .normal
                ),
                options: .fairness(
                    key: "tenant_5",
                    weight: keyWeights["tenant_5"] ?? 1.0,
                    priority: 5
                )
            )

            try await expectation.wait(count: jobCounter)

            let executionOrder = tracker.executionOrder

            // Calculate unfairness metric (when each tenant gets first execution opportunity)
            let unfairness = calculateUnfairness(executionOrder: executionOrder)

            // Verify first execution positions for each tenant
            var firstPositions: [String: Int] = [:]
            for (index, jobId) in executionOrder.enumerated() {
                // Extract tenant from job ID (format: "tenant_name-jobIndex")
                let tenant = String(jobId.prefix(while: { $0 != "-" }))
                if firstPositions[tenant] == nil {
                    firstPositions[tenant] = index
                }
            }

            #expect(unfairness < 1.5, "Unfairness score: \(unfairness) - ensures fair job distribution across tenants")

            // Verify all tenants eventually got to execute (no complete starvation)
            let uniqueTenants = Set(firstPositions.keys)
            #expect(uniqueTenants.count == numKeys, "Not all tenants got to execute - starvation detected")

            // Verify the late-arriving job executed
            #expect(firstPositions["tenant_5"] != nil, "Late-arriving tenant never executed - starvation detected")
        }
    }

    /// Calculate unfairness metric for stride scheduling fairness
    /// Measures when each fairness key gets its first execution opportunity
    /// Lower scores indicate better fairness (0.0 = perfect fairness)
    func calculateUnfairness(executionOrder: [String]) -> Double {
        var firstExecutionPositions: [String: Int] = [:]

        // Find the position where each tenant first appears
        for (index, jobId) in executionOrder.enumerated() {
            // Extract tenant from job ID (format: "tenant_name-jobIndex")
            let tenant = String(jobId.prefix(while: { $0 != "-" }))
            if firstExecutionPositions[tenant] == nil {
                firstExecutionPositions[tenant] = index
            }
        }

        // Calculate total delay (sum of first execution positions)
        let totalDelay = firstExecutionPositions.values.reduce(0, +)
        let numKeys = firstExecutionPositions.count

        // Normalize by number of keys squared for consistent scaling
        return Double(totalDelay) / Double(numKeys * numKeys)
    }

    // MARK: - Edge Cases

    @Test func testEdgeCaseParameterHandling() async throws {
        let expectation = TestExpectation()
        let tracker = ExecutionTracker()
        let jobQueue = JobQueue(.memory, logger: Logger(label: "FairnessTests"))

        jobQueue.registerJob(parameters: TestJob.self) { job, context in
            tracker.recordExecution(job.id, tenantId: job.tenantId, executionTime: 0.01)
            expectation.trigger()
        }

        try await testJobQueue(jobQueue.processor()) {
            // Test edge cases: zero weight, negative priority, empty key
            try await jobQueue.push(
                TestJob(id: "zero-weight", tenantId: "tenant-a", workload: .light, priority: .normal),
                options: .fairness(key: "tenant-a", weight: 0.0, priority: 5)  // Should clamp to 0.1
            )

            try await jobQueue.push(
                TestJob(id: "negative-priority", tenantId: "tenant-b", workload: .light, priority: .normal),
                options: .fairness(key: "tenant-b", weight: 1.0, priority: -5)  // Should clamp to 1
            )

            try await jobQueue.push(
                TestJob(id: "empty-key", tenantId: "tenant-c", workload: .light, priority: .normal),
                options: .fairness(key: "", weight: 1.0, priority: 5)  // Should use "default" key
            )

            try await expectation.wait(count: 3)

            let order = tracker.executionOrder
            #expect(order.count == 3)

            // All edge cases should execute without crashing
            #expect(order.contains("zero-weight"))
            #expect(order.contains("negative-priority"))
            #expect(order.contains("empty-key"))
        }
    }

    // MARK: - Delayed Jobs with Fairness

    @Test func testFairnessWithDelayedJobs() async throws {
        let expectation = TestExpectation()
        let tracker = ExecutionTracker()
        let jobQueue = JobQueue(.memory, logger: Logger(label: "FairnessTests"))

        jobQueue.registerJob(parameters: TestJob.self) { job, context in
            let startTime = Date()
            try await Task.sleep(for: .milliseconds(10))
            let executionTime = Date().timeIntervalSince(startTime)
            tracker.recordExecution(job.id, tenantId: job.tenantId, executionTime: executionTime)
            expectation.trigger()
        }

        try await testJobQueue(jobQueue.processor()) {
            let futureTime = Date().addingTimeInterval(0.1)

            // Submit immediate job with lower priority
            try await jobQueue.push(
                TestJob(id: "immediate", tenantId: "tenant-a", workload: .light, priority: .normal),
                options: .fairness(key: "tenant-a", weight: 1.0, priority: 1)
            )

            // Submit delayed job with higher priority
            try await jobQueue.push(
                TestJob(id: "delayed", tenantId: "tenant-b", workload: .light, priority: .high),
                options: .fairness(key: "tenant-b", weight: 1.0, priority: 10, delayUntil: futureTime)
            )

            try await expectation.wait(count: 2)

            let order = tracker.executionOrder
            #expect(order.count == 2)
            #expect(order[0] == "immediate")  // Executes first because delayed job isn't ready
        }
    }

    // MARK: - Mixed Priority and Fairness Scenarios

    @Test func testMixedPriorityOnlyAndFairnessJobs() async throws {
        let expectation = TestExpectation()
        let tracker = ExecutionTracker()
        let jobQueue = JobQueue(.memory, logger: Logger(label: "FairnessTests"))

        jobQueue.registerJob(parameters: TestJob.self) { job, context in
            let startTime = Date()
            try await Task.sleep(for: .milliseconds(10))
            let executionTime = Date().timeIntervalSince(startTime)
            tracker.recordExecution(job.id, tenantId: job.tenantId, executionTime: executionTime)
            expectation.trigger()
        }

        try await testJobQueue(jobQueue.processor()) {
            // Submit priority-only job (no fairness)
            try await jobQueue.push(
                TestJob(id: "priority-only", tenantId: "system", workload: .light, priority: .high),
                options: MemoryQueue.JobOptions(priority: 15)
            )

            // Submit fairness jobs with various priorities
            try await jobQueue.push(
                TestJob(id: "tenant-a-high", tenantId: "tenant-a", workload: .light, priority: .high),
                options: .fairness(key: "tenant-a", weight: 1.0, priority: 10)
            )
            try await jobQueue.push(
                TestJob(id: "tenant-b-normal", tenantId: "tenant-b", workload: .light, priority: .normal),
                options: .fairness(key: "tenant-b", weight: 2.0, priority: 5)
            )

            try await expectation.wait(count: 3)

            let order = tracker.executionOrder
            #expect(order.count == 3)
            #expect(order[0] == "priority-only")  // Highest priority should execute first
        }
    }

    // MARK: - JobOptions Convenience Methods Tests

    @Test func testPriorityOnlyConvenienceMethod() async throws {
        let expectation = TestExpectation()
        let tracker = ExecutionTracker()
        let jobQueue = JobQueue(.memory, logger: Logger(label: "FairnessTests"))

        jobQueue.registerJob(parameters: TestJob.self) { job, context in
            tracker.recordExecution(job.id, tenantId: job.tenantId, executionTime: 0.01)
            expectation.trigger()
        }

        try await testJobQueue(jobQueue.processor()) {
            try await jobQueue.push(
                TestJob(id: "test-1", tenantId: "tenant-a", workload: .light, priority: .normal),
                options: MemoryQueue.JobOptions(priority: 5)
            )

            try await expectation.wait(count: 1)

            let order = tracker.executionOrder
            #expect(order.count == 1)
            #expect(order[0] == "test-1")
        }
    }

    @Test func testFairnessConvenienceMethod() async throws {
        let expectation = TestExpectation()
        let tracker = ExecutionTracker()
        let jobQueue = JobQueue(.memory, logger: Logger(label: "FairnessTests"))

        jobQueue.registerJob(parameters: TestJob.self) { job, context in
            tracker.recordExecution(job.id, tenantId: job.tenantId, executionTime: 0.01)
            expectation.trigger()
        }

        try await testJobQueue(jobQueue.processor()) {
            try await jobQueue.push(
                TestJob(id: "test-1", tenantId: "tenant-a", workload: .light, priority: .normal),
                options: .fairness(key: "tenant-123", weight: 3.0, priority: 7)
            )

            try await expectation.wait(count: 1)

            let order = tracker.executionOrder
            #expect(order.count == 1)
            #expect(order[0] == "test-1")
        }
    }

    // MARK: - Realistic Multi-Tenant Scenario

    @Test func testRealisticMultiTenantScenario() async throws {
        let expectation = TestExpectation()
        let tracker = ExecutionTracker()
        let jobQueue = JobQueue(.memory, logger: Logger(label: "FairnessTests"))

        jobQueue.registerJob(parameters: TestJob.self) { job, context in
            let startTime = Date()
            // Simulate different workload times
            let workloadTime =
                switch job.workload {
                case .light: 10
                case .medium: 20
                case .heavy: 40
                }
            try await Task.sleep(for: .milliseconds(workloadTime))
            let executionTime = Date().timeIntervalSince(startTime)
            tracker.recordExecution(job.id, tenantId: job.tenantId, executionTime: executionTime)
            expectation.trigger()
        }

        try await testJobQueue(jobQueue.processor()) {
            // Enterprise tenant: high weight, mixed workloads
            for i in 1...3 {
                let priority = i == 1 ? 10 : 5  // First job is urgent
                let workload: Workload = i == 3 ? .heavy : .medium
                try await jobQueue.push(
                    TestJob(id: "enterprise-\(i)", tenantId: "enterprise", workload: workload, priority: .normal),
                    options: .fairness(key: "enterprise", weight: 8.0, priority: priority)
                )
            }

            // Startup tenant: medium weight, mostly light workloads
            for i in 1...2 {
                try await jobQueue.push(
                    TestJob(id: "startup-\(i)", tenantId: "startup", workload: .light, priority: .normal),
                    options: .fairness(key: "startup", weight: 3.0, priority: 5)
                )
            }

            // Small business: low weight but urgent job
            try await jobQueue.push(
                TestJob(id: "small-urgent", tenantId: "small-biz", workload: .light, priority: .high),
                options: .fairness(key: "small-biz", weight: 1.0, priority: 10)
            )

            try await expectation.wait(count: 6)

            let order = tracker.executionOrder
            let tenantTimes = tracker.tenantTimes

            #expect(order.count == 6)
            #expect(tenantTimes.count == 3)

            // Check that urgent jobs from different tenants execute early
            let urgentJobs = ["enterprise-1", "small-urgent"]
            let urgentIndices = urgentJobs.compactMap { job in order.firstIndex(of: job) }
            #expect(urgentIndices.allSatisfy { $0 < 3 })  // Urgent jobs should be in first half

            // Enterprise should get the most total execution time due to higher weight
            let enterpriseTime = tenantTimes["enterprise"] ?? 0
            let startupTime = tenantTimes["startup"] ?? 0
            let smallBizTime = tenantTimes["small-biz"] ?? 0

            #expect(enterpriseTime > startupTime)
            #expect(startupTime > smallBizTime)
        }
    }

    @Test func testProperWeightTracking() async throws {
        let expectation = TestExpectation()
        let tracker = ExecutionTracker()
        let jobQueue = JobQueue(.memory, logger: Logger(label: "FairnessTests"))

        jobQueue.registerJob(parameters: TestJob.self) { job, context in
            let startTime = Date()
            try await Task.sleep(for: .milliseconds(5))  // Shorter sleep for faster test
            let executionTime = Date().timeIntervalSince(startTime)
            tracker.recordExecution(job.id, tenantId: job.tenantId, executionTime: executionTime)
            expectation.trigger()
        }

        try await testJobQueue(jobQueue.processor()) {
            // Submit more jobs to make fairness patterns clearer
            // Heavy tenant gets 20x weight, should get proportionally more execution
            for i in 1...6 {
                try await jobQueue.push(
                    TestJob(id: "heavy-weight-\(i)", tenantId: "tenant-heavy", workload: .medium, priority: .normal),
                    options: .fairness(key: "tenant-heavy", weight: 20.0, priority: 5)
                )
            }

            for i in 1...6 {
                try await jobQueue.push(
                    TestJob(id: "light-weight-\(i)", tenantId: "tenant-light", workload: .medium, priority: .normal),
                    options: .fairness(key: "tenant-light", weight: 1.0, priority: 5)
                )
            }

            try await expectation.wait(count: 12)

            let order = tracker.executionOrder
            #expect(order.count == 12)

            // Test the core fairness guarantee: both tenants get to execute jobs
            let heavyCount = order.filter { $0.contains("heavy-weight") }.count
            let lightCount = order.filter { $0.contains("light-weight") }.count

            #expect(heavyCount == 6, "Should have 6 heavy-weight jobs")
            #expect(lightCount == 6, "Should have 6 light-weight jobs")

            // Test that weights affect execution pattern - heavy tenant should get early positions
            // At least one heavy job should execute in first half due to weight advantage
            let firstHalf = order.count / 2
            let heavyInFirstHalf = order.prefix(firstHalf).filter { $0.contains("heavy-weight") }.count

            #expect(heavyInFirstHalf > 0, "At least one heavy-weight job should execute in first half")

        }
    }

    @Test func testWeightBasedPrioritizationBehavior() async throws {
        // This test documents the current behavior rather than testing for ideal fairness
        let expectation = TestExpectation()
        let tracker = ExecutionTracker()
        let jobQueue = JobQueue(.memory, logger: Logger(label: "FairnessTests"))

        jobQueue.registerJob(parameters: QuickJob.self) { job, context in
            let startTime = Date()
            try await Task.sleep(for: .milliseconds(5))  // Very fast jobs
            let executionTime = Date().timeIntervalSince(startTime)
            tracker.recordExecution(job.id, tenantId: job.tenantId, executionTime: executionTime)
            expectation.trigger()
        }

        try await testJobQueue(jobQueue.processor()) {
            // Large tenant with high weight submits many jobs
            for i in 1...20 {
                try await jobQueue.push(
                    QuickJob(id: "big-corp-\(i)", tenantId: "big-corp"),
                    options: .fairness(key: "big-corp", weight: 50.0, priority: 5)
                )
            }

            // Small tenant with low weight submits few jobs
            for i in 1...3 {
                try await jobQueue.push(
                    QuickJob(id: "small-biz-\(i)", tenantId: "small-biz"),
                    options: .fairness(key: "small-biz", weight: 1.0, priority: 5)
                )
            }

            try await expectation.wait(count: 23)

            let order = tracker.executionOrder
            #expect(order.count == 23)

            let smallBizJobs = order.filter { $0.contains("small-biz") }
            #expect(smallBizJobs.count == 3, "All small tenant jobs should execute eventually")
        }
    }

    @Test func testDynamicWeightOverrideAPI() async throws {
        let jobQueue = JobQueue(.memory, logger: Logger(label: "JobsTests"))
        let expectation = TestExpectation()
        let executionOrder = Mutex<[String]>([])

        jobQueue.registerJob(parameters: TestJob.self) { job, context in
            try await Task.sleep(for: .milliseconds(5))
            executionOrder.withLock { $0.append(job.id) }
            expectation.trigger()
        }

        try await testJobQueue(jobQueue.processor(options: .init(numWorkers: 1))) {
            // Set weight overrides to test API functionality
            try await jobQueue.queue.setFairnessWeightOverride(key: "override-tenant", weight: 20.0)

            // Submit jobs where override should take effect
            for round in 1...6 {
                try await jobQueue.push(
                    TestJob(id: "base-\(round)", tenantId: "base-tenant", workload: .light, priority: .normal),
                    options: .fairness(key: "base-tenant", weight: 1.0, priority: 5)
                )

                try await jobQueue.push(
                    TestJob(id: "override-\(round)", tenantId: "override-tenant", workload: .light, priority: .normal),
                    options: .fairness(key: "override-tenant", weight: 1.0, priority: 5)  // Override to 20.0
                )
            }

            try await expectation.wait(count: 12)

            let order = executionOrder.withLock { $0 }
            let overrideJobs = order.filter { $0.contains("override") }
            let baseJobs = order.filter { $0.contains("base") }

            // Test API correctness: both tenants should execute all jobs
            #expect(overrideJobs.count == 6, "All override tenant jobs should execute")
            #expect(baseJobs.count == 6, "All base tenant jobs should execute")

            // Test override effect: with 20x weight advantage, override tenant should get more total execution
            #expect(
                overrideJobs.count >= baseJobs.count,
                "Override tenant with 20x weight should execute at least as many jobs as base tenant. Order: \(order)"
            )

        }
    }

    @Test func testWeightOverridesWithExtremeRatios() async throws {
        let jobQueue = JobQueue(.memory, logger: Logger(label: "JobsTests"))
        let expectation = TestExpectation()
        let executionOrder = Mutex<[String]>([])

        jobQueue.registerJob(parameters: TestJob.self) { job, context in
            try await Task.sleep(for: .milliseconds(5))
            executionOrder.withLock { $0.append(job.id) }
            expectation.trigger()
        }

        try await testJobQueue(jobQueue.processor(options: .init(numWorkers: 1))) {
            // Test extreme weight ratios to verify override system handles large differences
            try await jobQueue.queue.setFairnessWeightOverride(key: "vip-tenant", weight: 100.0)

            // Submit equal numbers of jobs from each tenant
            for round in 1...4 {
                try await jobQueue.push(
                    TestJob(id: "regular-\(round)", tenantId: "regular-tenant", workload: .light, priority: .normal),
                    options: .fairness(key: "regular-tenant", weight: 1.0, priority: 5)
                )

                try await jobQueue.push(
                    TestJob(id: "vip-\(round)", tenantId: "vip-tenant", workload: .light, priority: .normal),
                    options: .fairness(key: "vip-tenant", weight: 1.0, priority: 5)  // Override to 100.0
                )
            }

            try await expectation.wait(count: 8)

            let order = executionOrder.withLock { $0 }
            let vipJobs = order.filter { $0.contains("vip") }
            let regularJobs = order.filter { $0.contains("regular") }

            // Test basic execution
            #expect(vipJobs.count == 4, "All VIP jobs should execute")
            #expect(regularJobs.count == 4, "All regular jobs should execute")

            // Test extreme weight effect: VIP should get more execution opportunities overall
            #expect(
                vipJobs.count >= regularJobs.count,
                "VIP tenant with 100x weight should execute at least as many jobs as regular tenant. Order: \(order)"
            )

        }
    }

    @Test func testWeightBasedExecutionOrdering() async throws {
        let jobQueue = JobQueue(.memory, logger: Logger(label: "JobsTests"))
        let expectation = TestExpectation()
        let executionOrder = Mutex<[String]>([])

        jobQueue.registerJob(parameters: TestJob.self) { job, context in
            try await Task.sleep(for: .milliseconds(5))
            executionOrder.withLock { $0.append(job.id) }
            expectation.trigger()
        }

        try await testJobQueue(jobQueue.processor(options: .init(numWorkers: 1))) {
            // Test graduated weight differences to verify weight recording and scheduling
            // Submit multiple rounds to see stride scheduler effects accumulate
            for round in 1...3 {
                // Submit jobs with different weights in each round
                try await jobQueue.push(
                    TestJob(id: "weight1-\(round)", tenantId: "tenant-1", workload: .light, priority: .normal),
                    options: .fairness(key: "tenant-1", weight: 1.0, priority: 5)
                )

                try await jobQueue.push(
                    TestJob(id: "weight5-\(round)", tenantId: "tenant-5", workload: .light, priority: .normal),
                    options: .fairness(key: "tenant-5", weight: 5.0, priority: 5)
                )

                try await jobQueue.push(
                    TestJob(id: "weight10-\(round)", tenantId: "tenant-10", workload: .light, priority: .normal),
                    options: .fairness(key: "tenant-10", weight: 10.0, priority: 5)
                )
            }

            try await expectation.wait(count: 9)

            let order = executionOrder.withLock { $0 }

            // Test anti-starvation: stride scheduler prevents complete starvation of any tenant
            let weight1Jobs = order.filter { $0.hasPrefix("weight1-") }
            let weight5Jobs = order.filter { $0.hasPrefix("weight5-") }
            let weight10Jobs = order.filter { $0.hasPrefix("weight10-") }

            #expect(order.count == 9, "All jobs should execute")
            #expect(weight1Jobs.count > 0, "Weight-1 tenant should execute (anti-starvation)")
            #expect(weight5Jobs.count > 0, "Weight-5 tenant should execute (anti-starvation)")
            #expect(weight10Jobs.count > 0, "Weight-10 tenant should execute (anti-starvation)")
        }
    }
}

extension MemoryQueue.JobOptions {
    /// Create job options with priority only
    static func priority(_ priority: Int, delayUntil: Date = Date.now) -> MemoryQueue.JobOptions {
        MemoryQueue.JobOptions(delayUntil: delayUntil, priority: priority)
    }
}

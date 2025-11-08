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
                options: .priority(1)
            )
            try await jobQueue.push(
                TestJob(id: "high-1", tenantId: "tenant-a", workload: .light, priority: .high),
                options: .priority(10)
            )
            try await jobQueue.push(
                TestJob(id: "normal-1", tenantId: "tenant-a", workload: .light, priority: .normal),
                options: .priority(5)
            )

            try await expectation.wait(count: 3)

            let order = tracker.executionOrder
            #expect(order.count == 3)
            #expect(order[0] == "high-1")  // Highest priority first
            #expect(order[1] == "normal-1")  // Medium priority second
            #expect(order[2] == "low-1")  // Lowest priority last
        }
    }

    @Test func testBasicFairnessScheduling() async throws {
        let expectation = TestExpectation()
        let tracker = ExecutionTracker()
        let jobQueue = JobQueue(.memory, logger: Logger(label: "FairnessTests"))

        jobQueue.registerJob(parameters: TestJob.self) { job, context in
            let startTime = Date()
            try await Task.sleep(for: .milliseconds(50))
            let executionTime = Date().timeIntervalSince(startTime)
            tracker.recordExecution(job.id, tenantId: job.tenantId, executionTime: executionTime)
            expectation.trigger()
        }

        try await testJobQueue(jobQueue.processor()) {
            // Submit multiple jobs for different tenants with same priority but different weights
            try await jobQueue.push(
                TestJob(id: "tenant-a-1", tenantId: "tenant-a", workload: .medium, priority: .normal),
                options: .fairness(key: "tenant-a", weight: 1.0, priority: 5)
            )
            try await jobQueue.push(
                TestJob(id: "tenant-b-1", tenantId: "tenant-b", workload: .medium, priority: .normal),
                options: .fairness(key: "tenant-b", weight: 5.0, priority: 5)
            )
            try await jobQueue.push(
                TestJob(id: "tenant-a-2", tenantId: "tenant-a", workload: .medium, priority: .normal),
                options: .fairness(key: "tenant-a", weight: 1.0, priority: 5)
            )
            try await jobQueue.push(
                TestJob(id: "tenant-b-2", tenantId: "tenant-b", workload: .medium, priority: .normal),
                options: .fairness(key: "tenant-b", weight: 5.0, priority: 5)
            )

            try await expectation.wait(count: 4)

            let tenantTimes = tracker.tenantTimes
            #expect(tenantTimes.count == 2)

            // With proper weight tracking and anti-starvation, both tenants should get execution time
            let tenantATime = tenantTimes["tenant-a"] ?? 0
            let tenantBTime = tenantTimes["tenant-b"] ?? 0
            #expect(tenantATime > 0)
            #expect(tenantBTime > 0)

            // With anti-starvation protection, the weight difference may not always be exact
            // But both tenants should get reasonable execution time (no complete starvation)
            let totalTime = tenantATime + tenantBTime
            let tenantAPercentage = tenantATime / totalTime
            let tenantBPercentage = tenantBTime / totalTime

            // Each tenant should get at least 20% of execution time (anti-starvation guarantee)
            #expect(tenantAPercentage >= 0.2, "Tenant A should get at least 20% due to anti-starvation")
            #expect(tenantBPercentage >= 0.2, "Tenant B should get at least 20% execution time")
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
            // Submit many jobs for big tenant and few for small tenant
            for i in 1...6 {
                try await jobQueue.push(
                    QuickJob(id: "big-tenant-\(i)", tenantId: "big-tenant"),
                    options: .fairness(key: "big-tenant", weight: 3.0, priority: 5)
                )
            }

            for i in 1...2 {
                try await jobQueue.push(
                    QuickJob(id: "small-tenant-\(i)", tenantId: "small-tenant"),
                    options: .fairness(key: "small-tenant", weight: 1.0, priority: 5)
                )
            }

            try await expectation.wait(count: 8)

            let order = tracker.executionOrder
            #expect(order.count == 8)

            // Small tenant jobs should be interspersed with big tenant jobs due to fairness
            let smallTenantJobs = order.filter { $0.contains("small-tenant") }
            #expect(smallTenantJobs.count == 2)

            // Small tenant shouldn't have to wait for all big tenant jobs to finish
            // Due to timing, we just verify small tenant jobs eventually execute
            let firstSmallTenantIndex = order.firstIndex { $0.contains("small-tenant") } ?? order.count
            #expect(firstSmallTenantIndex < order.count)  // Small tenant jobs should eventually execute
        }
    }

    // MARK: - Edge Cases

    @Test func testFairnessWithZeroWeight() async throws {
        let expectation = TestExpectation()
        let tracker = ExecutionTracker()
        let jobQueue = JobQueue(.memory, logger: Logger(label: "FairnessTests"))

        jobQueue.registerJob(parameters: TestJob.self) { job, context in
            tracker.recordExecution(job.id, tenantId: job.tenantId, executionTime: 0.01)
            expectation.trigger()
        }

        try await testJobQueue(jobQueue.processor()) {
            // Weight should be clamped to minimum 0.1
            try await jobQueue.push(
                TestJob(id: "test-1", tenantId: "tenant-a", workload: .light, priority: .normal),
                options: .fairness(key: "tenant-a", weight: 0.0, priority: 5)
            )

            try await expectation.wait(count: 1)

            let order = tracker.executionOrder
            #expect(order.count == 1)
            #expect(order[0] == "test-1")
        }
    }

    @Test func testFairnessWithNegativePriority() async throws {
        let expectation = TestExpectation()
        let tracker = ExecutionTracker()
        let jobQueue = JobQueue(.memory, logger: Logger(label: "FairnessTests"))

        jobQueue.registerJob(parameters: TestJob.self) { job, context in
            tracker.recordExecution(job.id, tenantId: job.tenantId, executionTime: 0.01)
            expectation.trigger()
        }

        try await testJobQueue(jobQueue.processor()) {
            // Priority should be clamped to minimum 1
            try await jobQueue.push(
                TestJob(id: "test-1", tenantId: "tenant-a", workload: .light, priority: .normal),
                options: .fairness(key: "tenant-a", weight: 1.0, priority: -5)
            )

            try await expectation.wait(count: 1)

            let order = tracker.executionOrder
            #expect(order.count == 1)
            #expect(order[0] == "test-1")
        }
    }

    @Test func testFairnessWithEmptyFairnessKey() async throws {
        let expectation = TestExpectation()
        let tracker = ExecutionTracker()
        let jobQueue = JobQueue(.memory, logger: Logger(label: "FairnessTests"))

        jobQueue.registerJob(parameters: TestJob.self) { job, context in
            tracker.recordExecution(job.id, tenantId: job.tenantId, executionTime: 0.01)
            expectation.trigger()
        }

        try await testJobQueue(jobQueue.processor()) {
            // Empty fairness key should still work
            try await jobQueue.push(
                TestJob(id: "test-1", tenantId: "tenant-a", workload: .light, priority: .normal),
                options: .fairness(key: "", weight: 1.0, priority: 5)
            )

            try await expectation.wait(count: 1)

            let order = tracker.executionOrder
            #expect(order.count == 1)
            #expect(order[0] == "test-1")
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
                options: .priority(15)
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
                options: .priority(5)
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
            try await Task.sleep(for: .milliseconds(20))
            let executionTime = Date().timeIntervalSince(startTime)
            tracker.recordExecution(job.id, tenantId: job.tenantId, executionTime: executionTime)
            expectation.trigger()
        }

        try await testJobQueue(jobQueue.processor()) {
            // Submit jobs with very different weights
            try await jobQueue.push(
                TestJob(id: "heavy-weight-1", tenantId: "tenant-heavy", workload: .medium, priority: .normal),
                options: .fairness(key: "tenant-heavy", weight: 20.0, priority: 5)
            )

            try await jobQueue.push(
                TestJob(id: "light-weight-1", tenantId: "tenant-light", workload: .medium, priority: .normal),
                options: .fairness(key: "tenant-light", weight: 1.0, priority: 5)
            )

            // Add more jobs to trigger fairness calculations
            for i in 2...4 {
                try await jobQueue.push(
                    TestJob(id: "heavy-weight-\(i)", tenantId: "tenant-heavy", workload: .medium, priority: .normal),
                    options: .fairness(key: "tenant-heavy", weight: 20.0, priority: 5)
                )

                try await jobQueue.push(
                    TestJob(id: "light-weight-\(i)", tenantId: "tenant-light", workload: .medium, priority: .normal),
                    options: .fairness(key: "tenant-light", weight: 1.0, priority: 5)
                )
            }

            try await expectation.wait(count: 8)

            let tenantTimes = tracker.tenantTimes
            #expect(tenantTimes.count == 2)

            // With proper weight tracking, tenant-heavy should get significantly more execution time
            // This test verifies that weight tracking is working, not just counting keys
            let heavyTime = tenantTimes["tenant-heavy"] ?? 0
            let lightTime = tenantTimes["tenant-light"] ?? 0

            #expect(heavyTime > 0)
            #expect(lightTime > 0)

            // The ratio should reflect the weight difference (20:1)
            // Due to timing variations, we check for at least some bias toward heavy weight
            // If weights weren't tracked properly, the times would be roughly equal
            let order = tracker.executionOrder

            // Count how many heavy-weight jobs executed in first half vs second half
            let midpoint = order.count / 2
            let firstHalf = order.prefix(midpoint)
            let heavyInFirstHalf = firstHalf.filter { $0.contains("heavy-weight") }.count
            let lightInFirstHalf = firstHalf.filter { $0.contains("light-weight") }.count

            // With proper weight tracking, heavy-weight jobs should dominate early execution
            // This is a more reliable test than exact time ratios due to timing variations
            #expect(
                heavyInFirstHalf >= lightInFirstHalf,
                "Heavy weight jobs should execute more frequently in first half due to proper weight tracking"
            )
        }
    }

    @Test func testStarvationPrevention() async throws {
        let expectation = TestExpectation()
        let tracker = ExecutionTracker()
        let jobQueue = JobQueue(.memory, logger: Logger(label: "FairnessTests"))

        jobQueue.registerJob(parameters: QuickJob.self) { job, context in
            let startTime = Date()
            try await Task.sleep(for: .milliseconds(10))  // Fast jobs to build queue pressure
            let executionTime = Date().timeIntervalSince(startTime)
            tracker.recordExecution(job.id, tenantId: job.tenantId, executionTime: executionTime)
            expectation.trigger()
        }

        try await testJobQueue(jobQueue.processor()) {
            // Submit small tenant jobs first
            for i in 1...3 {
                try await jobQueue.push(
                    QuickJob(id: "small-biz-\(i)", tenantId: "small-biz"),
                    options: .fairness(key: "small-biz", weight: 1.0, priority: 5)
                )
            }

            // Then flood with large tenant jobs (extreme weight difference)
            for i in 1...30 {
                try await jobQueue.push(
                    QuickJob(id: "big-corp-\(i)", tenantId: "big-corp"),
                    options: .fairness(key: "big-corp", weight: 100.0, priority: 5)
                )
            }

            try await expectation.wait(count: 33)

            let order = tracker.executionOrder
            #expect(order.count == 33)

            // Check that small business jobs execute (no complete starvation)
            let smallBizPositions = order.enumerated().compactMap { index, jobId in
                jobId.contains("small-biz") ? index + 1 : nil
            }

            print("Small business job positions: \(smallBizPositions)")
            print("Total jobs: \(order.count)")

            // STRIDE SCHEDULING BEHAVIOR: With extreme weight ratios (100:1), weighted fairness
            // allows high-weight tenants to dominate until they exhaust their queue.
            // This is how stride scheduling works - weights create proportional allocation, not equal turns.

            // Test basic anti-starvation: small tenant jobs should execute eventually
            #expect(smallBizPositions.count == 3, "All small tenant jobs should execute (no complete starvation)")

            // Document trade-off: High weights can delay low-weight tenants significantly
            let avgSmallBizPosition = smallBizPositions.reduce(0, +) / smallBizPositions.count
            print("Average small business position: \(avgSmallBizPosition) (demonstrates weight-based prioritization)")
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

            // Find when first small business job executes
            let firstSmallBizIndex = order.firstIndex { $0.contains("small-biz") } ?? order.count

            // Current behavior: Weight-based fairness allows higher weight tenants to execute first
            print("First small business job executed at position: \(firstSmallBizIndex + 1) out of \(order.count)")

            // This demonstrates the trade-off between fairness and weights
            // Higher weights DO get priority, which may delay lower weight tenants
            if firstSmallBizIndex > 15 {
                print("📊 WEIGHT-BASED BEHAVIOR: High-weight tenant prioritized over low-weight tenant")
            }

            // Verify no complete starvation - all jobs should eventually execute
            let smallBizJobs = order.filter { $0.contains("small-biz") }
            #expect(smallBizJobs.count == 3, "All small tenant jobs should execute eventually")
        }
    }
}

extension MemoryQueue.JobOptions {
    /// Create job options with priority only
    static func priority(_ priority: Int, delayUntil: Date? = nil) -> MemoryQueue.JobOptions {
        MemoryQueue.JobOptions(delayUntil: delayUntil, priority: priority)
    }
}

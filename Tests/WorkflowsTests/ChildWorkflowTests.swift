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

@Suite("ChildWorkflowTests")
struct ChildWorkflowTests {

    // MARK: - Helper Methods

    private func setupWorkflowSystem() async throws -> (WorkflowEngine<MemoryWorkflowQueue>, any Service) {
        let logger = Logger(label: "ChildWorkflowTests")

        let activities = TestActivityContainer()
        let workflowEngine = WorkflowEngine(
            queue: .memoryWorkflow,
            logger: logger,
            activities: [activities]
        )

        let processor = workflowEngine.processor(options: .init(numWorkers: 20))

        // Register test workflows
        workflowEngine.registerWorkflow(ParentWorkflow.self)
        workflowEngine.registerWorkflow(SimpleChildWorkflow.self)
        workflowEngine.registerWorkflow(ComplexChildWorkflow.self)
        workflowEngine.registerWorkflow(FailingChildWorkflow.self)
        workflowEngine.registerWorkflow(CountryProcessingWorkflow.self)
        workflowEngine.registerWorkflow(QualityCheckWorkflow.self)

        return (workflowEngine, processor)
    }

    // MARK: - Test Workflows

    /// Simple parent workflow that executes one child workflow
    struct ParentWorkflow: WorkflowProtocol {

        struct Input: Codable, Sendable {
            let message: String
            let childData: String
        }

        struct Output: Codable, Sendable {
            let parentResult: String
            let childResult: String
            let executionTime: TimeInterval
        }

        func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
            let startTime = Date()

            // Execute a simple child workflow
            let childResult = try await context.executeChildWorkflow(
                SimpleChildWorkflow.self,
                input: SimpleChildWorkflow.Input(data: input.childData),
                options: ChildWorkflowOptions(
                    runTimeout: .seconds(30),
                    childWorkflowIdPrefix: "simple"
                )
            )

            let executionTime = Date().timeIntervalSince(startTime)

            return Output(
                parentResult: "Parent processed: \(input.message)",
                childResult: childResult.output.result,
                executionTime: executionTime
            )
        }
    }

    /// Simple child workflow for testing
    struct SimpleChildWorkflow: WorkflowProtocol {

        struct Input: Codable, Sendable {
            let data: String
        }

        struct Output: Codable, Sendable {
            let result: String
            let processedAt: Date
        }

        func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
            // Execute a simple activity
            let processedData: String = try await context.executeActivity(
                SimpleProcessActivity.self,
                input: SimpleProcessActivity.Input(text: input.data),
                options: ActivityOptions(startToCloseTimeout: .seconds(10))
            )

            return Output(
                result: processedData,
                processedAt: Date()
            )
        }
    }

    /// Complex child workflow that executes multiple activities
    struct ComplexChildWorkflow: WorkflowProtocol {

        struct Input: Codable, Sendable {
            let items: [String]
            let multiplier: Int
        }

        struct Output: Codable, Sendable {
            let processedItems: [String]
            let totalCount: Int
        }

        func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
            // Process each item in parallel
            let processedItems = try await withThrowingTaskGroup(of: String.self) { group in
                for item in input.items {
                    group.addTask {
                        try await context.executeActivity(
                            SimpleProcessActivity.self,
                            input: SimpleProcessActivity.Input(text: "\(item)_x\(input.multiplier)"),
                            options: ActivityOptions(startToCloseTimeout: .seconds(5))
                        )
                    }
                }

                var results: [String] = []
                for try await result in group {
                    results.append(result)
                }
                return results
            }

            return Output(
                processedItems: processedItems,
                totalCount: processedItems.count
            )
        }
    }

    /// Child workflow that fails for testing error handling
    struct FailingChildWorkflow: WorkflowProtocol {

        struct Input: Codable, Sendable {
            let shouldFail: Bool
            let failureMessage: String
        }

        struct Output: Codable, Sendable {
            let result: String
        }

        func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
            if input.shouldFail {
                throw ChildWorkflowTestError.intentionalFailure(input.failureMessage)
            }

            return Output(result: "Success: not failed")
        }
    }

    /// Country processing workflow from the example
    struct CountryProcessingWorkflow: WorkflowProtocol {

        struct Input: Codable, Sendable {
            let country: String
            let variants: Int
        }

        struct Output: Codable, Sendable {
            let country: String
            let processedVariants: Int
            let success: Bool
        }

        func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
            // Simulate processing variants in parallel
            let results = try await withThrowingTaskGroup(of: String.self) { group in
                for variant in 0..<input.variants {
                    group.addTask {
                        try await context.executeActivity(
                            SimpleProcessActivity.self,
                            input: SimpleProcessActivity.Input(text: "\(input.country)_variant_\(variant)"),
                            options: ActivityOptions(startToCloseTimeout: .seconds(5))
                        )
                    }
                }

                var variantResults: [String] = []
                for try await result in group {
                    variantResults.append(result)
                }
                return variantResults
            }

            return Output(
                country: input.country,
                processedVariants: results.count,
                success: results.count == input.variants
            )
        }
    }

    /// Quality check workflow from the example
    struct QualityCheckWorkflow: WorkflowProtocol {

        struct Input: Codable, Sendable {
            let countries: [String]
            let threshold: Double
        }

        struct Output: Codable, Sendable {
            let passedCountries: [String]
            let failedCountries: [String]
            let averageScore: Double
        }

        func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
            // Simulate quality checks
            let scores = try await withThrowingTaskGroup(of: (String, Double).self) { group in
                for country in input.countries {
                    group.addTask {
                        let score = Double.random(in: 0.6...1.0)
                        return (country, score)
                    }
                }

                var countryScores: [String: Double] = [:]
                for try await (country, score) in group {
                    countryScores[country] = score
                }
                return countryScores
            }

            let passed = scores.filter { $0.value >= input.threshold }.map { $0.key }
            let failed = scores.filter { $0.value < input.threshold }.map { $0.key }
            let average = scores.values.reduce(0, +) / Double(scores.count)

            return Output(
                passedCountries: passed,
                failedCountries: failed,
                averageScore: average
            )
        }
    }

    // MARK: - Test Activities

    struct SimpleProcessActivity: ActivityParameters {

        struct Input: Codable, Sendable {
            let text: String
        }

        typealias Output = String
    }

    struct TestActivityContainer: ActivityContainer {
        func registerActivities(with registry: ActivityRegistry) {
            registry.registerActivity(SimpleProcessActivity.self) { input, context in
                // Simulate processing time
                try await Task.sleep(for: .milliseconds(50))
                return "Processed: \(input.text)"
            }

            // Register DAG workflow activities
            registry.registerActivity(TextIngestionActivity.self) { input, context in
                // Simulate text ingestion processing
                try await Task.sleep(for: .milliseconds(100))
                return TextIngestionActivity.Output(
                    success: true,
                    corpusId: "corpus_\(input.country)_\(input.partition)"
                )
            }

            registry.registerActivity(TextAnalysisActivity.self) { input, context in
                // Simulate text analysis processing
                try await Task.sleep(for: .milliseconds(150))
                return TextAnalysisActivity.Output(
                    success: true,
                    analysisId: "analysis_\(input.country)_\(input.variant)"
                )
            }

            registry.registerActivity(ModelGenerationActivity.self) { input, context in
                // Simulate model generation processing
                try await Task.sleep(for: .milliseconds(200))
                return ModelGenerationActivity.Output(
                    success: true,
                    modelUrl: "https://models.example.com/\(input.country)/voice_model.bin"
                )
            }

            registry.registerActivity(ModelPublishingActivity.self) { input, context in
                // Simulate model publishing processing
                try await Task.sleep(for: .milliseconds(100))
                return ModelPublishingActivity.Output(
                    success: true,
                    publishedCount: input.modelUrls.count
                )
            }

            // Register billing workflow activities
            registry.registerActivity(InvoiceCalculationActivity.self) { input, context in
                // Simulate invoice calculation processing
                try await Task.sleep(for: .milliseconds(200))
                return InvoiceCalculationActivity.Output(
                    success: true,
                    amount: Double.random(in: 50...500),
                    invoiceId: "inv_\(input.userId)_\(Int.random(in: 1000...9999))"
                )
            }

            registry.registerActivity(CreditCardChargeActivity.self) { input, context in
                // Simulate credit card charging
                try await Task.sleep(for: .milliseconds(300))
                return CreditCardChargeActivity.Output(
                    success: true,
                    chargeId: "ch_\(input.userId)_\(Int.random(in: 10000...99999))"
                )
            }

            registry.registerActivity(PDFGenerationActivity.self) { input, context in
                // Simulate PDF generation
                try await Task.sleep(for: .milliseconds(250))
                return PDFGenerationActivity.Output(
                    success: true,
                    pdfUrl: "https://receipts.example.com/\(input.chargeId).pdf"
                )
            }

            registry.registerActivity(EmailSendingActivity.self) { input, context in
                // Simulate email sending
                try await Task.sleep(for: .milliseconds(100))
                return EmailSendingActivity.Output(
                    success: true,
                    emailId: "email_\(input.recipientType)_\(Int.random(in: 100000...999999))"
                )
            }
        }
    }

    // MARK: - Error Types

    enum ChildWorkflowTestError: Error, LocalizedError {
        case intentionalFailure(String)
        case unexpectedState(String)

        var errorDescription: String? {
            switch self {
            case .intentionalFailure(let message):
                return "Intentional test failure: \(message)"
            case .unexpectedState(let message):
                return "Unexpected state: \(message)"
            }
        }
    }

    // MARK: - Test Cases

    @Test("Single child workflow execution")
    func testSingleChildWorkflowExecution() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        try await testWorkflow(processor) {
            let workflowId = try await workflowEngine.startWorkflow(
                ParentWorkflow.self,
                input: ParentWorkflow.Input(
                    message: "Test parent workflow",
                    childData: "child test data"
                )
            )

            #expect(workflowId.value.count > 0)

            let finalStatus = try await waitForWorkflowCompletion(
                workflowId,
                engine: workflowEngine,
                inputType: ParentWorkflow.Input.self,
                outputType: ParentWorkflow.Output.self,
                timeout: .seconds(10),
                description: "parent workflow with child"
            )

            #expect(finalStatus.status == .completed)

            if let result = finalStatus.output {
                #expect(result.parentResult.contains("Test parent workflow"))
                #expect(result.childResult.contains("Processed: child test data"))
                #expect(result.executionTime > 0)
            }
        }
    }

    @Test("Multiple parallel child workflows")
    func testParallelChildWorkflowExecution() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        struct ParallelParentWorkflow: WorkflowProtocol {

            struct Input: Codable, Sendable {
                let countries: [String]
                let variants: Int
            }

            struct Output: Codable, Sendable {
                let results: [CountryProcessingWorkflow.Output]
                let totalProcessed: Int
            }

            func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
                let results = try await withThrowingTaskGroup(of: CountryProcessingWorkflow.Output.self) { group in
                    for (index, country) in input.countries.enumerated() {
                        group.addTask {
                            let result = try await context.executeChildWorkflow(
                                CountryProcessingWorkflow.self,
                                input: CountryProcessingWorkflow.Input(country: country, variants: input.variants),
                                options: ChildWorkflowOptions(
                                    runTimeout: .seconds(30),
                                    childWorkflowIdPrefix: "country-\(index)"
                                )
                            )
                            return result.output
                        }
                    }

                    var outputs: [CountryProcessingWorkflow.Output] = []
                    for try await output in group {
                        outputs.append(output)
                    }
                    return outputs
                }

                let totalProcessed = results.map { $0.processedVariants }.reduce(0, +)

                return Output(
                    results: results,
                    totalProcessed: totalProcessed
                )
            }
        }

        workflowEngine.registerWorkflow(ParallelParentWorkflow.self)

        try await testWorkflow(processor) {
            let workflowId = try await workflowEngine.startWorkflow(
                ParallelParentWorkflow.self,
                input: ParallelParentWorkflow.Input(
                    countries: ["US", "UK", "FR"],
                    variants: 2
                )
            )

            let finalStatus = try await waitForWorkflowCompletion(
                workflowId,
                engine: workflowEngine,
                inputType: ParallelParentWorkflow.Input.self,
                outputType: ParallelParentWorkflow.Output.self,
                timeout: .seconds(15),
                description: "parallel child workflows"
            )

            #expect(finalStatus.status == .completed)

            if let result = finalStatus.output {
                #expect(result.results.count == 3)
                #expect(result.totalProcessed == 6)  // 3 countries * 2 variants each
                #expect(result.results.allSatisfy { $0.success })
            }
        }
    }

    @Test("Child workflow failure handling")
    func testChildWorkflowFailureHandling() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        struct FailureTestParentWorkflow: WorkflowProtocol {

            struct Input: Codable, Sendable {
                let shouldChildFail: Bool
            }

            struct Output: Codable, Sendable {
                let result: String
            }

            func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
                do {
                    let childResult = try await context.executeChildWorkflow(
                        FailingChildWorkflow.self,
                        input: FailingChildWorkflow.Input(
                            shouldFail: input.shouldChildFail,
                            failureMessage: "Test failure"
                        ),
                        options: ChildWorkflowOptions(runTimeout: .seconds(10))
                    )

                    return Output(result: childResult.output.result)
                } catch {
                    return Output(result: "Child failed: \(error.localizedDescription)")
                }
            }
        }

        workflowEngine.registerWorkflow(FailureTestParentWorkflow.self)

        try await testWorkflow(processor) {
            // Test successful child workflow
            let successId = try await workflowEngine.startWorkflow(
                FailureTestParentWorkflow.self,
                input: FailureTestParentWorkflow.Input(shouldChildFail: false)
            )

            let successStatus = try await waitForWorkflowCompletion(
                successId,
                engine: workflowEngine,
                inputType: FailureTestParentWorkflow.Input.self,
                outputType: FailureTestParentWorkflow.Output.self,
                description: "successful child workflow"
            )

            #expect(successStatus.status == .completed)

            if let result = successStatus.output {
                #expect(result.result == "Success: not failed")
            }

            // Test failing child workflow
            let failId = try await workflowEngine.startWorkflow(
                FailureTestParentWorkflow.self,
                input: FailureTestParentWorkflow.Input(shouldChildFail: true)
            )

            let failStatus = try await waitForWorkflowCompletion(
                failId,
                engine: workflowEngine,
                inputType: FailureTestParentWorkflow.Input.self,
                outputType: FailureTestParentWorkflow.Output.self,
                expectedStatus: .completed,
                description: "failed child workflow"
            )

            #expect(failStatus.status == .completed)  // Parent should handle the failure

            if let result = failStatus.output {
                #expect(result.result.contains("Child failed"))
                #expect(result.result.contains("Child failed"))
            }
        }
    }

    @Test("Child workflow replay safety")
    func testChildWorkflowReplaySafety() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        struct ReplayTestWorkflow: WorkflowProtocol {

            struct Input: Codable, Sendable {
                let data: String
            }

            struct Output: Codable, Sendable {
                let childResult: String
                let executionCount: Int
            }

            func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
                // Execute the same child workflow twice with same input
                // Should get cached result on second call
                let result1 = try await context.executeChildWorkflow(
                    SimpleChildWorkflow.self,
                    input: SimpleChildWorkflow.Input(data: input.data),
                    options: ChildWorkflowOptions(childWorkflowIdPrefix: "replay_test")
                )

                let result2 = try await context.executeChildWorkflow(
                    SimpleChildWorkflow.self,
                    input: SimpleChildWorkflow.Input(data: input.data),
                    options: ChildWorkflowOptions(childWorkflowIdPrefix: "replay_test")
                )

                // Results should be identical (cached)
                return Output(
                    childResult: result1.output.result,
                    executionCount: result1.childWorkflowId == result2.childWorkflowId ? 1 : 2
                )
            }
        }

        workflowEngine.registerWorkflow(ReplayTestWorkflow.self)

        try await testWorkflow(processor) {
            let workflowId = try await workflowEngine.startWorkflow(
                ReplayTestWorkflow.self,
                input: ReplayTestWorkflow.Input(data: "replay test data")
            )

            let finalStatus = try await waitForWorkflowCompletion(
                workflowId,
                engine: workflowEngine,
                inputType: ReplayTestWorkflow.Input.self,
                outputType: ReplayTestWorkflow.Output.self,
                description: "replay safety test"
            )

            #expect(finalStatus.status == .completed)

            if let result = finalStatus.output {
                #expect(result.childResult.contains("Processed: replay test data"))
                #expect(result.executionCount == 1)  // Should be cached, so only executed once
            }
        }
    }

    @Test("Hierarchical workflow composition")
    func testHierarchicalWorkflowComposition() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        struct GrandparentWorkflow: WorkflowProtocol {

            struct Input: Codable, Sendable {
                let countries: [String]
                let enableQualityCheck: Bool
            }

            struct Output: Codable, Sendable {
                let processedCountries: [String]
                let qualityResults: QualityCheckWorkflow.Output?
                let totalSteps: Int
            }

            func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
                // Step 1: Process countries in parallel (parent -> child)
                let countryResults = try await withThrowingTaskGroup(of: CountryProcessingWorkflow.Output.self) { group in
                    for (index, country) in input.countries.enumerated() {
                        group.addTask {
                            let result = try await context.executeChildWorkflow(
                                CountryProcessingWorkflow.self,
                                input: CountryProcessingWorkflow.Input(country: country, variants: 2),
                                options: ChildWorkflowOptions(childWorkflowIdPrefix: "country-\(index)")
                            )
                            return result.output
                        }
                    }

                    var outputs: [CountryProcessingWorkflow.Output] = []
                    for try await output in group {
                        outputs.append(output)
                    }
                    return outputs
                }

                let processedCountries = countryResults.map { $0.country }

                // Step 2: Optional quality check (parent -> child)
                var qualityResults: QualityCheckWorkflow.Output?
                if input.enableQualityCheck {
                    let qualityResult = try await context.executeChildWorkflow(
                        QualityCheckWorkflow.self,
                        input: QualityCheckWorkflow.Input(
                            countries: processedCountries,
                            threshold: 0.8
                        ),
                        options: ChildWorkflowOptions(childWorkflowIdPrefix: "quality")
                    )
                    qualityResults = qualityResult.output
                }

                return Output(
                    processedCountries: processedCountries,
                    qualityResults: qualityResults,
                    totalSteps: qualityResults != nil ? 2 : 1
                )
            }
        }

        workflowEngine.registerWorkflow(GrandparentWorkflow.self)

        try await testWorkflow(processor) {
            let workflowId = try await workflowEngine.startWorkflow(
                GrandparentWorkflow.self,
                input: GrandparentWorkflow.Input(
                    countries: ["BV", "LU"],
                    enableQualityCheck: true
                )
            )

            let finalStatus = try await waitForWorkflowCompletion(
                workflowId,
                engine: workflowEngine,
                inputType: GrandparentWorkflow.Input.self,
                outputType: GrandparentWorkflow.Output.self,
                timeout: .seconds(15),
                description: "hierarchical workflow composition"
            )

            #expect(finalStatus.status == .completed)

            if let result = finalStatus.output {
                #expect(result.processedCountries.count == 2)
                #expect(result.processedCountries.contains("BV"))
                #expect(result.processedCountries.contains("LU"))
                #expect(result.qualityResults != nil)
                #expect(result.totalSteps == 2)

                if let qualityResults = result.qualityResults {
                    #expect(qualityResults.passedCountries.count + qualityResults.failedCountries.count == 2)
                    #expect(qualityResults.averageScore >= 0.0 && qualityResults.averageScore <= 1.0)
                }
            }
        }
    }

    @Test("Child workflow cancellation")
    func testChildWorkflowCancellation() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        struct CancellationTestWorkflow: WorkflowProtocol {

            struct Input: Codable, Sendable {
                let shouldCancel: Bool
            }

            struct Output: Codable, Sendable {
                let result: String
            }

            func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
                if input.shouldCancel {
                    // Start a child workflow and then try to cancel it
                    async let childTask: ChildWorkflowResult<SimpleChildWorkflow.Output> = context.executeChildWorkflow(
                        SimpleChildWorkflow.self,
                        input: SimpleChildWorkflow.Input(data: "to be cancelled"),
                        options: ChildWorkflowOptions(childWorkflowIdPrefix: "cancel_test")
                    )

                    // Simulate some delay then cancel
                    try await Task.sleep(for: .milliseconds(10))

                    do {
                        let result = try await childTask
                        return Output(result: "Child completed: \(result.output.result)")
                    } catch {
                        return Output(result: "Child was cancelled or failed: \(error.localizedDescription)")
                    }
                } else {
                    let childResult = try await context.executeChildWorkflow(
                        SimpleChildWorkflow.self,
                        input: SimpleChildWorkflow.Input(data: "normal execution"),
                        options: ChildWorkflowOptions(childWorkflowIdPrefix: "normal")
                    )

                    return Output(result: "Normal completion: \(childResult.output.result)")
                }
            }
        }

        workflowEngine.registerWorkflow(CancellationTestWorkflow.self)

        try await testWorkflow(processor) {
            // Test normal execution
            let normalId = try await workflowEngine.startWorkflow(
                CancellationTestWorkflow.self,
                input: CancellationTestWorkflow.Input(shouldCancel: false)
            )

            let normalStatus = try await waitForWorkflowCompletion(
                normalId,
                engine: workflowEngine,
                inputType: CancellationTestWorkflow.Input.self,
                outputType: CancellationTestWorkflow.Output.self,
                description: "normal child workflow execution"
            )

            #expect(normalStatus.status == .completed)

            if let result = normalStatus.output {
                #expect(result.result.contains("Normal completion"))
                #expect(result.result.contains("Processed: normal execution"))
            }
        }
    }

    @Test("Child workflow timeout handling")
    func testChildWorkflowTimeout() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        struct TimeoutTestWorkflow: WorkflowProtocol {

            struct Input: Codable, Sendable {
                let useShortTimeout: Bool
            }

            struct Output: Codable, Sendable {
                let result: String
                let timedOut: Bool
            }

            func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
                let timeout: Duration = input.useShortTimeout ? .milliseconds(1) : .seconds(10)

                do {
                    let childResult = try await context.executeChildWorkflow(
                        SimpleChildWorkflow.self,
                        input: SimpleChildWorkflow.Input(data: "timeout test"),
                        options: ChildWorkflowOptions(
                            runTimeout: timeout,
                            childWorkflowIdPrefix: "timeout_test"
                        )
                    )

                    return Output(
                        result: childResult.output.result,
                        timedOut: false
                    )
                } catch {
                    return Output(
                        result: "Error: \(error.localizedDescription)",
                        timedOut: true
                    )
                }
            }
        }
        workflowEngine.registerWorkflow(TimeoutTestWorkflow.self)

        try await testWorkflow(processor) {
            // Test successful execution with reasonable timeout
            let successId = try await workflowEngine.startWorkflow(
                TimeoutTestWorkflow.self,
                input: TimeoutTestWorkflow.Input(useShortTimeout: false)
            )

            let successStatus = try await waitForWorkflowCompletion(
                successId,
                engine: workflowEngine,
                inputType: TimeoutTestWorkflow.Input.self,
                outputType: TimeoutTestWorkflow.Output.self,
                description: "child workflow with reasonable timeout"
            )

            #expect(successStatus.status == .completed)

            if let result = successStatus.output {
                #expect(result.timedOut == false)
                #expect(result.result.contains("Processed: timeout test"))
            }
        }
    }

    @Test("Complex DAG workflow")
    func testComplexDAGWorkflowPattern() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        // Register the DAG workflow types
        workflowEngine.registerWorkflow(VoiceTrainingDAGWorkflow.self)
        workflowEngine.registerWorkflow(CountryDAGProcessingWorkflow.self)
        workflowEngine.registerWorkflow(TextIngestionWorkflow.self)
        workflowEngine.registerWorkflow(TextAnalysisWorkflow.self)
        workflowEngine.registerWorkflow(ModelGenerationWorkflow.self)
        workflowEngine.registerWorkflow(ModelPublishingWorkflow.self)

        try await testWorkflow(processor) {
            let workflowId = try await workflowEngine.startWorkflow(
                VoiceTrainingDAGWorkflow.self,
                input: VoiceTrainingDAGWorkflow.Input(
                    countries: ["BV", "LU", "SM"],
                    variants: 3
                )
            )

            #expect(workflowId.value.count > 0)

            let finalStatus = try await waitForWorkflowCompletion(
                workflowId,
                engine: workflowEngine,
                inputType: VoiceTrainingDAGWorkflow.Input.self,
                outputType: VoiceTrainingDAGWorkflow.Output.self,
                timeout: .seconds(20),
                description: "complex DAG workflow pattern"
            )

            #expect(finalStatus.status == .completed)

            if let result = finalStatus.output {
                #expect(result.countriesProcessed == 3)
                #expect(result.totalModelsGenerated == 3)
                #expect(result.publishedSuccessfully == true)
                #expect(result.totalExecutionTime > 0)
            }
        }
    }

    // MARK: - DAG Workflow Types

    /// Top-level orchestrator workflow modeling a TrainVoiceAssistant workflow
    struct VoiceTrainingDAGWorkflow: WorkflowProtocol {

        struct Input: Codable, Sendable {
            let countries: [String]
            let variants: Int
        }

        struct Output: Codable, Sendable {
            let countriesProcessed: Int
            let totalModelsGenerated: Int
            let publishedSuccessfully: Bool
            let totalExecutionTime: TimeInterval
            let countryResults: [CountryDAGProcessingWorkflow.Output]
        }

        func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
            let startTime = Date()

            // Step 1: Process each country in parallel using native Swift concurrency
            // This models the parallel country processing
            let countryResults = try await withThrowingTaskGroup(of: CountryDAGProcessingWorkflow.Output.self) { group in
                for (index, country) in input.countries.enumerated() {
                    group.addTask {
                        let result = try await context.executeChildWorkflow(
                            CountryDAGProcessingWorkflow.self,
                            input: CountryDAGProcessingWorkflow.Input(
                                country: country,
                                variants: input.variants,
                                partition: index
                            ),
                            options: ChildWorkflowOptions(childWorkflowIdPrefix: "country-\(index)")
                        )
                        return result.output
                    }
                }

                var outputs: [CountryDAGProcessingWorkflow.Output] = []
                for try await output in group {
                    outputs.append(output)
                }
                return outputs
            }

            // Step 2: Final model publishing (equivalent to PublishModels task)
            // This depends on all country processing being complete
            let publishResult = try await context.executeChildWorkflow(
                ModelPublishingWorkflow.self,
                input: ModelPublishingWorkflow.Input(
                    modelUrls: countryResults.compactMap { $0.modelUrl },
                    countries: input.countries
                ),
                options: ChildWorkflowOptions(
                    runTimeout: .minutes(5),
                    childWorkflowIdPrefix: "publish"
                )
            )

            let executionTime = Date().timeIntervalSince(startTime)

            return Output(
                countriesProcessed: countryResults.count,
                totalModelsGenerated: countryResults.filter { $0.success }.count,
                publishedSuccessfully: publishResult.output.success,
                totalExecutionTime: executionTime,
                countryResults: countryResults
            )
        }
    }

    /// Country-level processing workflow that models the sequential stages per country
    struct CountryDAGProcessingWorkflow: WorkflowProtocol {

        struct Input: Codable, Sendable {
            let country: String
            let variants: Int
            let partition: Int
        }

        struct Output: Codable, Sendable {
            let country: String
            let success: Bool
            let modelUrl: String?
            let variantsProcessed: Int
            let processingTime: TimeInterval
        }

        func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
            let startTime = Date()

            // Stage 1: Text Ingestion (IngestTextCorpus task)
            let ingestionResult = try await context.executeChildWorkflow(
                TextIngestionWorkflow.self,
                input: TextIngestionWorkflow.Input(
                    country: input.country,
                    partition: input.partition
                ),
                options: ChildWorkflowOptions(
                    runTimeout: .minutes(5),
                    childWorkflowIdPrefix: "ingest"
                )
            )

            guard ingestionResult.output.success else {
                return Output(
                    country: input.country,
                    success: false,
                    modelUrl: nil,
                    variantsProcessed: 0,
                    processingTime: Date().timeIntervalSince(startTime)
                )
            }

            // Stage 2: Text Analysis in parallel for each variant (AnalyzeTextCorpus tasks)
            let analysisResults = try await withThrowingTaskGroup(of: TextAnalysisWorkflow.Output.self) { group in
                for variant in 0..<input.variants {
                    group.addTask {
                        let result = try await context.executeChildWorkflow(
                            TextAnalysisWorkflow.self,
                            input: TextAnalysisWorkflow.Input(
                                country: input.country,
                                variant: variant,
                                partition: input.partition,
                                corpusId: ingestionResult.output.corpusId
                            ),
                            options: ChildWorkflowOptions(
                                runTimeout: .minutes(8),
                                childWorkflowIdPrefix: "analyze-\(variant)"
                            )
                        )
                        return result.output
                    }
                }

                var outputs: [TextAnalysisWorkflow.Output] = []
                for try await output in group {
                    outputs.append(output)
                }
                return outputs
            }

            let successfulAnalyses = analysisResults.filter { $0.success }
            guard !successfulAnalyses.isEmpty else {
                return Output(
                    country: input.country,
                    success: false,
                    modelUrl: nil,
                    variantsProcessed: 0,
                    processingTime: Date().timeIntervalSince(startTime)
                )
            }

            // Stage 3: Model Generation (GenerateVoiceModel task)
            // Depends on all analysis tasks completing
            let modelResult = try await context.executeChildWorkflow(
                ModelGenerationWorkflow.self,
                input: ModelGenerationWorkflow.Input(
                    country: input.country,
                    analysisIds: successfulAnalyses.map { $0.analysisId }
                ),
                options: ChildWorkflowOptions(
                    runTimeout: .minutes(10),
                    childWorkflowIdPrefix: "generate"
                )
            )

            return Output(
                country: input.country,
                success: modelResult.output.success,
                modelUrl: modelResult.output.modelUrl,
                variantsProcessed: successfulAnalyses.count,
                processingTime: Date().timeIntervalSince(startTime)
            )
        }
    }

    /// Text ingestion workflow (models IngestTextCorpus task)
    struct TextIngestionWorkflow: WorkflowProtocol {

        struct Input: Codable, Sendable {
            let country: String
            let partition: Int
        }

        struct Output: Codable, Sendable {
            let success: Bool
            let corpusId: String
            let error: String?
        }

        func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
            // Simulate text ingestion activity
            let result = try await context.executeActivity(
                TextIngestionActivity.self,
                input: TextIngestionActivity.Input(
                    country: input.country,
                    partition: input.partition
                ),
                options: ActivityOptions(
                    startToCloseTimeout: .seconds(30),
                    retryPolicy: .exponentialJitter(maxAttempts: 3)
                )
            )

            return Output(
                success: result.success,
                corpusId: result.corpusId,
                error: result.success ? nil : "Ingestion failed"
            )
        }
    }

    /// Text analysis workflow (models AnalyzeTextCorpus task)
    struct TextAnalysisWorkflow: WorkflowProtocol {

        struct Input: Codable, Sendable {
            let country: String
            let variant: Int
            let partition: Int
            let corpusId: String
        }

        struct Output: Codable, Sendable {
            let success: Bool
            let analysisId: String
        }

        func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
            // Simulate text analysis activity
            let result = try await context.executeActivity(
                TextAnalysisActivity.self,
                input: TextAnalysisActivity.Input(
                    country: input.country,
                    variant: input.variant,
                    corpusId: input.corpusId
                ),
                options: ActivityOptions(
                    startToCloseTimeout: .seconds(45),
                    retryPolicy: .exponentialJitter(maxAttempts: 2)
                )
            )

            return Output(
                success: result.success,
                analysisId: result.analysisId
            )
        }
    }

    /// Model generation workflow (models GenerateVoiceModel task)
    struct ModelGenerationWorkflow: WorkflowProtocol {

        struct Input: Codable, Sendable {
            let country: String
            let analysisIds: [String]
        }

        struct Output: Codable, Sendable {
            let success: Bool
            let modelUrl: String?
        }

        func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
            // Simulate model generation activity
            let result = try await context.executeActivity(
                ModelGenerationActivity.self,
                input: ModelGenerationActivity.Input(
                    country: input.country,
                    analysisIds: input.analysisIds
                ),
                options: ActivityOptions(
                    startToCloseTimeout: .minutes(2),
                    retryPolicy: .exponentialJitter(maxAttempts: 2)
                )
            )

            return Output(
                success: result.success,
                modelUrl: result.modelUrl
            )
        }
    }

    /// Model publishing workflow (models PublishModels task)
    struct ModelPublishingWorkflow: WorkflowProtocol {

        struct Input: Codable, Sendable {
            let modelUrls: [String]
            let countries: [String]
        }

        struct Output: Codable, Sendable {
            let success: Bool
            let publishedCount: Int
        }

        func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
            // Simulate model publishing activity
            let result = try await context.executeActivity(
                ModelPublishingActivity.self,
                input: ModelPublishingActivity.Input(
                    modelUrls: input.modelUrls,
                    countries: input.countries
                ),
                options: ActivityOptions(
                    startToCloseTimeout: .minutes(1),
                    retryPolicy: .exponentialJitter(maxAttempts: 3)
                )
            )

            return Output(
                success: result.success,
                publishedCount: result.publishedCount
            )
        }
    }

    // MARK: - DAG Test Activities

    struct TextIngestionActivity: ActivityParameters {

        struct Input: Codable, Sendable {
            let country: String
            let partition: Int
        }

        struct Output: Codable, Sendable {
            let success: Bool
            let corpusId: String
        }
    }

    struct TextAnalysisActivity: ActivityParameters {

        struct Input: Codable, Sendable {
            let country: String
            let variant: Int
            let corpusId: String
        }

        struct Output: Codable, Sendable {
            let success: Bool
            let analysisId: String
        }
    }

    struct ModelGenerationActivity: ActivityParameters {

        struct Input: Codable, Sendable {
            let country: String
            let analysisIds: [String]
        }

        struct Output: Codable, Sendable {
            let success: Bool
            let modelUrl: String?
        }
    }

    struct ModelPublishingActivity: ActivityParameters {

        struct Input: Codable, Sendable {
            let modelUrls: [String]
            let countries: [String]
        }

        struct Output: Codable, Sendable {
            let success: Bool
            let publishedCount: Int
        }
    }

    @Test("Monthly billing DAG workflow")
    func testMonthlyBillingDAGWorkflow() async throws {
        let (workflowEngine, processor) = try await setupWorkflowSystem()

        // Register the billing workflow types
        workflowEngine.registerWorkflow(MonthlyBillingWorkflow.self)
        workflowEngine.registerWorkflow(InvoiceCalculationWorkflow.self)
        workflowEngine.registerWorkflow(CreditCardChargingWorkflow.self)
        workflowEngine.registerWorkflow(PDFGenerationWorkflow.self)
        workflowEngine.registerWorkflow(EmailNotificationWorkflow.self)

        try await testWorkflow(processor) {
            let workflowId = try await workflowEngine.startWorkflow(
                MonthlyBillingWorkflow.self,
                input: MonthlyBillingWorkflow.Input(
                    userId: 448399,
                    userEmail: "sigmund.botsford@jaida.example",
                    adminEmail: "angelina.kutch@elfrieda.example"
                )
            )

            #expect(workflowId.value.count > 0)

            let finalStatus = try await waitForWorkflowCompletion(
                workflowId,
                engine: workflowEngine,
                inputType: MonthlyBillingWorkflow.Input.self,
                outputType: MonthlyBillingWorkflow.Output.self,
                timeout: .seconds(20),
                description: "monthly billing DAG workflow"
            )

            #expect(finalStatus.status == .completed)

            if let result = finalStatus.output {
                #expect(result.billingSuccess == true)
                #expect(result.invoiceId.count > 0)
                #expect(result.chargeId.count > 0)
                #expect(result.pdfUrl.count > 0)
                #expect(result.emailsSent == 2)
                #expect(result.totalProcessingTime > 0)
            }
        }
    }

    // MARK: - Monthly Billing Workflow Types

    /// Monthly billing workflow that models the sequential billing process with parallel email notifications
    struct MonthlyBillingWorkflow: WorkflowProtocol {

        struct Input: Codable, Sendable {
            let userId: Int
            let userEmail: String
            let adminEmail: String
        }

        struct Output: Codable, Sendable {
            let billingSuccess: Bool
            let invoiceId: String
            let chargeId: String
            let pdfUrl: String
            let emailsSent: Int
            let totalProcessingTime: TimeInterval
        }

        func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
            let startTime = Date()

            // Step 1: Calculate monthly invoice (CalculateMonthlyInvoice task)
            let invoiceResult = try await context.executeChildWorkflow(
                InvoiceCalculationWorkflow.self,
                input: InvoiceCalculationWorkflow.Input(userId: input.userId),
                options: ChildWorkflowOptions(
                    runTimeout: .seconds(30),
                    childWorkflowIdPrefix: "calculate_invoice"
                )
            )

            guard invoiceResult.output.success else {
                return Output(
                    billingSuccess: false,
                    invoiceId: "",
                    chargeId: "",
                    pdfUrl: "",
                    emailsSent: 0,
                    totalProcessingTime: Date().timeIntervalSince(startTime)
                )
            }

            // Step 2: Charge credit card (ChargeCreditCard task)
            // This depends on the invoice calculation completing
            let chargeResult = try await context.executeChildWorkflow(
                CreditCardChargingWorkflow.self,
                input: CreditCardChargingWorkflow.Input(
                    userId: input.userId,
                    invoiceId: invoiceResult.output.invoiceId,
                    amount: invoiceResult.output.amount
                ),
                options: ChildWorkflowOptions(
                    runTimeout: .seconds(60),
                    childWorkflowIdPrefix: "create_charge"
                )
            )

            guard chargeResult.output.success else {
                return Output(
                    billingSuccess: false,
                    invoiceId: invoiceResult.output.invoiceId,
                    chargeId: "",
                    pdfUrl: "",
                    emailsSent: 0,
                    totalProcessingTime: Date().timeIntervalSince(startTime)
                )
            }

            // Step 3: Generate PDF receipt (GeneratePDFReceipt task)
            // This depends on the charge completing
            let pdfResult = try await context.executeChildWorkflow(
                PDFGenerationWorkflow.self,
                input: PDFGenerationWorkflow.Input(
                    userId: input.userId,
                    chargeId: chargeResult.output.chargeId,
                    amount: invoiceResult.output.amount
                ),
                options: ChildWorkflowOptions(
                    runTimeout: .seconds(45),
                    childWorkflowIdPrefix: "generate_pdf"
                )
            )

            guard pdfResult.output.success else {
                return Output(
                    billingSuccess: false,
                    invoiceId: invoiceResult.output.invoiceId,
                    chargeId: chargeResult.output.chargeId,
                    pdfUrl: "",
                    emailsSent: 0,
                    totalProcessingTime: Date().timeIntervalSince(startTime)
                )
            }

            // Step 4: Send emails in parallel (EmailChargeReceipt tasks)
            // Both emails depend on the PDF being generated
            let emailInputs = [
                EmailNotificationWorkflow.Input(
                    recipientEmail: input.userEmail,
                    recipientType: "user",
                    pdfUrl: pdfResult.output.pdfUrl,
                    chargeId: chargeResult.output.chargeId
                ),
                EmailNotificationWorkflow.Input(
                    recipientEmail: input.adminEmail,
                    recipientType: "billing_admin",
                    pdfUrl: pdfResult.output.pdfUrl,
                    chargeId: chargeResult.output.chargeId
                ),
            ]

            let emailResults = try await withThrowingTaskGroup(of: EmailNotificationWorkflow.Output.self) { group in
                for (index, emailInput) in emailInputs.enumerated() {
                    group.addTask {
                        let result = try await context.executeChildWorkflow(
                            EmailNotificationWorkflow.self,
                            input: emailInput,
                            options: ChildWorkflowOptions(
                                runTimeout: .seconds(30),
                                childWorkflowIdPrefix: "email_receipt-\(index)"
                            )
                        )
                        return result.output
                    }
                }

                var outputs: [EmailNotificationWorkflow.Output] = []
                for try await output in group {
                    outputs.append(output)
                }
                return outputs
            }

            let successfulEmails = emailResults.filter { $0.success }.count

            return Output(
                billingSuccess: true,
                invoiceId: invoiceResult.output.invoiceId,
                chargeId: chargeResult.output.chargeId,
                pdfUrl: pdfResult.output.pdfUrl,
                emailsSent: successfulEmails,
                totalProcessingTime: Date().timeIntervalSince(startTime)
            )
        }
    }

    /// Invoice calculation workflow (models CalculateMonthlyInvoice task)
    struct InvoiceCalculationWorkflow: WorkflowProtocol {

        struct Input: Codable, Sendable {
            let userId: Int
        }

        struct Output: Codable, Sendable {
            let success: Bool
            let invoiceId: String
            let amount: Double
        }

        func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
            let result = try await context.executeActivity(
                InvoiceCalculationActivity.self,
                input: InvoiceCalculationActivity.Input(userId: input.userId),
                options: ActivityOptions(
                    startToCloseTimeout: .seconds(30),
                    retryPolicy: .exponentialJitter(maxAttempts: 3)
                )
            )

            return Output(
                success: result.success,
                invoiceId: result.invoiceId,
                amount: result.amount
            )
        }
    }

    /// Credit card charging workflow (models ChargeCreditCard task)
    struct CreditCardChargingWorkflow: WorkflowProtocol {

        struct Input: Codable, Sendable {
            let userId: Int
            let invoiceId: String
            let amount: Double
        }

        struct Output: Codable, Sendable {
            let success: Bool
            let chargeId: String
        }

        func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
            let result = try await context.executeActivity(
                CreditCardChargeActivity.self,
                input: CreditCardChargeActivity.Input(
                    userId: input.userId,
                    invoiceId: input.invoiceId,
                    amount: input.amount
                ),
                options: ActivityOptions(
                    startToCloseTimeout: .seconds(45),
                    retryPolicy: .exponentialJitter(maxAttempts: 5)
                )
            )

            return Output(
                success: result.success,
                chargeId: result.chargeId
            )
        }
    }

    /// PDF generation workflow (models GeneratePDFReceipt task)
    struct PDFGenerationWorkflow: WorkflowProtocol {

        struct Input: Codable, Sendable {
            let userId: Int
            let chargeId: String
            let amount: Double
        }

        struct Output: Codable, Sendable {
            let success: Bool
            let pdfUrl: String
        }

        func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
            let result = try await context.executeActivity(
                PDFGenerationActivity.self,
                input: PDFGenerationActivity.Input(
                    userId: input.userId,
                    chargeId: input.chargeId,
                    amount: input.amount
                ),
                options: ActivityOptions(
                    startToCloseTimeout: .seconds(60),
                    retryPolicy: .exponentialJitter(maxAttempts: 2)
                )
            )

            return Output(
                success: result.success,
                pdfUrl: result.pdfUrl
            )
        }
    }

    /// Email notification workflow (models EmailChargeReceipt task)
    struct EmailNotificationWorkflow: WorkflowProtocol {

        struct Input: Codable, Sendable {
            let recipientEmail: String
            let recipientType: String
            let pdfUrl: String
            let chargeId: String
        }

        struct Output: Codable, Sendable {
            let success: Bool
            let emailId: String
        }

        func run(input: Input, context: WorkflowExecutionContext) async throws -> Output {
            let result = try await context.executeActivity(
                EmailSendingActivity.self,
                input: EmailSendingActivity.Input(
                    recipientEmail: input.recipientEmail,
                    recipientType: input.recipientType,
                    pdfUrl: input.pdfUrl,
                    chargeId: input.chargeId
                ),
                options: ActivityOptions(
                    startToCloseTimeout: .seconds(30),
                    retryPolicy: .exponentialJitter(maxAttempts: 3)
                )
            )

            return Output(
                success: result.success,
                emailId: result.emailId
            )
        }
    }

    // MARK: - Billing Test Activities

    struct InvoiceCalculationActivity: ActivityParameters {

        struct Input: Codable, Sendable {
            let userId: Int
        }

        struct Output: Codable, Sendable {
            let success: Bool
            let amount: Double
            let invoiceId: String
        }
    }

    struct CreditCardChargeActivity: ActivityParameters {

        struct Input: Codable, Sendable {
            let userId: Int
            let invoiceId: String
            let amount: Double
        }

        struct Output: Codable, Sendable {
            let success: Bool
            let chargeId: String
        }
    }

    struct PDFGenerationActivity: ActivityParameters {

        struct Input: Codable, Sendable {
            let userId: Int
            let chargeId: String
            let amount: Double
        }

        struct Output: Codable, Sendable {
            let success: Bool
            let pdfUrl: String
        }
    }

    struct EmailSendingActivity: ActivityParameters {

        struct Input: Codable, Sendable {
            let recipientEmail: String
            let recipientType: String
            let pdfUrl: String
            let chargeId: String
        }

        struct Output: Codable, Sendable {
            let success: Bool
            let emailId: String
        }
    }
}

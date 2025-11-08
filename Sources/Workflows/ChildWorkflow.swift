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
import NIOCore

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

// MARK: - Child Workflow Execution

/// Options for child workflow execution
public struct ChildWorkflowOptions: Codable, Sendable {
    /// Maximum time allowed for a single child workflow execution run
    /// This timeout applies to individual workflow runs, measuring from workflow start to completion
    /// If nil, individual runs use a default timeout or inherit from parent workflow constraints
    public let runTimeout: Duration?

    /// Maximum total time allowed for child workflow execution including all retries
    /// This timeout encompasses the entire child workflow lifecycle from start to final completion
    /// If nil, the child workflow can execute indefinitely within the parent workflow's constraints
    public let executionTimeout: Duration?

    /// Maximum time allowed for processing a single workflow task
    /// This timeout controls how long a workflow task can take to process on a worker
    /// If nil, uses the system default task timeout
    public let taskTimeout: Duration?

    /// Custom child workflow ID prefix
    public let childWorkflowIdPrefix: String?
    /// Child key identifier for this workflow run
    public let childKey: String?
    /// Child index if there are multiple child workflows
    public let childIndex: Int?

    public init(
        runTimeout: Duration? = nil,
        executionTimeout: Duration? = nil,
        taskTimeout: Duration? = nil,
        childWorkflowIdPrefix: String? = nil,
        childKey: String? = nil,
        childIndex: Int? = nil
    ) {
        self.runTimeout = runTimeout
        self.executionTimeout = executionTimeout
        self.taskTimeout = taskTimeout
        self.childWorkflowIdPrefix = childWorkflowIdPrefix
        self.childKey = childKey
        self.childIndex = childIndex
    }
}

/// Result of child workflow execution
public struct ChildWorkflowResult<Output: Codable & Sendable>: Sendable {
    /// The child workflow ID
    public let childWorkflowId: WorkflowID
    /// The workflow output
    public let output: Output
    /// Execution duration
    public let duration: TimeInterval
    /// Whether the workflow completed successfully
    public let completed: Bool

    internal init(childWorkflowId: WorkflowID, output: Output, duration: TimeInterval, completed: Bool) {
        self.childWorkflowId = childWorkflowId
        self.output = output
        self.duration = duration
        self.completed = completed
    }
}

// MARK: - WorkflowExecutionContext Extension

extension WorkflowExecutionContext {

    /// Execute a child workflow within this workflow
    public func executeChildWorkflow<W: WorkflowProtocol>(
        _ workflowType: W.Type,
        input: W.Input,
        options: ChildWorkflowOptions = .init()
    ) async throws -> ChildWorkflowResult<W.Output> {

        // Generate deterministic child workflow ID
        let inputBuffer = try JSONEncoder().encodeAsByteBuffer(input, allocator: self.allocator)
        let inputHash = WorkflowUtilities.deterministicHash(buffer: inputBuffer, activityName: W.workflowName)
        let childWorkflowIdPrefix = options.childWorkflowIdPrefix ?? "child"
        // Generate deterministic run ID for replay safety
        let runIdInput = "\(workflowId.value):\(childWorkflowIdPrefix):\(W.workflowName):\(inputHash):run"
        let deterministicRunId = WorkflowUtilities.deterministicHash(string: runIdInput)
        let childWorkflowId = WorkflowID(
            workflowId: "\(workflowId.workflowId):\(childWorkflowIdPrefix):\(W.workflowName):\(inputHash)",
            runId: deterministicRunId
        )

        // Save the child workflow state via WorkflowRepository with proper parent relationship
        let childInputBuffer = try workflowRegistry.encode(input)
        let childExecution = WorkflowExecution(
            id: childWorkflowId,
            workflowName: W.workflowName,
            inputData: childInputBuffer,
            workflowState: nil,
            status: .queued,
            createdAt: .now,
            queuedAt: .now,
            parentId: workflowId,
            parentStepRunId: WorkflowUtilities.deterministicHash(
                string: "\(workflowId.value):step:\(currentStep)",
                context: "step-run-id"
            ),
            childKey: options.childKey,
            childIndex: options.childIndex
        )
        try await queueDriver.workflowRepository.createExecution(childExecution)

        logger.debug(
            "🚀 Starting child workflow",
            metadata: [
                "parentWorkflowId": .string(workflowId.value),
                "childWorkflowId": .string(childWorkflowId.value),
                "childWorkflowType": .string(W.workflowName),
            ]
        )

        // Check if child workflow has already been started (replay safety)
        // Use proper parent-child relationship instead of composite key
        // Check if child workflow already exists
        if let existingExecution = try? await queueDriver.workflowRepository.getExecution(childWorkflowId) {
            logger.debug(
                "Child workflow already exists, checking status",
                metadata: [
                    "childWorkflowId": .string(childWorkflowId.value),
                    "existingStatus": .string(existingExecution.status.rawValue),
                    "createdAt": .stringConvertible(existingExecution.createdAt),
                ]
            )

            // If already completed, return cached result
            if existingExecution.status == .completed, let resultBuffer = existingExecution.result {
                let output = try workflowRegistry.decode(resultBuffer, as: W.Output.self)
                let duration = existingExecution.completedAt?.timeIntervalSince(existingExecution.startedAt ?? existingExecution.createdAt) ?? 0
                return ChildWorkflowResult(
                    childWorkflowId: childWorkflowId,
                    output: output,
                    duration: duration,
                    completed: true
                )
            }

            // If failed, throw error
            if existingExecution.status == .failed {
                throw WorkflowError.workflowFailed(existingExecution.error ?? "Child workflow failed")
            }

            // If already running, wait for completion
            if existingExecution.status == .running {
                return try await waitForChildWorkflowCompletion(
                    childWorkflowId: childWorkflowId,
                    childWorkflowType: W.workflowName,
                    inputType: W.Input.self,
                    outputType: W.Output.self,
                    options: options
                )
            }

            // If queued, we need to start it by pushing the coordinator job
            // Fall through to push the coordinator job below
        }

        // Create workflow coordinator job directly (skip the extra layer)
        let coordinatorJob = WorkflowCoordinatorJob(
            workflowId: childWorkflowId,
            workflowType: W.workflowName,
            action: .start,
            stepIndex: 0
        )

        logger.debug(
            "📤 About to push child workflow coordinator job",
            metadata: [
                "childWorkflowId": .string(childWorkflowId.value),
                "childWorkflowType": .string(W.workflowName),
                "parentWorkflowId": .string(workflowId.value),
            ]
        )

        // Push coordinator job to queue (non-blocking)
        let jobId = try await pushJob(coordinatorJob)

        logger.debug(
            "✅ Successfully pushed child workflow coordinator job",
            metadata: [
                "jobId": .stringConvertible(jobId),
                "childWorkflowId": .string(childWorkflowId.value),
                "childWorkflowType": .string(W.workflowName),
            ]
        )

        // Wait for child workflow completion (like activities do)
        return try await waitForChildWorkflowCompletion(
            childWorkflowId: childWorkflowId,
            childWorkflowType: W.workflowName,
            inputType: W.Input.self,
            outputType: W.Output.self,
            options: options
        )
    }

    /// Cancel a running child workflow
    public func cancelChildWorkflow(_ childWorkflowId: WorkflowID) async throws {
        logger.debug("Cancelling child workflow")

        // Create cancellation coordinator job directly
        let coordinatorJob = WorkflowCoordinatorJob(
            workflowId: childWorkflowId,
            workflowType: "",  // Not needed for cancellation
            action: .cancel,
            stepIndex: 0
        )

        _ = try await pushJob(coordinatorJob)
    }

    /// Get the status of a child workflow
    func getChildWorkflowStatus<Input: Codable & Sendable, Output: Codable & Sendable>(
        _ childWorkflowId: WorkflowID,
        inputType: Input.Type,
        outputType: Output.Type
    ) async throws -> WorkflowExecutionStatus<Input, Output>? {
        // Get execution from repository
        guard let execution = try await queueDriver.workflowRepository.getExecution(childWorkflowId) else {
            return nil
        }

        // Decode input and output from ByteBuffers using registry
        let input = try workflowRegistry.decode(execution.inputData, as: Input.self)
        let output = try execution.result.map { try workflowRegistry.decode($0, as: Output.self) }

        // Convert WorkflowExecution to WorkflowExecutionStatus
        var workflowState = WorkflowState<Input, Output>(
            id: execution.id,
            workflowType: execution.workflowName,
            input: input,
            status: execution.status,
            queuedAt: execution.queuedAt,
            scheduledAt: nil,
            startTime: execution.startedAt ?? execution.createdAt,
            currentStep: execution.currentStep
        )

        // Set additional properties
        workflowState.output = output
        workflowState.error = execution.error
        workflowState.endTime = execution.completedAt

        return WorkflowExecutionStatus(from: workflowState)
    }

    // MARK: - Private Helper Methods

    private func waitForChildWorkflowCompletion<Input: Codable & Sendable, Output: Codable & Sendable>(
        childWorkflowId: WorkflowID,
        childWorkflowType: String,
        inputType: Input.Type,
        outputType: Output.Type,
        options: ChildWorkflowOptions
    ) async throws -> ChildWorkflowResult<Output> {

        logger.debug(
            "⏳ Parent waiting for child workflow completion (event-driven)",
            metadata: [
                "parentWorkflowId": .string(workflowId.value),
                "childWorkflowId": .string(childWorkflowId.value),
            ]
        )

        // Create completion stream for event-driven waiting
        let (stream, continuation) = ChildWorkflowCompletionStream.makeStream(childWorkflowId: childWorkflowId.value)

        // Register the continuation in the global registry so the middleware can complete it
        ChildWorkflowRegistry.shared.register(childWorkflowId: childWorkflowId, continuation: continuation)

        let startWaitTime = Date()

        // Calculate timeout from options
        let timeoutTask: Task<Void, Never>?
        if let runTimeout = options.runTimeout {
            let timeoutSeconds =
                TimeInterval(runTimeout.components.seconds) + (TimeInterval(runTimeout.components.attoseconds) / 1_000_000_000_000_000_000)
            timeoutTask = Task {
                try? await Task.sleep(for: .seconds(timeoutSeconds))
                ChildWorkflowRegistry.shared.failWorkflow(
                    childWorkflowId: childWorkflowId,
                    error: WorkflowError.workflowTimedOut
                )
            }
        } else if let executionTimeout = options.executionTimeout {
            let timeoutSeconds =
                TimeInterval(executionTimeout.components.seconds)
                + (TimeInterval(executionTimeout.components.attoseconds) / 1_000_000_000_000_000_000)
            timeoutTask = Task {
                try? await Task.sleep(for: .seconds(timeoutSeconds))
                ChildWorkflowRegistry.shared.failWorkflow(
                    childWorkflowId: childWorkflowId,
                    error: WorkflowError.workflowTimedOut
                )
            }
        } else {
            timeoutTask = nil
        }

        defer {
            // Cancel timeout task and cleanup registry
            timeoutTask?.cancel()
            ChildWorkflowRegistry.shared.cancelWorkflow(childWorkflowId: childWorkflowId)
        }

        // Wait for child workflow completion via event stream
        for try await result in stream {
            switch result {
            case .success(let outputBuffer):
                logger.debug(
                    "🎉 Child workflow completed successfully! (event-driven)",
                    metadata: [
                        "childWorkflowId": .string(childWorkflowId.value),
                        "waitTime": .stringConvertible(Date().timeIntervalSince(startWaitTime)),
                    ]
                )

                // Decode the output from ByteBuffer
                let output = try JSONDecoder().decode(Output.self, from: outputBuffer)

                return ChildWorkflowResult(
                    childWorkflowId: childWorkflowId,
                    output: output,
                    duration: Date().timeIntervalSince(startWaitTime),
                    completed: true
                )

            case .failure(let error):
                logger.error(
                    "❌ Child workflow failed (event-driven)",
                    metadata: [
                        "childWorkflowId": .string(childWorkflowId.value),
                        "error": .string(error.localizedDescription),
                    ]
                )
                throw error
            }
        }

        // This should never be reached due to the stream design
        throw WorkflowError.workflowFailed("Child workflow stream ended unexpectedly")
    }
}

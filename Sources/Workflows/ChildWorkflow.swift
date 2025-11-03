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
    /// Maximum time the child workflow can run before timing out
    public let timeout: Duration?
    /// Whether to inherit parent workflow's timeout
    public let inheritTimeout: Bool
    /// Custom child workflow ID prefix
    public let childWorkflowIdPrefix: String?

    public init(
        timeout: Duration? = nil,
        inheritTimeout: Bool = false,
        childWorkflowIdPrefix: String? = nil
    ) {
        self.timeout = timeout
        self.inheritTimeout = inheritTimeout
        self.childWorkflowIdPrefix = childWorkflowIdPrefix
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

// MARK: - Child Workflow Job Types

/// Job that manages child workflow lifecycle
internal struct ChildWorkflowCoordinatorJob: JobParameters {
    public static let jobName = "ChildWorkflowCoordinator"

    /// Parent workflow ID
    public let parentWorkflowId: WorkflowID
    /// Child workflow ID
    public let childWorkflowId: WorkflowID
    /// Child workflow type name
    public let childWorkflowType: String
    /// Serialized input for child workflow
    public let childInput: ByteBuffer
    /// Action to perform
    public let action: ChildWorkflowAction
    /// Child workflow options
    public let options: ChildWorkflowOptions

    public init(
        parentWorkflowId: WorkflowID,
        childWorkflowId: WorkflowID,
        childWorkflowType: String,
        childInput: ByteBuffer,
        action: ChildWorkflowAction,
        options: ChildWorkflowOptions = .init()
    ) {
        self.parentWorkflowId = parentWorkflowId
        self.childWorkflowId = childWorkflowId
        self.childWorkflowType = childWorkflowType
        self.childInput = childInput
        self.action = action
        self.options = options
    }
}

/// Actions for child workflow coordination
public enum ChildWorkflowAction: String, Codable, Sendable {
    case start
    case monitor
    case complete
    case fail
    case cancel
}

/// Information about child workflow relationship
internal struct ChildWorkflowInfo: Codable, Sendable {
    let parentWorkflowId: WorkflowID
    let childWorkflowId: WorkflowID
    let childWorkflowType: String
    let startTime: Date
    var endTime: Date?
    var status: WorkflowStatus
    var output: ByteBuffer?
    var error: String?

    init(
        parentWorkflowId: WorkflowID,
        childWorkflowId: WorkflowID,
        childWorkflowType: String,
        startTime: Date = .now
    ) {
        self.parentWorkflowId = parentWorkflowId
        self.childWorkflowId = childWorkflowId
        self.childWorkflowType = childWorkflowType
        self.startTime = startTime
        self.endTime = nil
        self.status = .running
        self.output = nil
        self.error = nil
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
        let inputBuffer = try JSONEncoder().encodeAsByteBuffer(input, allocator: ByteBufferAllocator())
        let inputHash = WorkflowUtilities.deterministicHash(buffer: inputBuffer, activityName: W.workflowName)
        let childWorkflowIdPrefix = options.childWorkflowIdPrefix ?? "child"
        // Generate deterministic run ID for replay safety
        let runIdInput = "\(workflowId.value):\(childWorkflowIdPrefix):\(W.workflowName):\(inputHash):run"
        let deterministicRunId = WorkflowUtilities.deterministicHash(string: runIdInput)
        let childWorkflowId = WorkflowID(
            workflowId: "\(workflowId.workflowId):\(childWorkflowIdPrefix):\(W.workflowName):\(inputHash)",
            runId: deterministicRunId
        )

        logger.debug(
            "🚀 Starting child workflow",
            metadata: [
                "parentWorkflowId": .string(workflowId.value),
                "childWorkflowId": .string(childWorkflowId.value),
                "childWorkflowType": .string(W.workflowName),
            ]
        )

        // Check if child workflow has already been started (replay safety)
        let childInfoKey = "child_workflow:\(workflowId.value):\(childWorkflowId.value)"
        if let existingInfoBuffer = try await metadataQueue.getMetadata(childInfoKey) {
            let existingInfo = try JSONDecoder().decode(ChildWorkflowInfo.self, from: existingInfoBuffer)

            logger.debug("Child workflow already exists, checking status")

            // If already completed, return cached result
            if existingInfo.status == WorkflowStatus.completed, let output = existingInfo.output {
                let result = try JSONDecoder().decode(W.Output.self, from: output)
                let duration = existingInfo.endTime?.timeIntervalSince(existingInfo.startTime) ?? 0
                return ChildWorkflowResult(
                    childWorkflowId: childWorkflowId,
                    output: result,
                    duration: duration,
                    completed: true
                )
            }

            // If failed, throw error
            if existingInfo.status == WorkflowStatus.failed {
                throw WorkflowError.workflowFailed(existingInfo.error ?? "Child workflow failed")
            }

            // If still running, wait for completion
            return try await waitForChildWorkflowCompletion(
                childWorkflowId: childWorkflowId,
                childWorkflowType: W.workflowName,
                outputType: W.Output.self
            )
        }

        // Create child workflow info
        let childInfo = ChildWorkflowInfo(
            parentWorkflowId: workflowId,
            childWorkflowId: childWorkflowId,
            childWorkflowType: W.workflowName
        )

        // Store child workflow relationship
        let childInfoBuffer = try JSONEncoder().encodeAsByteBuffer(
            childInfo,
            allocator: ByteBufferAllocator()
        )
        try await metadataQueue.setMetadata(key: childInfoKey, value: childInfoBuffer)

        // Create child workflow coordinator job
        let coordinatorJob = ChildWorkflowCoordinatorJob(
            parentWorkflowId: workflowId,
            childWorkflowId: childWorkflowId,
            childWorkflowType: W.workflowName,
            childInput: inputBuffer,
            action: .start,
            options: options
        )

        // Push coordinator job to queue (non-blocking)
        let jobId = try await jobQueue.push(coordinatorJob)

        logger.debug("✅ Pushed child workflow coordinator job", metadata: ["jobId": .stringConvertible(jobId)])

        // Wait for child workflow completion (like activities do)
        return try await waitForChildWorkflowCompletion(
            childWorkflowId: childWorkflowId,
            childWorkflowType: W.workflowName,
            outputType: W.Output.self
        )
    }

    /// Execute multiple child workflows in parallel
    public func executeChildWorkflowsInParallel<W: WorkflowProtocol>(
        _ workflowType: W.Type,
        inputs: [W.Input],
        options: ChildWorkflowOptions = .init()
    ) async throws -> [ChildWorkflowResult<W.Output>] {

        try await withThrowingTaskGroup(of: ChildWorkflowResult<W.Output>.self) { group in
            for (index, input) in inputs.enumerated() {
                group.addTask {
                    // Use index to ensure unique child workflow IDs
                    let childOptions = ChildWorkflowOptions(
                        timeout: options.timeout,
                        inheritTimeout: options.inheritTimeout,
                        childWorkflowIdPrefix: "\(options.childWorkflowIdPrefix ?? "parallel")_\(index)"
                    )

                    return try await self.executeChildWorkflow(
                        workflowType,
                        input: input,
                        options: childOptions
                    )
                }
            }

            var results: [ChildWorkflowResult<W.Output>] = []
            for try await result in group {
                results.append(result)
            }
            return results
        }
    }

    /// Cancel a running child workflow
    public func cancelChildWorkflow(_ childWorkflowId: WorkflowID) async throws {
        logger.debug("Cancelling child workflow")

        // Create cancellation coordinator job
        let coordinatorJob = ChildWorkflowCoordinatorJob(
            parentWorkflowId: workflowId,
            childWorkflowId: childWorkflowId,
            childWorkflowType: "",  // Not needed for cancellation
            childInput: ByteBuffer(bytes: []),
            action: .cancel
        )

        _ = try await jobQueue.push(coordinatorJob)
    }

    /// Get the status of a child workflow
    public func getChildWorkflowStatus(_ childWorkflowId: WorkflowID) async throws -> WorkflowExecutionStatus? {
        // Check if we have child workflow info
        let childInfoKey = "child_workflow:\(workflowId.value):\(childWorkflowId.value)"
        guard let childInfoBuffer = try await metadataQueue.getMetadata(childInfoKey) else {
            return nil
        }

        let childInfo = try JSONDecoder().decode(ChildWorkflowInfo.self, from: childInfoBuffer)

        let workflowState = WorkflowState(
            id: childWorkflowId,
            workflowType: childInfo.childWorkflowType,
            input: ByteBuffer(),
            status: childInfo.status,
            queuedAt: childInfo.startTime,
            scheduledAt: nil,
            startTime: childInfo.startTime,
            currentStep: 0
        )
        return WorkflowExecutionStatus(from: workflowState)
    }

    // MARK: - Private Helper Methods

    private func waitForChildWorkflowCompletion<Output: Codable & Sendable>(
        childWorkflowId: WorkflowID,
        childWorkflowType: String,
        outputType: Output.Type
    ) async throws -> ChildWorkflowResult<Output> {

        logger.debug(
            "⏳ Parent waiting for child workflow completion",
            metadata: [
                "parentWorkflowId": .string(workflowId.value),
                "childWorkflowId": .string(childWorkflowId.value),
            ]
        )

        let childInfoKey = "child_workflow:\(workflowId.value):\(childWorkflowId.value)"
        let maxWaitTime: TimeInterval = 30  // 30 seconds for debugging
        let startWaitTime = Date()
        var pollCount = 0

        while Date().timeIntervalSince(startWaitTime) < maxWaitTime {
            pollCount += 1

            // Check child workflow status
            guard let childInfoBuffer = try await metadataQueue.getMetadata(childInfoKey) else {
                logger.error("❌ Child workflow info not found during polling", metadata: ["key": .string(childInfoKey)])
                throw WorkflowError.executionNotFound(childWorkflowId)
            }

            let childInfo = try JSONDecoder().decode(ChildWorkflowInfo.self, from: childInfoBuffer)

            if pollCount % 10 == 0 {  // Log every 10 polls (1 second)
                logger.debug(
                    "🔄 Polling child workflow status (poll #\(pollCount))",
                    metadata: [
                        "childWorkflowId": .string(childWorkflowId.value),
                        "status": .string(childInfo.status.rawValue),
                        "waitTime": .stringConvertible(Date().timeIntervalSince(startWaitTime)),
                    ]
                )
            }

            switch childInfo.status {
            case .completed:
                logger.debug("🎉 Child workflow completed successfully!")
                guard let outputBuffer = childInfo.output else {
                    throw WorkflowError.noOutput(childWorkflowId)
                }

                let output = try JSONDecoder().decode(Output.self, from: outputBuffer)
                let duration = childInfo.endTime?.timeIntervalSince(childInfo.startTime) ?? 0

                return ChildWorkflowResult(
                    childWorkflowId: childWorkflowId,
                    output: output,
                    duration: duration,
                    completed: true
                )

            case .failed:
                logger.error("❌ Child workflow failed: \(childInfo.error ?? "Unknown error")")
                throw WorkflowError.workflowFailed(childInfo.error ?? "Child workflow failed")

            case .cancelled:
                logger.error("🚫 Child workflow was cancelled")
                throw WorkflowError.workflowCancelled(childWorkflowId)

            case .running:
                // Continue waiting
                try await Task.sleep(for: .milliseconds(100))
            }
        }

        logger.error("⏰ Child workflow timed out after \(maxWaitTime) seconds")
        throw WorkflowError.timeout(childWorkflowId, Duration.seconds(Int64(maxWaitTime)))
    }
}

// MARK: - WorkflowEngine Extension

extension WorkflowEngine {

    /// Register child workflow coordinator job processor
    public func registerChildWorkflowCoordinator() {
        jobQueue.registerJob(parameters: ChildWorkflowCoordinatorJob.self) { job, context in
            try await self.handleChildWorkflowCoordination(job, context: context)
        }
    }

    /// Handle child workflow coordination
    private func handleChildWorkflowCoordination(
        _ job: ChildWorkflowCoordinatorJob,
        context: JobExecutionContext
    ) async throws {

        switch job.action {
        case .start:
            try await startChildWorkflow(job, context: context)

        case .monitor:
            try await monitorChildWorkflow(job, context: context)

        case .cancel:
            try await cancelChildWorkflow(job, context: context)

        case .complete, .fail:
            // These are handled by the monitoring system
            break
        }
    }

    private func startChildWorkflow(
        _ job: ChildWorkflowCoordinatorJob,
        context: JobExecutionContext
    ) async throws {

        logger.debug(
            "🔄 Starting child workflow",
            metadata: [
                "parentWorkflowId": .string(job.parentWorkflowId.value),
                "childWorkflowId": .string(job.childWorkflowId.value),
                "childWorkflowType": .string(job.childWorkflowType),
            ]
        )

        // Create child workflow state first
        let childWorkflowState = WorkflowState(
            id: job.childWorkflowId,
            workflowType: job.childWorkflowType,
            input: job.childInput,
            status: .running,
            queuedAt: context.queuedAt,
            scheduledAt: context.nextScheduledAt,
            startTime: .now
        )

        // Store child workflow state
        let stateBuffer = try JSONEncoder().encodeAsByteBuffer(
            childWorkflowState,
            allocator: ByteBufferAllocator()
        )

        try await jobQueue.queue.setMetadata(
            key: "workflow:\(job.childWorkflowId.value)",
            value: stateBuffer
        )

        logger.debug("✅ Created child workflow state")

        // Start the child workflow using existing infrastructure
        let childCoordinatorJob = WorkflowCoordinatorJob(
            workflowId: job.childWorkflowId,
            workflowType: job.childWorkflowType,
            action: .start,
            stepIndex: 0
        )

        let childJobId = try await jobQueue.push(childCoordinatorJob)
        logger.debug("✅ Pushed child workflow coordinator job", metadata: ["jobId": .stringConvertible(childJobId)])

        // Schedule monitoring job with delay to let child start
        let monitorJob = ChildWorkflowCoordinatorJob(
            parentWorkflowId: job.parentWorkflowId,
            childWorkflowId: job.childWorkflowId,
            childWorkflowType: job.childWorkflowType,
            childInput: job.childInput,
            action: .monitor
        )

        let monitorJobId = try await jobQueue.push(monitorJob)
        logger.debug("✅ Pushed monitor job", metadata: ["monitorJobId": .stringConvertible(monitorJobId)])
    }

    private func monitorChildWorkflow(
        _ job: ChildWorkflowCoordinatorJob,
        context: JobExecutionContext
    ) async throws {

        logger.debug(
            "👀 Monitoring child workflow",
            metadata: [
                "parentWorkflowId": .string(job.parentWorkflowId.value),
                "childWorkflowId": .string(job.childWorkflowId.value),
            ]
        )

        // Get child workflow status
        do {
            let childState = try await getInternalWorkflowState(job.childWorkflowId)

            logger.debug(
                "📊 Child workflow status",
                metadata: [
                    "childWorkflowId": .string(job.childWorkflowId.value),
                    "status": .string(childState.status.rawValue),
                ]
            )

            // Update child workflow info with current status
            let childInfoKey = "child_workflow:\(job.parentWorkflowId.value):\(job.childWorkflowId.value)"
            guard let childInfoBuffer = try await jobQueue.queue.getMetadata(childInfoKey) else {
                logger.error("❌ Child workflow info not found", metadata: ["key": .string(childInfoKey)])
                return
            }

            var childInfo = try JSONDecoder().decode(ChildWorkflowInfo.self, from: childInfoBuffer)
            childInfo.status = childState.status
            childInfo.endTime = childState.endTime
            childInfo.output = childState.output
            childInfo.error = childState.error

            // Save updated child info
            let updatedInfoBuffer = try JSONEncoder().encodeAsByteBuffer(
                childInfo,
                allocator: ByteBufferAllocator()
            )
            try await jobQueue.queue.setMetadata(key: childInfoKey, value: updatedInfoBuffer)

            // If still running, schedule another monitoring check
            if childState.status == .running {
                logger.info("⏳ Child still running, scheduling next monitor check")
                let nextMonitorJob = ChildWorkflowCoordinatorJob(
                    parentWorkflowId: job.parentWorkflowId,
                    childWorkflowId: job.childWorkflowId,
                    childWorkflowType: job.childWorkflowType,
                    childInput: job.childInput,
                    action: .monitor
                )

                _ = try await jobQueue.push(nextMonitorJob)
            } else {
                logger.info("🎯 Child workflow completed with status: \(childState.status.rawValue)")
            }
        } catch {
            logger.error("❌ Error monitoring child workflow: \(error)")

            // Mark child workflow as failed
            let childInfoKey = "child_workflow:\(job.parentWorkflowId.value):\(job.childWorkflowId.value)"
            if let childInfoBuffer = try await jobQueue.queue.getMetadata(childInfoKey) {
                var childInfo = try JSONDecoder().decode(ChildWorkflowInfo.self, from: childInfoBuffer)
                childInfo.status = .failed
                childInfo.endTime = .now
                childInfo.error = error.localizedDescription

                let updatedInfoBuffer = try JSONEncoder().encodeAsByteBuffer(
                    childInfo,
                    allocator: ByteBufferAllocator()
                )
                try await jobQueue.queue.setMetadata(key: childInfoKey, value: updatedInfoBuffer)
            }
        }
    }

    private func cancelChildWorkflow(
        _ job: ChildWorkflowCoordinatorJob,
        context: JobExecutionContext
    ) async throws {

        logger.debug(
            "Cancelling child workflow",
            metadata: [
                "childWorkflowId": .string(job.childWorkflowId.value)
            ]
        )

        // Cancel the child workflow
        try await cancelWorkflow(job.childWorkflowId)

        // Update child workflow info
        let childInfoKey = "child_workflow:\(job.parentWorkflowId.value):\(job.childWorkflowId.value)"
        if let childInfoBuffer = try await jobQueue.queue.getMetadata(childInfoKey) {
            var childInfo = try JSONDecoder().decode(ChildWorkflowInfo.self, from: childInfoBuffer)
            childInfo.status = .cancelled
            childInfo.endTime = .now

            let updatedInfoBuffer = try JSONEncoder().encodeAsByteBuffer(
                childInfo,
                allocator: ByteBufferAllocator()
            )
            try await jobQueue.queue.setMetadata(key: childInfoKey, value: updatedInfoBuffer)
        }
    }
}

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
import NIOCore

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Enhanced workflow queue driver that combines job queue functionality with workflow repositories
public protocol WorkflowQueueDriver: JobQueueDriver, JobMetadataDriver, WorkflowRepository, CancellableJobQueue, ResumableJobQueue {

    /// Get the workflow repository for managing workflow-specific data
    var workflowRepository: WorkflowRepository { get }

    /// Initialize workflow-specific storage and indices
    func initializeWorkflowStorage() async throws

    /// Push a workflow job (non-generic helper for existential usage)
    func pushWorkflowJob<Parameters: JobParameters>(_ job: Parameters) async throws -> JobID

    /// Push a workflow job with delay options
    func pushWorkflowJob<Parameters: JobParameters>(_ job: Parameters, delayUntil: Date) async throws -> JobID

    /// Queue a new workflow execution
    /// - Parameters:
    ///   - workflowName: Name of the workflow to execute
    ///   - input: Input data for the workflow
    ///   - options: Job options for the workflow execution
    /// - Returns: Workflow execution ID
    @discardableResult
    func queueWorkflow<Input: Codable & Sendable>(
        _ workflowName: String,
        input: Input,
        options: JobOptions
    ) async throws -> WorkflowID

    /// Resume a paused workflow execution
    /// - Parameter workflowId: ID of the workflow to resume
    func resumeWorkflow(_ workflowId: WorkflowID) async throws

    /// Pause a running workflow execution
    /// - Parameter workflowId: ID of the workflow to pause
    func pauseWorkflow(_ workflowId: WorkflowID) async throws

    /// Cancel a workflow execution
    /// - Parameter workflowId: ID of the workflow to cancel
    func cancelWorkflow(_ workflowId: WorkflowID) async throws

    /// Get workflow execution metrics
    /// - Parameter workflowName: Optional workflow name to filter by
    /// - Returns: Workflow metrics
    func getWorkflowMetrics(_ workflowName: String?) async throws -> WorkflowMetrics

    /// Get workflow execution summary without requiring compile-time types
    /// - Parameter workflowId: Workflow execution ID
    /// - Returns: Workflow execution summary if found
    func getExecutionSummary(_ workflowId: WorkflowID) async throws -> WorkflowExecutionSummary?

    /// Update workflow execution status without requiring types
    /// - Parameters:
    ///   - workflowId: Workflow execution ID
    ///   - status: New workflow status
    ///   - startTime: Optional start time if transitioning to running
    ///   - endTime: Optional end time if completing
    ///   - error: Optional error message if failing
    func updateExecutionStatus(
        _ workflowId: WorkflowID,
        status: WorkflowStatus,
        startTime: Date?,
        endTime: Date?,
        error: String?
    ) async throws

}

/// Workflow execution metrics
public struct WorkflowMetrics: Codable, Sendable {
    /// Total number of workflow executions
    public let totalExecutions: Int
    /// Number of pending executions
    public let pendingExecutions: Int
    /// Number of running executions
    public let runningExecutions: Int
    /// Number of completed executions
    public let completedExecutions: Int
    /// Number of failed executions
    public let failedExecutions: Int
    /// Number of cancelled executions
    public let cancelledExecutions: Int
    /// Average execution time in seconds
    public let averageExecutionTime: Double?
    /// Success rate (0.0 to 1.0)
    public let successRate: Double

    public init(
        totalExecutions: Int,
        pendingExecutions: Int,
        runningExecutions: Int,
        completedExecutions: Int,
        failedExecutions: Int,
        cancelledExecutions: Int,
        averageExecutionTime: Double? = nil,
        successRate: Double = 0.0
    ) {
        self.totalExecutions = totalExecutions
        self.pendingExecutions = pendingExecutions
        self.runningExecutions = runningExecutions
        self.completedExecutions = completedExecutions
        self.failedExecutions = failedExecutions
        self.cancelledExecutions = cancelledExecutions
        self.averageExecutionTime = averageExecutionTime
        self.successRate = successRate
    }
}

/// Default implementations for WorkflowQueueDriver
extension WorkflowQueueDriver {

    /// Default implementation for workflow repository access
    public var workflowRepository: WorkflowRepository {
        self
    }

    /// Default implementation for workflow storage initialization
    public func initializeWorkflowStorage() async throws {
        // Override in concrete implementations if needed
    }

    /// Default implementation for workflow metrics calculation
    public func getWorkflowMetrics(_ workflowName: String?) async throws -> WorkflowMetrics {
        let totalQueued = try await countExecutionsByStatus(.queued)
        let totalRunning = try await countExecutionsByStatus(.running)
        let totalCompleted = try await countExecutionsByStatus(.completed)
        let totalFailed = try await countExecutionsByStatus(.failed)
        let totalCancelled = try await countExecutionsByStatus(.cancelled)

        let totalExecutions = totalQueued + totalRunning + totalCompleted + totalFailed + totalCancelled
        let successRate = totalExecutions > 0 ? Double(totalCompleted) / Double(totalExecutions) : 0.0

        return WorkflowMetrics(
            totalExecutions: totalExecutions,
            pendingExecutions: totalQueued,
            runningExecutions: totalRunning,
            completedExecutions: totalCompleted,
            failedExecutions: totalFailed,
            cancelledExecutions: totalCancelled,
            successRate: successRate
        )
    }
}

// MARK: - Convenience Extensions with Default Parameters

extension WorkflowQueueDriver {
    /// Update execution status with default parameters
    public func updateExecutionStatus(
        _ workflowId: WorkflowID,
        status: WorkflowStatus,
        startTime: Date? = nil,
        endTime: Date? = nil,
        error: String? = nil
    ) async throws {
        try await updateExecutionStatus(workflowId, status: status, startTime: startTime, endTime: endTime, error: error)
    }
}

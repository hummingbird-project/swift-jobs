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

/// In-memory implementation of WorkflowRepository with comprehensive history tracking
/// This implementation is suitable for development and testing, but not for production
/// as all data is lost when the process restarts.
public final class MemoryWorkflowRepository: WorkflowRepository, Sendable {

    private let storage: Storage
    private let workflowStateRegistry: WorkflowRegistry

    public init(workflowStateRegistry: WorkflowRegistry) {
        self.storage = Storage()
        self.workflowStateRegistry = workflowStateRegistry
    }

    // MARK: - WorkflowDefinitionRepository

    public func registerWorkflow(_ definition: WorkflowDefinition) async throws {
        await storage.setWorkflowDefinition(definition.name, definition)
    }

    public func getWorkflow(name: String) async throws -> WorkflowDefinition? {
        await storage.getWorkflowDefinition(name)
    }

    public func listWorkflows() async throws -> [WorkflowDefinition] {
        await storage.listWorkflowDefinitions()
    }

    public func deleteWorkflow(name: String) async throws {
        await storage.deleteWorkflowDefinition(name)
    }

    // MARK: - WorkflowExecutionRepository

    public func createExecution(_ execution: WorkflowExecution) async throws {
        let buffer = try workflowStateRegistry.encode(execution)

        // Create execution summary from the execution
        let summary = WorkflowExecutionSummary(
            id: execution.id,
            workflowName: execution.workflowName,
            status: execution.status,
            createdAt: execution.createdAt,
            scheduledAt: execution.scheduledAt,
            startedAt: execution.startedAt,
            completedAt: execution.completedAt,
            currentStep: execution.currentStep,
            totalSteps: execution.totalSteps,
            error: execution.error,
            parentId: execution.parentId
        )

        await storage.setWorkflowExecution(execution.id, buffer, summary: summary)

        // Record creation event
        let event = WorkflowExecutionEvent(
            workflowId: execution.id,
            eventType: .created,
            message: "Workflow execution created",
            data: [
                "workflow_name": execution.workflowName,
                "status": execution.status.rawValue,
            ]
        )
        await storage.addWorkflowEvent(event)
    }

    public func getExecution(_ id: WorkflowID) async throws -> WorkflowExecution? {
        guard let buffer = await storage.getWorkflowExecution(id) else { return nil }
        return try workflowStateRegistry.decode(buffer, as: WorkflowExecution.self)
    }

    public func getWorkflowRun<Input: Codable & Sendable, Output: Codable & Sendable>(
        _ id: WorkflowID,
        inputType: Input.Type,
        outputType: Output.Type
    ) async throws -> WorkflowRun<Input, Output>? {
        // Get the storage execution first
        guard let execution = try await getExecution(id) else { return nil }

        // Decode input from ByteBuffer
        let input = try workflowStateRegistry.decode(execution.inputData, as: Input.self)

        // Decode output from ByteBuffer if it exists
        let output: Output?
        if let resultBuffer = execution.result {
            output = try workflowStateRegistry.decode(resultBuffer, as: Output.self)
        } else {
            output = nil
        }

        // Convert to typed WorkflowRun
        return WorkflowRun<Input, Output>(
            id: execution.id,
            workflowName: execution.workflowName,
            input: input,
            output: output,
            status: execution.status,
            createdAt: execution.createdAt,
            queuedAt: execution.queuedAt,
            startedAt: execution.startedAt,
            completedAt: execution.completedAt,
            currentStep: execution.currentStep,
            totalSteps: execution.totalSteps,
            error: execution.error,
            parentId: execution.parentId,
            parentStepRunId: execution.parentStepRunId,
            childKey: execution.childKey,
            childIndex: execution.childIndex,
            metadata: execution.metadata
        )
    }

    public func updateExecution(_ execution: WorkflowExecution) async throws {
        let buffer = try workflowStateRegistry.encode(execution)

        // Create execution summary from the execution
        let summary = WorkflowExecutionSummary(
            id: execution.id,
            workflowName: execution.workflowName,
            status: execution.status,
            createdAt: execution.createdAt,
            scheduledAt: execution.scheduledAt,
            startedAt: execution.startedAt,
            completedAt: execution.completedAt,
            currentStep: execution.currentStep,
            totalSteps: execution.totalSteps,
            error: execution.error,
            parentId: execution.parentId
        )

        await storage.setWorkflowExecution(execution.id, buffer, summary: summary)
    }

    public func deleteExecution(_ id: WorkflowID) async throws {
        await storage.deleteWorkflowExecution(id)
        await storage.deleteWorkflowSteps(id)
        await storage.deleteWorkflowEvents(id)
        await storage.deleteStepEvents(id)
    }

    public func listExecutions(_ options: ListWorkflowExecutionsOptions) async throws -> WorkflowExecutionList {
        let allSummaries = await storage.listWorkflowExecutionSummaries()

        // Apply filters
        var filtered = allSummaries

        if let workflowName = options.workflowName {
            filtered = filtered.filter { $0.workflowName == workflowName }
        }

        if let status = options.status {
            filtered = filtered.filter { $0.status == status }
        }

        if let createdAfter = options.createdAfter {
            filtered = filtered.filter { $0.createdAt >= createdAfter }
        }

        if let createdBefore = options.createdBefore {
            filtered = filtered.filter { $0.createdAt <= createdBefore }
        }

        // Apply sorting
        filtered.sort { lhs, rhs in
            let comparison: ComparisonResult
            switch options.orderBy {
            case .createdAt:
                comparison = lhs.createdAt.compare(rhs.createdAt)
            case .startedAt:
                let lhsStarted = lhs.startedAt ?? .distantPast
                let rhsStarted = rhs.startedAt ?? .distantPast
                comparison = lhsStarted.compare(rhsStarted)
            case .completedAt:
                let lhsCompleted = lhs.completedAt ?? .distantPast
                let rhsCompleted = rhs.completedAt ?? .distantPast
                comparison = lhsCompleted.compare(rhsCompleted)
            case .workflowName:
                comparison = lhs.workflowName.compare(rhs.workflowName)
            case .status:
                comparison = lhs.status.rawValue.compare(rhs.status.rawValue)
            }

            return options.orderDirection == .ascending ? comparison == .orderedAscending : comparison == .orderedDescending
        }

        let totalCount = filtered.count

        // Apply pagination
        if let offset = options.offset {
            filtered = Array(filtered.dropFirst(offset))
        }

        if let limit = options.limit {
            filtered = Array(filtered.prefix(limit))
        }

        return WorkflowExecutionList(executions: filtered, totalCount: totalCount)
    }

    public func countExecutionsByStatus(_ status: WorkflowStatus) async throws -> Int {
        await storage.countExecutionsByStatus(status)
    }

    // MARK: - WorkflowStepRepository

    public func saveStep(_ step: WorkflowStepExecution) async throws {
        let buffer = try workflowStateRegistry.encode(step)
        await storage.setWorkflowStep(step.workflowId, step.stepIndex, buffer)

        // Record step creation event
        let event = WorkflowStepEvent(
            workflowId: step.workflowId,
            stepIndex: step.stepIndex,
            stepName: step.stepName,
            eventType: .created,
            message: "Step execution created",
            data: [
                "step_type": step.stepType.rawValue,
                "status": step.status.rawValue,
            ],
            retryAttempt: step.retryAttempt,
            workerId: step.workerId
        )
        await storage.addStepEvent(event)
    }

    public func getStep(_ workflowId: WorkflowID, stepIndex: Int) async throws -> WorkflowStepExecution? {
        guard let buffer = await storage.getWorkflowStep(workflowId, stepIndex) else { return nil }
        return try workflowStateRegistry.decode(buffer, as: WorkflowStepExecution.self)
    }

    public func getSteps(_ workflowId: WorkflowID) async throws -> [WorkflowStepExecution] {
        let buffers = await storage.getWorkflowSteps(workflowId)
        return try buffers.map { try workflowStateRegistry.decode($0, as: WorkflowStepExecution.self) }
    }

    public func updateStep(_ step: WorkflowStepExecution) async throws {
        let buffer = try workflowStateRegistry.encode(step)
        await storage.setWorkflowStep(step.workflowId, step.stepIndex, buffer)

        // Record step status change event
        let eventType: WorkflowStepEventType
        let message: String
        let severity: WorkflowEventSeverity

        switch step.status {
        case .queued:
            eventType = .queued
            message = "Step queued for execution"
            severity = .info
        case .assigned:
            eventType = .assigned
            message = "Step assigned to worker"
            severity = .info
        case .running:
            eventType = .started
            message = "Step execution started"
            severity = .info
        case .succeeded:
            eventType = .completed
            message = "Step completed successfully"
            severity = .info
        case .failed:
            eventType = .failed
            message = "Step execution failed"
            severity = .error
        case .cancelled:
            eventType = .cancelled
            message = "Step execution cancelled"
            severity = .warning
        case .timedOut:
            eventType = .timedOut
            message = "Step execution timed out"
            severity = .error
        case .skipped:
            eventType = .skipped
            message = "Step execution skipped"
            severity = .info
        case .backoff:
            eventType = .retrying
            message = "Step retrying after failure"
            severity = .warning
        default:
            return  // Don't record events for intermediate states
        }

        let event = WorkflowStepEvent(
            workflowId: step.workflowId,
            stepIndex: step.stepIndex,
            stepName: step.stepName,
            eventType: eventType,
            severity: severity,
            message: message,
            data: [
                "step_type": step.stepType.rawValue,
                "status": step.status.rawValue,
            ],
            error: step.error,
            retryAttempt: step.retryAttempt,
            workerId: step.workerId
        )
        await storage.addStepEvent(event)
    }

    public func deleteSteps(_ workflowId: WorkflowID) async throws {
        await storage.deleteWorkflowSteps(workflowId)
    }

    // MARK: - ActivityResultRepository

    public func saveResult<Result: Codable & Sendable>(
        _ activityId: ActivityID,
        result: ActivityResult<Result>
    ) async throws {
        let buffer = try workflowStateRegistry.encode(result)
        await storage.setActivityResult(activityId, buffer)
    }

    public func getResult<Result: Codable & Sendable>(
        _ activityId: ActivityID,
        resultType: Result.Type
    ) async throws -> ActivityResult<Result>? {
        guard let buffer = await storage.getActivityResult(activityId) else { return nil }
        return try workflowStateRegistry.decode(buffer, as: ActivityResult<Result>.self)
    }

    public func deleteResult(_ activityId: ActivityID) async throws {
        await storage.deleteActivityResult(activityId)
    }

    public func cleanupResults(before: Date) async throws -> Int {
        await storage.cleanupActivityResults(before: before)
    }

    // MARK: - WorkflowHistoryRepository

    public func recordWorkflowEvent(_ event: WorkflowExecutionEvent) async throws {
        await storage.addWorkflowEvent(event)
    }

    public func recordStepEvent(_ event: WorkflowStepEvent) async throws {
        await storage.addStepEvent(event)
    }

    public func getWorkflowHistory(_ workflowId: WorkflowID, options: WorkflowHistoryOptions) async throws -> [WorkflowExecutionEvent] {
        let events = await storage.getWorkflowEvents(workflowId)
        return filterAndSortWorkflowEvents(events, options: options)
    }

    public func getStepHistory(_ workflowId: WorkflowID, stepIndex: Int?, options: WorkflowHistoryOptions) async throws -> [WorkflowStepEvent] {
        let events = await storage.getStepEvents(workflowId, stepIndex: stepIndex)
        return filterAndSortStepEvents(events, options: options)
    }

    public func archiveStepResult(_ workflowId: WorkflowID, stepIndex: Int, result: WorkflowStepExecution) async throws {
        let buffer = try workflowStateRegistry.encode(result)
        await storage.setArchivedStepResult(workflowId, stepIndex, buffer)
    }

    public func getArchivedStepResult(_ workflowId: WorkflowID, stepIndex: Int) async throws -> WorkflowStepExecution? {
        guard let buffer = await storage.getArchivedStepResult(workflowId, stepIndex) else { return nil }
        return try workflowStateRegistry.decode(buffer, as: WorkflowStepExecution.self)
    }

    public func cleanupHistoryEvents(before: Date) async throws -> Int {
        await storage.cleanupHistoryEvents(before: before)
    }

    public func cleanupArchivedResults(before: Date) async throws -> Int {
        await storage.cleanupArchivedResults(before: before)
    }

    // MARK: - Combined Repository Methods

    public func cleanupCompletedWorkflows(before: Date) async throws -> Int {
        await storage.cleanupCompletedWorkflows(before: before)
    }

    public func cleanupFailedWorkflows(before: Date) async throws -> Int {
        await storage.cleanupFailedWorkflows(before: before)
    }

    public func getExecutionSummary(_ workflowId: WorkflowID) async throws -> WorkflowExecutionSummary? {
        await storage.getExecutionSummary(workflowId)
    }

    public func updateExecutionStatus(
        _ workflowId: WorkflowID,
        status: WorkflowStatus,
        startTime: Date?,
        endTime: Date?,
        error: String?
    ) async throws {
        // Get previous status to determine appropriate event type
        let previousStatus = await storage.getExecutionStatus(workflowId)

        await storage.updateExecutionStatus(workflowId, status: status, startTime: startTime, endTime: endTime, error: error)

        // Record workflow status change event based on state transition
        let eventType: WorkflowExecutionEventType
        let message: String
        let severity: WorkflowEventSeverity

        switch (previousStatus, status) {
        case (_, .queued):
            eventType = .queued
            message = "Workflow queued for execution"
            severity = .info
        case (_, .running) where previousStatus == .sleeping:
            eventType = .resumed
            message = "Workflow resumed from sleep"
            severity = .info
        case (_, .running):
            eventType = .started
            message = "Workflow execution started"
            severity = .info
        case (_, .sleeping):
            eventType = .paused
            message = "Workflow entered sleep state"
            severity = .info
        case (_, .completed):
            eventType = .completed
            message = "Workflow execution completed successfully"
            severity = .info
        case (_, .failed):
            eventType = .failed
            message = "Workflow execution failed"
            severity = .error
        case (_, .cancelled):
            eventType = .cancelled
            message = "Workflow execution cancelled"
            severity = .warning

        }

        let event = WorkflowExecutionEvent(
            workflowId: workflowId,
            eventType: eventType,
            severity: severity,
            message: message,
            data: ["status": status.rawValue],
            error: error
        )
        await storage.addWorkflowEvent(event)
    }

    // MARK: - Private Helper Methods

    private func filterAndSortWorkflowEvents(_ events: [WorkflowExecutionEvent], options: WorkflowHistoryOptions) -> [WorkflowExecutionEvent] {
        var filtered = events

        if let eventTypes = options.eventTypes {
            filtered = filtered.filter { eventTypes.contains($0.eventType.rawValue) }
        }

        if let severities = options.severities {
            filtered = filtered.filter { severities.contains($0.severity) }
        }

        if let after = options.after {
            filtered = filtered.filter { $0.timestamp >= after }
        }

        if let before = options.before {
            filtered = filtered.filter { $0.timestamp <= before }
        }

        // Sort by timestamp (default descending)
        filtered.sort { lhs, rhs in
            options.orderDirection == .ascending ? lhs.timestamp < rhs.timestamp : lhs.timestamp > rhs.timestamp
        }

        // Apply pagination
        if let offset = options.offset {
            filtered = Array(filtered.dropFirst(offset))
        }

        if let limit = options.limit {
            filtered = Array(filtered.prefix(limit))
        }

        return filtered
    }

    private func filterAndSortStepEvents(_ events: [WorkflowStepEvent], options: WorkflowHistoryOptions) -> [WorkflowStepEvent] {
        var filtered = events

        if let eventTypes = options.eventTypes {
            filtered = filtered.filter { eventTypes.contains($0.eventType.rawValue) }
        }

        if let severities = options.severities {
            filtered = filtered.filter { severities.contains($0.severity) }
        }

        if let after = options.after {
            filtered = filtered.filter { $0.timestamp >= after }
        }

        if let before = options.before {
            filtered = filtered.filter { $0.timestamp <= before }
        }

        // Sort by timestamp (default descending)
        filtered.sort { lhs, rhs in
            options.orderDirection == .ascending ? lhs.timestamp < rhs.timestamp : lhs.timestamp > rhs.timestamp
        }

        // Apply pagination
        if let offset = options.offset {
            filtered = Array(filtered.dropFirst(offset))
        }

        if let limit = options.limit {
            filtered = Array(filtered.prefix(limit))
        }

        return filtered
    }
}

// MARK: - Internal Storage Actor

extension MemoryWorkflowRepository {
    private actor Storage {
        // Workflow definitions
        private var workflowDefinitions: [String: WorkflowDefinition] = [:]

        // Workflow executions (stored as encoded buffers)
        private var workflowExecutions: [WorkflowID: ByteBuffer] = [:]
        private var executionSummaries: [WorkflowID: WorkflowExecutionSummary] = [:]

        // Workflow steps (workflowId -> stepIndex -> buffer)
        private var workflowSteps: [WorkflowID: [Int: ByteBuffer]] = [:]

        // Activity results
        private var activityResults: [ActivityID: ByteBuffer] = [:]

        // History events
        private var workflowEvents: [WorkflowID: [WorkflowExecutionEvent]] = [:]
        private var stepEvents: [WorkflowID: [WorkflowStepEvent]] = [:]

        // Archived results
        private var archivedStepResults: [WorkflowID: [Int: ByteBuffer]] = [:]

        // MARK: - Workflow Definitions

        func setWorkflowDefinition(_ name: String, _ definition: WorkflowDefinition) {
            workflowDefinitions[name] = definition
        }

        func getWorkflowDefinition(_ name: String) -> WorkflowDefinition? {
            workflowDefinitions[name]
        }

        func listWorkflowDefinitions() -> [WorkflowDefinition] {
            Array(workflowDefinitions.values)
        }

        func deleteWorkflowDefinition(_ name: String) {
            workflowDefinitions.removeValue(forKey: name)
        }

        // MARK: - Workflow Executions

        func setWorkflowExecution(_ id: WorkflowID, _ buffer: ByteBuffer, summary: WorkflowExecutionSummary) {
            workflowExecutions[id] = buffer
            executionSummaries[id] = summary
        }

        func getWorkflowExecution(_ id: WorkflowID) -> ByteBuffer? {
            workflowExecutions[id]
        }

        func deleteWorkflowExecution(_ id: WorkflowID) {
            workflowExecutions.removeValue(forKey: id)
            executionSummaries.removeValue(forKey: id)
        }

        func listWorkflowExecutionSummaries() -> [WorkflowExecutionSummary] {
            Array(executionSummaries.values)
        }

        func countExecutionsByStatus(_ status: WorkflowStatus) -> Int {
            executionSummaries.values.filter { $0.status == status }.count
        }

        func getExecutionSummary(_ workflowId: WorkflowID) -> WorkflowExecutionSummary? {
            executionSummaries[workflowId]
        }

        func getExecutionStatus(_ workflowId: WorkflowID) -> WorkflowStatus? {
            executionSummaries[workflowId]?.status
        }

        func updateExecutionStatus(_ workflowId: WorkflowID, status: WorkflowStatus, startTime: Date?, endTime: Date?, error: String?) {
            if var summary = executionSummaries[workflowId] {
                summary.status = status
                if let startTime = startTime {
                    summary.startedAt = startTime
                }
                if let endTime = endTime {
                    summary.completedAt = endTime
                }
                if let error = error {
                    summary.error = error
                }
                executionSummaries[workflowId] = summary
            }
        }

        // MARK: - Workflow Steps

        func setWorkflowStep(_ workflowId: WorkflowID, _ stepIndex: Int, _ buffer: ByteBuffer) {
            if workflowSteps[workflowId] == nil {
                workflowSteps[workflowId] = [:]
            }
            workflowSteps[workflowId]?[stepIndex] = buffer
        }

        func getWorkflowStep(_ workflowId: WorkflowID, _ stepIndex: Int) -> ByteBuffer? {
            workflowSteps[workflowId]?[stepIndex]
        }

        func getWorkflowSteps(_ workflowId: WorkflowID) -> [ByteBuffer] {
            guard let steps = workflowSteps[workflowId] else { return [] }
            return steps.keys.sorted().compactMap { steps[$0] }
        }

        func deleteWorkflowSteps(_ workflowId: WorkflowID) {
            workflowSteps.removeValue(forKey: workflowId)
        }

        // MARK: - Activity Results

        func setActivityResult(_ activityId: ActivityID, _ buffer: ByteBuffer) {
            activityResults[activityId] = buffer
        }

        func getActivityResult(_ activityId: ActivityID) -> ByteBuffer? {
            activityResults[activityId]
        }

        func deleteActivityResult(_ activityId: ActivityID) {
            activityResults.removeValue(forKey: activityId)
        }

        func cleanupActivityResults(before: Date) -> Int {
            var cleanedCount = 0

            // Find activity results to delete by checking their associated workflow completion times
            let keysToDelete = activityResults.compactMap { activityId, _ in
                // Extract workflow ID from activity ID if possible
                // Activity IDs typically contain workflow information
                let activityIdString = activityId.value

                // Look for workflows that completed before the cutoff date
                for (workflowId, summary) in executionSummaries {
                    if activityIdString.contains(workflowId.value),
                        let completedAt = summary.completedAt,
                        completedAt < before
                    {
                        return activityId
                    }
                }
                return nil
            }

            // Remove the activity results
            for key in keysToDelete {
                activityResults.removeValue(forKey: key)
                cleanedCount += 1
            }

            return cleanedCount
        }

        // MARK: - History Events

        func addWorkflowEvent(_ event: WorkflowExecutionEvent) {
            if workflowEvents[event.workflowId] == nil {
                workflowEvents[event.workflowId] = []
            }
            workflowEvents[event.workflowId]?.append(event)
        }

        func addStepEvent(_ event: WorkflowStepEvent) {
            if stepEvents[event.workflowId] == nil {
                stepEvents[event.workflowId] = []
            }
            stepEvents[event.workflowId]?.append(event)
        }

        func getWorkflowEvents(_ workflowId: WorkflowID) -> [WorkflowExecutionEvent] {
            workflowEvents[workflowId] ?? []
        }

        func getStepEvents(_ workflowId: WorkflowID, stepIndex: Int?) -> [WorkflowStepEvent] {
            let events = stepEvents[workflowId] ?? []
            if let stepIndex = stepIndex {
                return events.filter { $0.stepIndex == stepIndex }
            }
            return events
        }

        func deleteWorkflowEvents(_ workflowId: WorkflowID) {
            workflowEvents.removeValue(forKey: workflowId)
        }

        func deleteStepEvents(_ workflowId: WorkflowID) {
            stepEvents.removeValue(forKey: workflowId)
        }

        func cleanupHistoryEvents(before: Date) -> Int {
            var deletedCount = 0

            for (workflowId, events) in workflowEvents {
                let filteredEvents = events.filter { $0.timestamp >= before }
                deletedCount += events.count - filteredEvents.count
                workflowEvents[workflowId] = filteredEvents.isEmpty ? nil : filteredEvents
            }

            for (workflowId, events) in stepEvents {
                let filteredEvents = events.filter { $0.timestamp >= before }
                deletedCount += events.count - filteredEvents.count
                stepEvents[workflowId] = filteredEvents.isEmpty ? nil : filteredEvents
            }

            return deletedCount
        }

        // MARK: - Archived Results

        func setArchivedStepResult(_ workflowId: WorkflowID, _ stepIndex: Int, _ buffer: ByteBuffer) {
            if archivedStepResults[workflowId] == nil {
                archivedStepResults[workflowId] = [:]
            }
            archivedStepResults[workflowId]?[stepIndex] = buffer
        }

        func getArchivedStepResult(_ workflowId: WorkflowID, _ stepIndex: Int) -> ByteBuffer? {
            archivedStepResults[workflowId]?[stepIndex]
        }

        func cleanupArchivedResults(before: Date) -> Int {
            var cleanedCount = 0

            let workflowsToDelete = archivedStepResults.compactMap { workflowId, _ in
                // Check if this workflow was completed/archived before the cutoff date
                if let summary = executionSummaries[workflowId],
                    let completedAt = summary.completedAt,
                    completedAt < before
                {
                    return workflowId
                }
                return nil
            }

            for workflowId in workflowsToDelete {
                if let stepResults = archivedStepResults.removeValue(forKey: workflowId) {
                    cleanedCount += stepResults.count
                }
            }

            return cleanedCount
        }

        // MARK: - Cleanup Methods

        func cleanupCompletedWorkflows(before: Date) -> Int {
            let completedWorkflows = executionSummaries.filter { _, summary in
                summary.status == .completed && (summary.completedAt ?? .distantPast) < before
            }

            for (workflowId, _) in completedWorkflows {
                deleteWorkflowExecution(workflowId)
                deleteWorkflowSteps(workflowId)
                deleteWorkflowEvents(workflowId)
                deleteStepEvents(workflowId)
            }

            return completedWorkflows.count
        }

        func cleanupFailedWorkflows(before: Date) -> Int {
            let failedWorkflows = executionSummaries.filter { _, summary in
                summary.status == .failed && (summary.completedAt ?? .distantPast) < before
            }

            for (workflowId, _) in failedWorkflows {
                deleteWorkflowExecution(workflowId)
                deleteWorkflowSteps(workflowId)
                deleteWorkflowEvents(workflowId)
                deleteStepEvents(workflowId)
            }

            return failedWorkflows.count
        }
    }
}

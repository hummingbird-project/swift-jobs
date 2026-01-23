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

import Collections
import Jobs
import NIOCore

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Memory-based workflow queue implementation
public final class MemoryWorkflowQueue: WorkflowQueueDriver, Sendable {
    public typealias Element = JobQueueResult<JobID>
    public typealias JobID = UUID

    /// Job options for workflow queue
    public struct JobOptions: JobOptionsProtocol {

        /// When to execute the job
        public let delayUntil: Date

        public init(delayUntil: Date = .now) {
            self.delayUntil = delayUntil
        }
    }

    // MARK: - Internal Storage

    let storage: Internal
    let jobRegistry: JobRegistry
    let workflowStateRegistry: WorkflowRegistry
    let onFailedJob: @Sendable (JobID, any Error) -> Void

    public init(onFailedJob: @escaping @Sendable (JobID, any Error) -> Void = { _, _ in }) {
        self.workflowStateRegistry = WorkflowRegistry()
        self.storage = Internal(workflowStateRegistry: self.workflowStateRegistry)
        self.jobRegistry = JobRegistry()
        self.onFailedJob = onFailedJob
    }

    // MARK: - JobQueueDriver Implementation

    public func registerJob<Parameters>(_ job: JobDefinition<Parameters>) {
        jobRegistry.registerJob(job)
    }

    public func registerCustomJobBuilder(
        name: String,
        builder: @escaping @Sendable (Decoder) throws -> any JobInstanceProtocol
    ) {
        // Override the default implementation to actually register the custom builder
        jobRegistry.builderTypeMap.withLock {
            $0[name] = builder
        }
    }

    @discardableResult
    public func push<Parameters>(_ jobRequest: JobRequest<Parameters>, options: JobOptions) async throws -> JobID {
        let buffer = try jobRegistry.encode(jobRequest: jobRequest)
        return try await storage.push(buffer, options: options)
    }

    public func retry<Parameters>(_ id: JobID, jobRequest: JobRequest<Parameters>, options: JobRetryOptions) async throws {
        let buffer = try jobRegistry.encode(jobRequest: jobRequest)
        let jobOptions = JobOptions(delayUntil: options.delayUntil)
        try await storage.retry(id, buffer: buffer, options: jobOptions)
    }

    public func pushWorkflowJob<Parameters: JobParameters>(_ job: Parameters) async throws -> JobID {
        let jobRequest = JobRequest(name: Parameters.jobName, parameters: job, queuedAt: .now, attempt: 1)
        return try await push(jobRequest, options: .init())
    }

    public func pushWorkflowJob<Parameters: JobParameters>(_ job: Parameters, delayUntil: Date) async throws -> JobID {
        let jobRequest = JobRequest(name: Parameters.jobName, parameters: job, queuedAt: .now, attempt: 1)
        return try await push(jobRequest, options: .init(delayUntil: delayUntil))
    }

    public func finished(jobID: JobID) async throws {
        await storage.clearPendingJob(jobID: jobID)
    }

    public func failed(jobID: JobID, error: any Error) async throws {
        if await storage.clearAndReturnPendingJob(jobID: jobID) != nil {
            onFailedJob(jobID, error)
        }
    }

    public func stop() async {
        await storage.stop()
    }

    public func shutdownGracefully() async {
        await storage.shutdown()
    }

    // MARK: - WorkflowQueueDriver Implementation

    public func initializeWorkflowStorage() async throws {
        await storage.initializeWorkflowIndices()
    }

    @discardableResult
    public func queueWorkflow<Input: Codable & Sendable>(
        _ workflowName: String,
        input: Input,
        options: JobOptions
    ) async throws -> WorkflowID {
        let workflowId = WorkflowID()

        // Create workflow execution record
        let inputBuffer = try workflowStateRegistry.encode(input)
        let execution = WorkflowExecution(
            id: workflowId,
            workflowName: workflowName,
            inputData: inputBuffer,
            workflowState: nil,
            status: .queued,
            createdAt: .now,
            queuedAt: .now,
            scheduledAt: nil,
            parentId: nil,
            parentStepRunId: nil,
            childKey: nil,
            childIndex: nil,
            metadata: [:]
        )

        try await createExecution(execution)

        // Queue workflow coordinator job
        let coordinatorJob = WorkflowCoordinatorJob(
            workflowId: workflowId,
            workflowType: workflowName,
            action: .start,
            stepIndex: 0
        )

        // Create JobRequest using the same pattern as MemoryJobQueue
        let jobRequest: JobRequest<WorkflowCoordinatorJob> = JobRequest(
            name: WorkflowCoordinatorJob.jobName,
            parameters: coordinatorJob,
            queuedAt: Date.now,
            attempt: 1
        )

        try await push(jobRequest, options: options)
        return workflowId
    }

    public func resumeWorkflow(_ workflowId: WorkflowID) async throws {
        await storage.resumeWorkflow(workflowId)
    }

    public func pauseWorkflow(_ workflowId: WorkflowID) async throws {
        await storage.pauseWorkflow(workflowId)
    }

    public func cancelWorkflow(_ workflowId: WorkflowID) async throws {
        await storage.cancelWorkflow(workflowId)
    }

    // MARK: - CancellableJobQueue Implementation

    public func cancel(jobID: JobID) async throws {
        await storage.cancelJob(jobID: jobID)
    }

    // MARK: - ResumableJobQueue Implementation

    public func pause(jobID: JobID) async throws {
        await storage.pauseJob(jobID: jobID)
    }

    public func resume(jobID: JobID) async throws {
        await storage.resumeJob(jobID: jobID)
    }

    // MARK: - WorkflowDefinitionRepository Implementation

    public func registerWorkflow(_ definition: WorkflowDefinition) async throws {
        try await storage.saveWorkflowDefinition(definition)
    }

    public func getWorkflow(name: String) async throws -> WorkflowDefinition? {
        await storage.getWorkflowDefinition(name: name)
    }

    public func listWorkflows() async throws -> [WorkflowDefinition] {
        await storage.listWorkflowDefinitions()
    }

    public func deleteWorkflow(name: String) async throws {
        await storage.deleteWorkflowDefinition(name: name)
    }

    // MARK: - WorkflowExecutionRepository Implementation

    public func createExecution(_ execution: WorkflowExecution) async throws {
        let buffer = try workflowStateRegistry.encode(execution)
        try await storage.saveWorkflowExecution(execution.id, buffer: buffer, status: execution.status)
    }

    public func getExecution(_ id: WorkflowID) async throws -> WorkflowExecution? {
        guard let buffer = await storage.getWorkflowExecution(id) else { return nil }
        var execution = try workflowStateRegistry.decode(buffer, as: WorkflowExecution.self)

        // Get the latest status from the status storage
        if let summary = try await storage.getExecutionSummary(id) {
            execution.status = summary.status
            execution.startedAt = summary.startedAt
            execution.completedAt = summary.completedAt
            execution.currentStep = summary.currentStep
            execution.totalSteps = summary.totalSteps
            execution.error = summary.error
        }

        return execution
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
        try await storage.updateWorkflowExecution(execution.id, buffer: buffer, status: execution.status)
    }

    public func deleteExecution(_ id: WorkflowID) async throws {
        await storage.deleteWorkflowExecution(id)
    }

    public func listExecutions(_ options: ListWorkflowExecutionsOptions) async throws -> WorkflowExecutionList {
        await storage.listWorkflowExecutions(options)
    }

    public func countExecutionsByStatus(_ status: WorkflowStatus) async throws -> Int {
        await storage.countExecutionsByStatus(status)
    }

    // MARK: - WorkflowStepRepository Implementation

    public func saveStep(_ step: WorkflowStepExecution) async throws {
        let buffer = try workflowStateRegistry.encode(step)
        let key = "\(step.workflowId.value):\(step.stepIndex)"
        await storage.saveWorkflowStep(key: key, buffer: buffer)
    }

    public func getStep(_ workflowId: WorkflowID, stepIndex: Int) async throws -> WorkflowStepExecution? {
        let key = "\(workflowId.value):\(stepIndex)"
        guard let buffer = await storage.getWorkflowStep(key: key) else { return nil }
        return try workflowStateRegistry.decode(buffer, as: WorkflowStepExecution.self)
    }

    public func getSteps(_ workflowId: WorkflowID) async throws -> [WorkflowStepExecution] {
        await storage.getWorkflowSteps(workflowId: workflowId)
    }

    public func updateStep(_ step: WorkflowStepExecution) async throws {
        let buffer = try workflowStateRegistry.encode(step)
        let key = "\(step.workflowId.value):\(step.stepIndex)"
        await storage.saveWorkflowStep(key: key, buffer: buffer)
    }

    public func deleteSteps(_ workflowId: WorkflowID) async throws {
        await storage.deleteWorkflowSteps(workflowId: workflowId)
    }

    // MARK: - ActivityResultRepository Implementation

    public func saveResult<Result: Codable & Sendable>(
        _ activityId: ActivityID,
        result: ActivityResult<Result>
    ) async throws {
        // Always encode the full ActivityResult to maintain consistent format
        let buffer = try workflowStateRegistry.encode(result)
        await storage.saveActivityResult(activityId: activityId.value, buffer: buffer)
    }

    public func getResult<Result: Codable & Sendable>(
        _ activityId: ActivityID,
        resultType: Result.Type
    ) async throws -> ActivityResult<Result>? {
        guard let buffer = await storage.getActivityResult(activityId: activityId.value) else { return nil }

        // Decode as ActivityResult<ByteBuffer> then convert to target type
        let bufferResult = try workflowStateRegistry.decode(buffer, as: ActivityResult<ByteBuffer>.self)
        switch bufferResult {
        case .success(let resultBuffer):
            // Decode the ByteBuffer to the expected result type
            let result = try workflowStateRegistry.decode(resultBuffer, as: Result.self)
            return .success(result)
        case .failure(let errorInfo):
            return .failure(errorInfo)
        }
    }

    public func deleteResult(_ activityId: ActivityID) async throws {
        await storage.deleteActivityResult(activityId: activityId.value)
    }

    public func cleanupResults(before: Date) async throws -> Int {
        await storage.cleanupActivityResults(before: before)
    }

    // MARK: - WorkflowRepository Implementation

    public func cleanupCompletedWorkflows(before: Date) async throws -> Int {
        await storage.cleanupWorkflows(status: .completed, before: before)
    }

    public func cleanupFailedWorkflows(before: Date) async throws -> Int {
        await storage.cleanupWorkflows(status: .failed, before: before)
    }

    // MARK: - Type-Erased Operations Implementation

    public func getExecutionSummary(_ workflowId: WorkflowID) async throws -> WorkflowExecutionSummary? {
        try await storage.getExecutionSummary(workflowId)
    }

    public func updateExecutionStatus(
        _ workflowId: WorkflowID,
        status: WorkflowStatus,
        startTime: Date?,
        endTime: Date?,
        error: String?
    ) async throws {
        await storage.updateExecutionStatus(workflowId, status: status, startTime: startTime, endTime: endTime, error: error)
    }

    // MARK: - WorkflowHistoryRepository Implementation

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

// MARK: - JobMetadataDriver Implementation

extension MemoryWorkflowQueue: JobMetadataDriver {
    public func getMetadata(_ key: String) async throws -> ByteBuffer? {
        await storage.getMetadata(key)
    }

    public func setMetadata(key: String, value: ByteBuffer) async throws {
        await storage.setMetadata(key: key, value: value)
    }

    public func acquireLock(key: String, id: ByteBuffer, expiresIn: TimeInterval) async throws -> Bool {
        await storage.acquireMetadataLock(key: key, id: id, expiresIn: expiresIn)
    }

    public func releaseLock(key: String, id: ByteBuffer) async throws {
        await storage.releaseMetadataLock(key: key, id: id)
    }
}

// MARK: - AsyncSequence Implementation

extension MemoryWorkflowQueue {
    public struct AsyncIterator: AsyncIteratorProtocol {
        fileprivate let storage: Internal
        fileprivate let jobRegistry: JobRegistry

        public mutating func next() async throws -> Element? {
            guard let queuedJob = try await self.storage.next() else { return nil }
            do {
                let jobInstance = try jobRegistry.decode(queuedJob.jobBuffer)
                return .init(id: queuedJob.id, result: .success(jobInstance))
            } catch let error as JobQueueError {
                return .init(id: queuedJob.id, result: .failure(error))
            }
        }
    }

    public func makeAsyncIterator() -> AsyncIterator {
        .init(storage: storage, jobRegistry: jobRegistry)
    }
}

// MARK: - Internal Storage Actor

extension MemoryWorkflowQueue {
    actor Internal {
        struct QueuedJob: Sendable {
            let id: JobID
            let jobBuffer: ByteBuffer
        }

        // Job queue storage
        var queue: Deque<(job: QueuedJob, options: JobOptions)>
        var pendingJobs: [JobID: ByteBuffer]
        var isStopped: Bool

        // Workflow-specific storage
        var workflowDefinitions: [String: WorkflowDefinition]
        var workflowExecutions: [WorkflowID: ByteBuffer]
        var workflowSteps: [String: ByteBuffer]
        var activityResults: [String: (buffer: ByteBuffer, timestamp: Date)]
        var metadata: [String: (data: ByteBuffer, expires: Date)]

        // Indices for efficient querying
        var executionsByStatus: [WorkflowStatus: Set<WorkflowID>]
        var stepsByWorkflow: [WorkflowID: Set<Int>]

        // History events
        var workflowEvents: [WorkflowID: [WorkflowExecutionEvent]]
        var stepEvents: [WorkflowID: [WorkflowStepEvent]]

        // Archived results
        var archivedStepResults: [WorkflowID: [Int: ByteBuffer]]

        // Registry for encoding/decoding workflow data
        let workflowStateRegistry: WorkflowRegistry

        init(workflowStateRegistry: WorkflowRegistry) {
            self.queue = .init()
            self.isStopped = false
            self.pendingJobs = .init()
            self.workflowDefinitions = .init()
            self.workflowExecutions = .init()

            self.workflowSteps = .init()
            self.activityResults = .init()
            self.metadata = .init()
            self.executionsByStatus = .init()
            self.stepsByWorkflow = .init()
            self.workflowEvents = .init()
            self.stepEvents = .init()
            self.archivedStepResults = .init()

            self.workflowStateRegistry = workflowStateRegistry

            // Initialize status indices
            for status in [WorkflowStatus.queued, .running, .sleeping, .completed, .failed, .cancelled] {
                self.executionsByStatus[status] = Set<WorkflowID>()
            }
        }

        // MARK: - Job Queue Operations

        func push(_ jobBuffer: ByteBuffer, options: JobOptions) throws -> JobID {
            let id = JobID()
            self.queue.append((job: QueuedJob(id: id, jobBuffer: jobBuffer), options: options))
            return id
        }

        func retry(_ id: JobID, buffer: ByteBuffer, options: JobOptions) throws {
            self.clearPendingJob(jobID: id)
            self.queue.append((job: QueuedJob(id: id, jobBuffer: buffer), options: options))
        }

        func clearPendingJob(jobID: JobID) {
            self.pendingJobs[jobID] = nil
        }

        func clearAndReturnPendingJob(jobID: JobID) -> ByteBuffer? {
            let instance = self.pendingJobs[jobID]
            self.pendingJobs[jobID] = nil
            return instance
        }

        func next() async throws -> QueuedJob? {
            var maxTimesToLoop = self.queue.count
            while true {
                if self.isStopped {
                    return nil
                }
                if let request = self.queue.popFirst() {
                    if request.options.delayUntil > Date.now {
                        self.queue.append(request)
                        maxTimesToLoop -= 1
                        if maxTimesToLoop > 0 {
                            continue
                        }
                    } else {
                        self.pendingJobs[request.job.id] = request.job.jobBuffer
                        return request.job
                    }
                }
                try await Task.sleep(for: .milliseconds(100))
            }
        }

        func stop() {
            self.isStopped = true
        }

        func shutdown() {
            assert(self.pendingJobs.count == 0)
            self.isStopped = true
        }

        // MARK: - Workflow-Specific Operations

        func initializeWorkflowIndices() {
            // Initialize indices if needed
        }

        func resumeWorkflow(_ workflowId: WorkflowID) {
            // Move from pending to running status if paused
            if let pausedIds = self.executionsByStatus[.queued], pausedIds.contains(workflowId) {
                self.executionsByStatus[.queued]?.remove(workflowId)
                self.executionsByStatus[.running]?.insert(workflowId)
            }
        }

        func pauseWorkflow(_ workflowId: WorkflowID) {
            // Move from running to pending status
            if let runningIds = self.executionsByStatus[.running], runningIds.contains(workflowId) {
                self.executionsByStatus[.running]?.remove(workflowId)
                self.executionsByStatus[.queued]?.insert(workflowId)
            }
        }

        func cancelWorkflow(_ workflowId: WorkflowID) {
            // Move to cancelled status and remove from job queue
            for status in self.executionsByStatus.keys {
                self.executionsByStatus[status]?.remove(workflowId)
            }
            self.executionsByStatus[.cancelled]?.insert(workflowId)

            // Also remove any pending jobs for this workflow
            self.queue.removeAll { queueItem in
                // Try to decode the job to check if it's a WorkflowCoordinatorJob for our workflow
                do {
                    let coordinatorJob = try JSONDecoder().decode(WorkflowCoordinatorJob.self, from: queueItem.job.jobBuffer)
                    return coordinatorJob.workflowId == workflowId
                } catch {
                    // If we can't decode as WorkflowCoordinatorJob, keep the job to be safe
                    return false
                }
            }
        }

        // MARK: - Job-level Operations

        func cancelJob(jobID: JobID) {
            // Remove job from queue
            self.queue.removeAll { queueItem in
                queueItem.job.id == jobID
            }
            // Remove from pending jobs if it exists
            self.pendingJobs.removeValue(forKey: jobID)
        }

        func pauseJob(jobID: JobID) {
            // Find the job in the queue and move it to pending jobs
            if let index = self.queue.firstIndex(where: { $0.job.id == jobID }) {
                let queuedItem = self.queue.remove(at: index)
                self.pendingJobs[jobID] = queuedItem.job.jobBuffer
            }
        }

        func resumeJob(jobID: JobID) {
            // Move job from pending back to the queue
            if let jobBuffer = self.pendingJobs[jobID] {
                let queuedJob = QueuedJob(id: jobID, jobBuffer: jobBuffer)
                self.queue.append((job: queuedJob, options: JobOptions()))
                self.pendingJobs.removeValue(forKey: jobID)
            }
        }

        // MARK: - Workflow Definition Operations

        func saveWorkflowDefinition(_ definition: WorkflowDefinition) throws {
            self.workflowDefinitions[definition.name] = definition
        }

        func getWorkflowDefinition(name: String) -> WorkflowDefinition? {
            self.workflowDefinitions[name]
        }

        func listWorkflowDefinitions() -> [WorkflowDefinition] {
            Array(self.workflowDefinitions.values)
        }

        func deleteWorkflowDefinition(name: String) {
            self.workflowDefinitions[name] = nil
        }

        // MARK: - Workflow Execution Operations

        func saveWorkflowExecution(_ id: WorkflowID, buffer: ByteBuffer, status: WorkflowStatus) throws {
            // Update indices
            if let _ = self.workflowExecutions[id] {
                // Remove from old status index if updating
                for (existingStatus, ids) in self.executionsByStatus {
                    if ids.contains(id) {
                        self.executionsByStatus[existingStatus]?.remove(id)
                        break
                    }
                }
            }

            self.workflowExecutions[id] = buffer
            self.executionsByStatus[status]?.insert(id)
        }

        func updateWorkflowExecution(_ id: WorkflowID, buffer: ByteBuffer, status: WorkflowStatus) throws {
            try self.saveWorkflowExecution(id, buffer: buffer, status: status)
        }

        func getWorkflowExecution(_ id: WorkflowID) -> ByteBuffer? {
            self.workflowExecutions[id]
        }

        func deleteWorkflowExecution(_ id: WorkflowID) {
            self.workflowExecutions[id] = nil
            for status in self.executionsByStatus.keys {
                self.executionsByStatus[status]?.remove(id)
            }
            self.stepsByWorkflow[id] = nil
        }

        func listWorkflowExecutions(_ options: ListWorkflowExecutionsOptions) -> WorkflowExecutionList {
            var allExecutions: [WorkflowExecutionSummary] = []

            // Get executions matching the filter criteria
            let executionsToCheck =
                if let status = options.status {
                    Array(self.executionsByStatus[status] ?? [])
                } else {
                    Array(self.workflowExecutions.keys)
                }

            for workflowId in executionsToCheck {
                guard let buffer = self.workflowExecutions[workflowId] else { continue }

                // Decode the actual WorkflowExecution to get complete information
                do {
                    let execution = try workflowStateRegistry.decode(buffer, as: WorkflowExecution.self)
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
                    allExecutions.append(summary)
                } catch {
                    // Skip corrupted executions
                    continue
                }
            }

            // Apply filtering
            if let workflowName = options.workflowName {
                allExecutions = allExecutions.filter { $0.workflowName == workflowName }
            }

            if let createdAfter = options.createdAfter {
                allExecutions = allExecutions.filter { $0.createdAt > createdAfter }
            }

            if let createdBefore = options.createdBefore {
                allExecutions = allExecutions.filter { $0.createdAt < createdBefore }
            }

            // Apply sorting
            switch options.orderBy {
            case .createdAt:
                allExecutions.sort {
                    options.orderDirection == .ascending ? $0.createdAt < $1.createdAt : $0.createdAt > $1.createdAt
                }
            case .workflowName:
                allExecutions.sort {
                    options.orderDirection == .ascending ? $0.workflowName < $1.workflowName : $0.workflowName > $1.workflowName
                }
            default:
                break
            }

            let totalCount = allExecutions.count

            // Apply pagination
            if let offset = options.offset {
                allExecutions = Array(allExecutions.dropFirst(offset))
            }

            if let limit = options.limit {
                allExecutions = Array(allExecutions.prefix(limit))
            }

            return WorkflowExecutionList(executions: allExecutions, totalCount: totalCount)
        }

        func countExecutionsByStatus(_ status: WorkflowStatus) -> Int {
            self.executionsByStatus[status]?.count ?? 0
        }

        // MARK: - Workflow Step Operations

        func saveWorkflowStep(key: String, buffer: ByteBuffer) {
            self.workflowSteps[key] = buffer

            // Update step index
            let components = key.split(separator: ":")
            if components.count == 2,
                let workflowIdStr = components.first,
                let stepIndexStr = components.last,
                let stepIndex = Int(stepIndexStr)
            {
                let workflowId = WorkflowID(String(workflowIdStr))

                if self.stepsByWorkflow[workflowId] == nil {
                    self.stepsByWorkflow[workflowId] = Set<Int>()
                }
                self.stepsByWorkflow[workflowId]?.insert(stepIndex)
            }
        }

        func getWorkflowStep(key: String) -> ByteBuffer? {
            self.workflowSteps[key]
        }

        func getWorkflowSteps(workflowId: WorkflowID) -> [WorkflowStepExecution] {
            guard let stepIndices = self.stepsByWorkflow[workflowId] else { return [] }

            var steps: [WorkflowStepExecution] = []
            for stepIndex in stepIndices.sorted() {
                let key = "\(workflowId.value):\(stepIndex)"
                if let buffer = self.workflowSteps[key] {
                    do {
                        // Use workflowStateRegistry to decode WorkflowStepExecution
                        let step: WorkflowStepExecution = try self.workflowStateRegistry.decode(buffer, as: WorkflowStepExecution.self)
                        steps.append(step)
                    } catch {
                        // Log error but continue with other steps
                        print("Failed to decode workflow step \(key): \(error)")
                    }
                }

            }
            return steps
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
            let workflowsToDelete = self.archivedStepResults.compactMap { workflowId, _ in
                // Check if this workflow was completed/archived before the cutoff date
                if let summary = try? getExecutionSummary(workflowId),
                    let completedAt = summary.completedAt,
                    completedAt < before
                {
                    return workflowId
                }
                return nil
            }

            for workflowId in workflowsToDelete {
                if let stepResults = self.archivedStepResults.removeValue(forKey: workflowId) {
                    cleanedCount += stepResults.count
                }
            }

            return cleanedCount
        }

        func deleteWorkflowSteps(workflowId: WorkflowID) {
            guard let stepIndices = self.stepsByWorkflow[workflowId] else { return }

            for stepIndex in stepIndices {
                let key = "\(workflowId.value):\(stepIndex)"
                self.workflowSteps[key] = nil
            }
            self.stepsByWorkflow[workflowId] = nil
        }

        // MARK: - Activity Result Operations

        func saveActivityResult(activityId: String, buffer: ByteBuffer) {
            self.activityResults[activityId] = (buffer: buffer, timestamp: .now)
        }

        func getActivityResult(activityId: String) -> ByteBuffer? {
            self.activityResults[activityId]?.buffer
        }

        func deleteActivityResult(activityId: String) {
            self.activityResults[activityId] = nil
        }

        func cleanupActivityResults(before: Date) -> Int {
            var cleanedCount = 0
            let keysToDelete = self.activityResults.compactMap { key, value in
                value.timestamp < before ? key : nil
            }

            for key in keysToDelete {
                self.activityResults[key] = nil
                cleanedCount += 1
            }

            return cleanedCount
        }

        // MARK: - Cleanup Operations

        func cleanupWorkflows(status: WorkflowStatus, before: Date) -> Int {
            guard let statusExecutions = self.executionsByStatus[status] else { return 0 }

            var deletedCount = 0
            let idsToCheck = Array(statusExecutions)

            for id in idsToCheck {
                guard let _ = self.workflowExecutions[id] else { continue }

                // For simplicity in memory implementation, delete based on status and assume old
                // In a real database implementation, we will have proper timestamp columns
                self.deleteWorkflowExecution(id)
                deletedCount += 1
            }

            return deletedCount
        }

        // MARK: - Metadata Operations

        func getMetadata(_ key: String) -> ByteBuffer? {
            self.metadata[key]?.data
        }

        func setMetadata(key: String, value: ByteBuffer) {
            self.metadata[key] = (data: value, expires: .distantFuture)
        }

        func acquireMetadataLock(key: String, id: ByteBuffer, expiresIn: TimeInterval) async -> Bool {
            guard let lock = self.metadata[key] else {
                self.metadata[key] = (data: id, expires: .now + expiresIn)
                return true
            }
            if lock.data == id {
                self.metadata[key]!.expires = .now + expiresIn
                return true
            } else if lock.expires < .now {
                self.metadata[key] = (data: id, expires: .now + expiresIn)
                return true
            } else {
                return false
            }
        }

        func releaseMetadataLock(key: String, id: ByteBuffer) async {
            if self.metadata[key]?.data == id {
                self.metadata[key] = nil
            }
        }

        // MARK: - Type-Erased Operations

        func getExecutionSummary(_ workflowId: WorkflowID) throws -> WorkflowExecutionSummary? {
            guard let buffer = self.workflowExecutions[workflowId] else { return nil }

            // Find the status for this workflow
            var currentStatus: WorkflowStatus = .queued
            for (status, ids) in self.executionsByStatus {
                if ids.contains(workflowId) {
                    currentStatus = status
                    break
                }
            }

            // Decode the full WorkflowExecution to get all fields including parentId
            // If decoding fails, this indicates data corruption and should propagate as an error
            let execution = try workflowStateRegistry.decode(buffer, as: WorkflowExecution.self)
            return WorkflowExecutionSummary(
                id: execution.id,
                workflowName: execution.workflowName,
                status: currentStatus,
                createdAt: execution.createdAt,
                scheduledAt: execution.scheduledAt,
                startedAt: execution.startedAt,
                completedAt: execution.completedAt,
                currentStep: execution.currentStep,
                totalSteps: execution.totalSteps,
                error: execution.error,
                parentId: execution.parentId
            )

        }

        func updateExecutionStatus(
            _ workflowId: WorkflowID,
            status: WorkflowStatus,
            startTime: Date?,
            endTime: Date?,
            error: String?
        ) {
            // Update the status index
            for existingStatus in self.executionsByStatus.keys {
                self.executionsByStatus[existingStatus]?.remove(workflowId)
            }
            self.executionsByStatus[status]?.insert(workflowId)

            // Also update the main workflow execution storage (type-erased)
            if let buffer = workflowExecutions[workflowId] {
                do {
                    var execution = try workflowStateRegistry.decode(buffer, as: WorkflowExecution.self)
                    execution.status = status
                    if let startTime = startTime {
                        execution.startedAt = startTime
                    }
                    if let endTime = endTime {
                        execution.completedAt = endTime
                    }
                    if let error = error {
                        execution.error = error
                    }
                    let updatedBuffer = try workflowStateRegistry.encode(execution)
                    workflowExecutions[workflowId] = updatedBuffer
                } catch {
                    // Log error but don't throw - we don't want to break status updates
                    // Error updating workflow execution status - this shouldn't break the workflow
                }
            }
        }
    }
}

// MARK: - Factory Methods

extension WorkflowQueueDriver where Self == MemoryWorkflowQueue {
    /// Return memory-based workflow queue driver
    /// - Parameters:
    ///   - onFailedJob: Closure called when a job fails
    public static func memory(onFailedJob: @escaping @Sendable (UUID, any Error) -> Void = { _, _ in }) -> MemoryWorkflowQueue {
        MemoryWorkflowQueue(onFailedJob: onFailedJob)
    }
}

extension JobQueueDriver where Self == MemoryWorkflowQueue {
    /// Return In memory driver for Job Queue
    /// - Parameters:
    ///   - onFailedJob: Closure called when a job fails
    public static var memoryWorkflow: MemoryWorkflowQueue {
        .init()
    }
}

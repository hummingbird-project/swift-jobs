//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2026 the Hummingbird authors
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

/// Unified middleware to handle both child and top-level workflow completion events
///
/// Detects when WorkflowCoordinator jobs complete and:
/// - For child workflows: notifies parent workflows via ChildWorkflowRegistry
/// - For top-level workflows: notifies runWorkflow calls via WorkflowCompletionRegistry
final class WorkflowCompletionMiddleware: JobMiddleware {

    private let repository: any WorkflowRepository

    init(repository: any WorkflowRepository) {
        self.repository = repository
    }

    func onCompletedJob(job: any JobInstanceProtocol, result: Result<Void, any Error>, context: JobCompletedQueueContext) async {
        // Handle WorkflowCoordinator completions
        guard let coordinatorJob = job.parameters as? WorkflowCoordinatorJob else {
            return
        }

        // Only handle completion actions (not start actions)
        guard coordinatorJob.action == .complete || coordinatorJob.action == .fail else {
            return
        }

        // Get workflow execution summary to determine if it's a child or top-level workflow
        guard let summary = try? await repository.getExecutionSummary(coordinatorJob.workflowId) else {
            return
        }

        if summary.parentId != nil {
            // This is a child workflow - notify parent via ChildWorkflowRegistry
            await handleChildWorkflowCompletion(coordinatorJob: coordinatorJob, result: result)
        } else {
            // This is a top-level workflow - notify runWorkflow calls via WorkflowCompletionRegistry
            await handleTopLevelWorkflowCompletion(coordinatorJob: coordinatorJob, result: result)
        }
    }

    private func handleChildWorkflowCompletion(coordinatorJob: WorkflowCoordinatorJob, result: Result<Void, any Error>) async {
        switch result {
        case .success:
            // Child workflow completed successfully
            await handleChildWorkflowSuccess(coordinatorJob: coordinatorJob)
        case .failure(let error):
            // Child workflow failed
            ChildWorkflowRegistry.shared.failWorkflow(childWorkflowId: coordinatorJob.workflowId, error: error)
        }
    }

    private func handleTopLevelWorkflowCompletion(coordinatorJob: WorkflowCoordinatorJob, result: Result<Void, any Error>) async {
        switch result {
        case .success:
            // Top-level workflow completed successfully
            await handleTopLevelWorkflowSuccess(coordinatorJob: coordinatorJob)
        case .failure(let error):
            // Top-level workflow failed
            WorkflowCompletionRegistry.shared.failWorkflow(workflowId: coordinatorJob.workflowId, error: error)
        }
    }

    private func handleChildWorkflowSuccess(coordinatorJob: WorkflowCoordinatorJob) async {
        do {
            // Retrieve the workflow output from the repository
            if let outputBuffer = try await getWorkflowOutput(coordinatorJob.workflowId) {
                ChildWorkflowRegistry.shared.completeWorkflow(
                    childWorkflowId: coordinatorJob.workflowId,
                    result: outputBuffer
                )
            } else {
                // No output found - complete with empty result
                let emptyResult = ByteBufferAllocator().buffer(capacity: 0)
                ChildWorkflowRegistry.shared.completeWorkflow(
                    childWorkflowId: coordinatorJob.workflowId,
                    result: emptyResult
                )
            }
        } catch {
            // Failed to retrieve output - treat as failure
            ChildWorkflowRegistry.shared.failWorkflow(
                childWorkflowId: coordinatorJob.workflowId,
                error: error
            )
        }
    }

    private func handleTopLevelWorkflowSuccess(coordinatorJob: WorkflowCoordinatorJob) async {
        do {
            // Retrieve the workflow output from the repository
            if let outputBuffer = try await getWorkflowOutput(coordinatorJob.workflowId) {
                WorkflowCompletionRegistry.shared.completeWorkflow(
                    workflowId: coordinatorJob.workflowId,
                    result: outputBuffer
                )
            } else {
                // No output found - complete with empty result
                let emptyResult = ByteBufferAllocator().buffer(capacity: 0)
                WorkflowCompletionRegistry.shared.completeWorkflow(
                    workflowId: coordinatorJob.workflowId,
                    result: emptyResult
                )
            }
        } catch {
            // Failed to retrieve output - treat as failure
            WorkflowCompletionRegistry.shared.failWorkflow(
                workflowId: coordinatorJob.workflowId,
                error: error
            )
        }
    }

    private func getWorkflowOutput(_ workflowId: WorkflowID) async throws -> ByteBuffer? {
        // Get execution summary to check status
        guard let summary = try await repository.getExecutionSummary(workflowId) else {
            return nil
        }

        // Only return output if workflow completed successfully
        guard summary.status == .completed else {
            return nil
        }

        // Get the full execution to access its result field
        guard let execution = try await repository.getExecution(workflowId) else {
            return nil
        }

        return execution.result
    }
}

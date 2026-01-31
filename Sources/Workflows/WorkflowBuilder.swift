//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

import Jobs

@resultBuilder
public struct WorkflowBuilder<WorkflowInput: Codable & Sendable> {
    /// First item with output
    public static func buildPartialBlock<Output: Codable & Sendable>(
        first job: WorkflowJob<WorkflowInput, Output>
    ) -> PartialWorkflow<Output> {
        PartialWorkflow(
            firstJobName: job.name
        ) { queue, workflowName, nextItem in
            queue.registerWorkflowJob(
                workflowName: workflowName,
                workflowJob: job,
                nextItem: nextItem
            )
        }
    }

    /// First item with no output
    public static func buildPartialBlock(
        first job: WorkflowJob<WorkflowInput, Void>
    ) -> PartialWorkflow<Void> {
        PartialWorkflow(
            firstJobName: job.name
        ) { queue, workflowName, nextItem in
            queue.registerFinalWorkflowJob(
                workflowName: workflowName,
                workflowJob: job
            )
        }
    }

    /// Item with previous and subsequents items
    public static func buildPartialBlock<Input: Codable & Sendable, Output: Codable & Sendable>(
        accumulated workflow: PartialWorkflow<Input>,
        next job2: WorkflowJob<Input, Output>
    ) -> PartialWorkflow<Output> {
        PartialWorkflow(
            firstJobName: workflow.firstJobName
        ) { queue, workflowName, nextItem in
            workflow.registerJobs(queue, workflowName, .job(named: job2.name, workflow: workflowName))
            queue.registerWorkflowJob(
                workflowName: workflowName,
                workflowJob: job2,
                nextItem: nextItem
            )
        }
    }

    /// Item with previous and no more after
    public static func buildPartialBlock<Input: Codable & Sendable>(
        accumulated workflow: PartialWorkflow<Input>,
        next job2: WorkflowJob<Input, Void>
    ) -> PartialWorkflow<Void> {
        PartialWorkflow(
            firstJobName: workflow.firstJobName
        ) { queue, workflowName, _ in
            workflow.registerJobs(queue, workflowName, .job(named: job2.name, workflow: workflowName))
            queue.registerFinalWorkflowJob(
                workflowName: workflowName,
                workflowJob: job2
            )
        }
    }

    /// Final result
    public static func buildFinalResult(
        _ workflow: PartialWorkflow<Void>
    ) -> Workflow<WorkflowInput> {
        Workflow(firstJobName: workflow.firstJobName) { queue, workflowName in
            workflow.registerJobs(queue, workflowName, .none)
        }
    }
}

@resultBuilder
public struct PartialWorkflowBuilder<WorkflowInput: Codable & Sendable, WorkflowOutput> {
    /// First item with output
    public static func buildPartialBlock<Output: Codable & Sendable>(
        first job: WorkflowJob<WorkflowInput, Output>
    ) -> PartialWorkflow<Output> {
        PartialWorkflow(
            firstJobName: job.name
        ) { queue, workflowName, nextItem in
            queue.registerWorkflowJob(
                workflowName: workflowName,
                workflowJob: job,
                nextItem: nextItem
            )
        }
    }

    /// First item without output
    public static func buildPartialBlock(
        first job: WorkflowJob<WorkflowInput, Void>
    ) -> PartialWorkflow<Void> {
        PartialWorkflow(
            firstJobName: job.name
        ) { queue, workflowName, nextItem in
            queue.registerFinalWorkflowJob(
                workflowName: workflowName,
                workflowJob: job
            )
        }
    }

    /// Item with previous and subsequents items
    public static func buildPartialBlock<Input: Codable & Sendable, Output: Codable & Sendable>(
        accumulated workflow: PartialWorkflow<Input>,
        next job2: WorkflowJob<Input, Output>
    ) -> PartialWorkflow<Output> {
        PartialWorkflow(
            firstJobName: workflow.firstJobName
        ) { queue, workflowName, nextItem in
            workflow.registerJobs(queue, workflowName, .job(named: job2.name, workflow: workflowName))
            queue.registerWorkflowJob(
                workflowName: workflowName,
                workflowJob: job2,
                nextItem: nextItem
            )
        }
    }

    // Final result
    public static func buildFinalResult(
        _ workflow: PartialWorkflow<WorkflowOutput>
    ) -> PartialWorkflow<WorkflowOutput> {
        workflow
    }
}

/// Partially constructed Workflow
public struct PartialWorkflow<Output> {
    /// name of first job in workflow
    let firstJobName: String
    /// function to call to register all the workflow jobs
    let registerJobs: (any JobQueueProtocol, String, WorkflowNextJob<Output>) -> Void

    init(
        firstJobName: String,
        registerJobs: @escaping (any JobQueueProtocol, String, WorkflowNextJob<Output>) -> Void
    ) {
        self.firstJobName = firstJobName
        self.registerJobs = registerJobs
    }
}

/// Fully constructed workflow
public struct Workflow<WorkflowInput: Codable & Sendable> {
    /// name of first job to trigger workflow
    let firstJobName: String
    /// function to call to register all the workflow jobs
    let registerJobs: (any JobQueueProtocol, String) -> Void
}

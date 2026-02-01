//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

import Jobs

/*
@resultBuilder
public struct WorkflowBuilder<WorkflowInput: Codable & Sendable> {
    /// First item with output
    public static func buildPartialBlock<Output: Codable & Sendable>(
        first job: WorkflowJob<WorkflowInput, Output>
    ) -> Workflow<WorkflowInput, Output> {
        Workflow(
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
    ) -> Workflow<WorkflowInput, Void> {
        Workflow(
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
        accumulated workflow: Workflow<WorkflowInput, Input>,
        next job2: WorkflowJob<Input, Output>
    ) -> Workflow<WorkflowInput, Output> {
        Workflow(
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
        accumulated workflow: Workflow<WorkflowInput, Input>,
        next job2: WorkflowJob<Input, Void>
    ) -> Workflow<WorkflowInput, Void> {
        Workflow(
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
        _ workflow: Workflow<WorkflowInput, Void>
    ) -> Workflow<WorkflowInput, Void> {
        Workflow(firstJobName: workflow.firstJobName) { queue, workflowName, nextItem in
            workflow.registerJobs(queue, workflowName, nextItem)
        }
    }
}
*/
@resultBuilder
public struct WorkflowBuilder<WorkflowInput: Codable & Sendable, WorkflowOutput> {
    /// First item with output
    public static func buildPartialBlock<Output: Codable & Sendable>(
        first job: WorkflowJob<WorkflowInput, Output>
    ) -> Workflow<WorkflowInput, Output> {
        Workflow(
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
    ) -> Workflow<WorkflowInput, Void> {
        Workflow(
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
        accumulated workflow: Workflow<WorkflowInput, Input>,
        next job: WorkflowJob<Input, Output>
    ) -> Workflow<WorkflowInput, Output> {
        workflow.addStep(job)
    }

    /// Item with previous and no more after
    public static func buildPartialBlock<Input: Codable & Sendable>(
        accumulated workflow: Workflow<WorkflowInput, Input>,
        next job: WorkflowJob<Input, Void>
    ) -> Workflow<WorkflowInput, Void> {
        workflow.addStep(job)
    }

    // Final result
    public static func buildFinalResult(
        _ workflow: Workflow<WorkflowInput, WorkflowOutput>
    ) -> Workflow<WorkflowInput, WorkflowOutput> {
        workflow
    }
}

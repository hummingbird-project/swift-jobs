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
        let workflowName = WorkflowBuilderState.current?.name ?? "workflow"
        return PartialWorkflow(
            name: WorkflowBuilderState.current?.name ?? "workflow",
            firstJobName: job.name
        ) { queue, nextItem in
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
        let workflowName = WorkflowBuilderState.current?.name ?? "workflow"
        return PartialWorkflow(
            name: WorkflowBuilderState.current?.name ?? "workflow",
            firstJobName: job.name
        ) { queue, nextItem in
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
            name: workflow.name,
            firstJobName: workflow.firstJobName
        ) { queue, nextItem in
            workflow.registerJobs(queue, .job(named: job2.name))
            queue.registerWorkflowJob(
                workflowName: workflow.name,
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
            name: workflow.name,
            firstJobName: workflow.firstJobName
        ) { queue, _ in
            workflow.registerJobs(queue, .job(named: job2.name))
            queue.registerFinalWorkflowJob(
                workflowName: workflow.name,
                workflowJob: job2
            )
        }
    }

    /// Final result
    public static func buildFinalResult(
        _ workflow: PartialWorkflow<Void>
    ) -> Workflow<WorkflowInput> {
        Workflow(workflowName: .init("\(workflow.name).\(workflow.firstJobName)")) { queue in
            workflow.registerJobs(queue, .none)
        }
    }
}

@resultBuilder
public struct PartialWorkflowBuilder<WorkflowInput: Codable & Sendable, WorkflowOutput> {
    /// First item with output
    public static func buildPartialBlock<Output: Codable & Sendable>(
        first job: WorkflowJob<WorkflowInput, Output>
    ) -> PartialWorkflow<Output> {
        let workflowName = WorkflowBuilderState.current?.name ?? "workflow"
        return PartialWorkflow(
            name: WorkflowBuilderState.current?.name ?? "workflow",
            firstJobName: job.name
        ) { queue, nextItem in
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
        let workflowName = WorkflowBuilderState.current?.name ?? "workflow"
        return PartialWorkflow(
            name: WorkflowBuilderState.current?.name ?? "workflow",
            firstJobName: job.name
        ) { queue, nextItem in
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
            name: workflow.name,
            firstJobName: workflow.firstJobName
        ) { queue, nextItem in
            workflow.registerJobs(queue, .job(named: job2.name))
            queue.registerWorkflowJob(
                workflowName: workflow.name,
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

/// Workflow builder state used when building Workflows.
///
/// Currently used to propagate workflow name down to workflow jobs
internal struct WorkflowBuilderState {
    @TaskLocal static var current: WorkflowBuilderState?
    var name: String
}

/// Partially constructed Workflow
public struct PartialWorkflow<Output> {
    /// workflow name
    let name: String
    /// name of first job in workflow
    let firstJobName: String
    /// function to call to register all the workflow jobs
    let registerJobs: (any JobQueueProtocol, WorkflowNextJob<Output>) -> Void

    init(
        name: String,
        firstJobName: String,
        registerJobs: @escaping (any JobQueueProtocol, WorkflowNextJob<Output>) -> Void
    ) {
        self.name = name
        self.firstJobName = firstJobName
        self.registerJobs = registerJobs
    }
}

/// Fully constructed workflow
public struct Workflow<WorkflowInput: Codable & Sendable> {
    /// name of workflow
    let workflowName: WorkflowName<WorkflowInput>
    /// function to call to register all the workflow jobs
    let registerJobs: (any JobQueueProtocol) -> Void
}

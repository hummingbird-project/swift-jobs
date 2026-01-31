//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//
public struct ChildWorkflow<Input: Codable & Sendable, Output> {
    let workflow: PartialWorkflow<Output>

    init(
        input: Input.Type = Input.self,
        output: Output.Type = Output.self,
        workflow: PartialWorkflow<Output>
    ) {
        self.workflow = workflow
    }
}

extension WorkflowBuilder {
    /// First item
    public static func buildPartialBlock<Output>(
        first workflow: ChildWorkflow<WorkflowInput, Output>
    ) -> PartialWorkflow<Output> {
        workflow.workflow
    }

    /// Item with previous and subsequents items
    public static func buildPartialBlock<Input: Codable & Sendable, Output>(
        accumulated workflow: PartialWorkflow<Input>,
        next childWorkflow: ChildWorkflow<Input, Output>
    ) -> PartialWorkflow<Output> {
        PartialWorkflow(
            firstJobName: workflow.firstJobName
        ) { queue, workflowName, nextItem in
            workflow.registerJobs(queue, workflowName, .job(named: childWorkflow.workflow.firstJobName, workflow: workflowName))
            childWorkflow.workflow.registerJobs(queue, workflowName, nextItem)
        }
    }
}

extension PartialWorkflowBuilder {
    /// First item
    public static func buildPartialBlock<Output>(
        first workflow: ChildWorkflow<WorkflowInput, Output>
    ) -> PartialWorkflow<Output> {
        workflow.workflow
    }

    /// Item with previous and subsequents items
    public static func buildPartialBlock<Input: Codable & Sendable, Output>(
        accumulated workflow: PartialWorkflow<Input>,
        next childWorkflow: ChildWorkflow<Input, Output>
    ) -> PartialWorkflow<Output> {
        PartialWorkflow(
            firstJobName: workflow.firstJobName
        ) { queue, workflowName, nextItem in
            workflow.registerJobs(queue, workflowName, .job(named: childWorkflow.workflow.firstJobName, workflow: workflowName))
            childWorkflow.workflow.registerJobs(queue, workflowName, nextItem)
        }
    }
}

//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

public struct IfThen<Input: Codable & Sendable, Output> {
    let condition: @Sendable (Input) async throws -> Bool
    let thenWorkflow: Workflow<Input, Output>

    public init(
        output: Output.Type,
        if condition: @Sendable @escaping (Input) async throws -> Bool,
        @WorkflowBuilder<Input, Output> then thenWorkflow: () -> Workflow<Input, Output>,
    ) {
        self.condition = condition
        self.thenWorkflow = thenWorkflow()
    }
}

extension WorkflowBuilder {
    /// If then where then returns a value that will be passed to next block
    public static func buildPartialBlock<Input>(
        accumulated workflow: Workflow<WorkflowInput, Input>,
        next condition: IfThen<Input, Input>
    ) -> Workflow<WorkflowInput, Input> {
        Workflow(
            firstJobName: workflow.firstJobName
        ) { queue, workflowName, nextItem in
            workflow.registerJobs(
                queue,
                workflowName,
                .ifelse(
                    ifName: .job(named: condition.thenWorkflow.firstJobName, workflow: workflowName),
                    elseName: nextItem,
                    condition: condition.condition
                )
            )
            condition.thenWorkflow.registerJobs(queue, workflowName, nextItem)
        }
    }

    /// If then where then finishes workflow
    public static func buildPartialBlock<Input, Void>(
        accumulated workflow: Workflow<WorkflowInput, Input>,
        next condition: IfThen<Input, Void>
    ) -> Workflow<WorkflowInput, Input> {
        Workflow(
            firstJobName: workflow.firstJobName
        ) { queue, workflowName, nextItem in
            workflow.registerJobs(
                queue,
                workflowName,
                .ifelse(
                    ifName: .job(named: condition.thenWorkflow.firstJobName, workflow: workflowName),
                    elseName: nextItem,
                    condition: condition.condition
                )
            )
            condition.thenWorkflow.registerJobs(queue, workflowName, .none)
        }
    }
}

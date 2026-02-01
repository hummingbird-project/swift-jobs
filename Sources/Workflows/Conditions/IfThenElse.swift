//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

public struct IfThenElse<Input: Codable & Sendable, Output> {
    let condition: @Sendable (Input) async throws -> Bool
    let thenWorkflow: Workflow<Input, Output>
    let elseWorkflow: Workflow<Input, Output>

    public init(
        output: Output.Type,
        if condition: @Sendable @escaping (Input) async throws -> Bool,
        @WorkflowResultBuilder<Input, Output> then thenWorkflow: () -> Workflow<Input, Output>,
        @WorkflowResultBuilder<Input, Output> else elseWorkflow: () -> Workflow<Input, Output>
    ) {
        self.condition = condition
        self.thenWorkflow = thenWorkflow()
        self.elseWorkflow = elseWorkflow()
    }
}

extension WorkflowResultBuilder {
    /// If else with previous and subsequent items
    public static func buildPartialBlock<Input: Codable & Sendable, Output: Codable & Sendable>(
        accumulated workflow: Workflow<WorkflowInput, Input>,
        next condition: IfThenElse<Input, Output>
    ) -> Workflow<WorkflowInput, Output> {
        Workflow(
            firstJobName: workflow.firstJobName
        ) { queue, workflowName, nextItem in
            workflow.registerJobs(
                queue,
                workflowName,
                .ifelse(
                    ifName: .job(named: condition.thenWorkflow.firstJobName, workflow: workflowName),
                    elseName: .job(named: condition.elseWorkflow.firstJobName, workflow: workflowName),
                    condition: condition.condition
                )
            )
            condition.thenWorkflow.registerJobs(queue, workflowName, nextItem)
            condition.elseWorkflow.registerJobs(queue, workflowName, nextItem)
        }
    }

    /// If else that is last item in list
    public static func buildPartialBlock<Input: Codable & Sendable>(
        accumulated workflow: Workflow<WorkflowInput, Input>,
        next condition: IfThenElse<Input, Void>
    ) -> Workflow<WorkflowInput, Void> {
        Workflow(
            firstJobName: workflow.firstJobName
        ) { queue, workflowName, nextItem in
            workflow.registerJobs(
                queue,
                workflowName,
                .ifelse(
                    ifName: .job(named: condition.thenWorkflow.firstJobName, workflow: workflowName),
                    elseName: .job(named: condition.elseWorkflow.firstJobName, workflow: workflowName),
                    condition: condition.condition
                )
            )
            condition.thenWorkflow.registerJobs(queue, workflowName, nextItem)
            condition.elseWorkflow.registerJobs(queue, workflowName, nextItem)
        }
    }
}

//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

public struct IfThenElse<Input: Codable & Sendable, Output> {
    let condition: @Sendable (Input) async throws -> Bool
    let thenWorkflow: PartialWorkflow<Output>
    let elseWorkflow: PartialWorkflow<Output>

    public init(
        output: Output.Type,
        if condition: @Sendable @escaping (Input) async throws -> Bool,
        @PartialWorkflowBuilder<Input, Output> then thenWorkflow: () -> PartialWorkflow<Output>,
        @PartialWorkflowBuilder<Input, Output> else elseWorkflow: () -> PartialWorkflow<Output>
    ) {
        self.condition = condition
        self.thenWorkflow = thenWorkflow()
        self.elseWorkflow = elseWorkflow()
    }
}

extension WorkflowBuilder {
    /// If else with previous and subsequent items
    public static func buildPartialBlock<Input: Codable & Sendable, Output: Codable & Sendable>(
        accumulated workflow: PartialWorkflow<Input>,
        next condition: IfThenElse<Input, Output>
    ) -> PartialWorkflow<Output> {
        PartialWorkflow(
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
        accumulated workflow: PartialWorkflow<Input>,
        next condition: IfThenElse<Input, Void>
    ) -> PartialWorkflow<Void> {
        PartialWorkflow(
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

extension PartialWorkflowBuilder {
    /// If else with previous and subsequent items
    public static func buildPartialBlock<Input: Codable & Sendable, Output: Codable & Sendable>(
        accumulated workflow: PartialWorkflow<Input>,
        next condition: IfThenElse<Input, Output>
    ) -> PartialWorkflow<Output> {
        PartialWorkflow(
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
        accumulated workflow: PartialWorkflow<Input>,
        next condition: IfThenElse<Input, Void>
    ) -> PartialWorkflow<Void> {
        PartialWorkflow(
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

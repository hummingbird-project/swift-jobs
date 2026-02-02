//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

public struct IfThen<Input: Codable & Sendable, Output: Codable & Sendable> {
    let condition: @Sendable (Input) async throws -> Bool
    let thenWorkflow: Workflow<Input, Output>

    public init(
        output: Output.Type = Output.self,
        if condition: @Sendable @escaping (Input) async throws -> Bool,
        @WorkflowResultBuilder<Input, Output> then thenWorkflow: () -> Workflow<Input, Output>
    ) {
        self.condition = condition
        self.thenWorkflow = thenWorkflow()
    }
}

public struct Guard<Input: Codable & Sendable> {
    let condition: @Sendable (Input) async throws -> Bool
    let elseWorkflow: Workflow<Input, EmptyOutput>

    public init(
        guard condition: @Sendable @escaping (Input) async throws -> Bool,
        @WorkflowResultBuilder<Input, EmptyOutput> else elseWorkflow: () -> Workflow<Input, EmptyOutput>
    ) {
        self.condition = { try await !condition($0) }
        self.elseWorkflow = elseWorkflow()
    }
}

extension WorkflowResultBuilder {
    /// If then where then returns a value that will be passed to next block
    public static func buildPartialBlock<Input>(
        accumulated workflow: Workflow<WorkflowInput, Input>,
        next condition: IfThen<Input, Input>
    ) -> Workflow<WorkflowInput, Input> {
        Workflow(
            firstJobName: workflow.firstJobName
        ) { queue, workflowName, nextStep in
            workflow.registerJobs(
                queue,
                workflowName,
                .ifelse(
                    ifName: .job(named: condition.thenWorkflow.firstJobName, workflow: workflowName),
                    elseName: nextStep,
                    condition: condition.condition
                )
            )
            condition.thenWorkflow.registerJobs(queue, workflowName, nextStep)
        }
    }

    public static func buildPartialBlock<Input>(
        accumulated workflow: Workflow<WorkflowInput, Input>,
        next condition: Guard<Input>
    ) -> Workflow<WorkflowInput, Input> {
        Workflow(
            firstJobName: workflow.firstJobName
        ) { queue, workflowName, nextStep in
            workflow.registerJobs(
                queue,
                workflowName,
                .ifelse(
                    ifName: .job(named: condition.elseWorkflow.firstJobName, workflow: workflowName),
                    elseName: nextStep,
                    condition: condition.condition
                )
            )
            condition.elseWorkflow.registerJobs(queue, workflowName, .none)
        }
    }
}

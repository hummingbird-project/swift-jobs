//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

import Jobs

/// Workflow
public struct Workflow<Input: Codable & Sendable, Output> {
    /// name of first job to trigger workflow
    let firstJobName: String
    /// function to call to register all the workflow jobs
    let registerJobs: (any JobQueueProtocol, String, WorkflowNextStep<Output>) -> Void
}

extension Workflow where Output: Codable & Sendable {
    ///  Return new workflow with step added
    /// - Parameter step: Workflow step to add
    /// - Returns: New workflow with step added
    public func addStep<Output2: Codable & Sendable>(_ step: WorkflowStep<Output, Output2>) -> Workflow<Input, Output2> {
        Workflow<Input, Output2>(
            firstJobName: self.firstJobName
        ) { queue, workflowName, nextItem in
            self.registerJobs(queue, workflowName, .job(named: step.name, workflow: workflowName))
            queue.registerWorkflowJob(
                workflowName: workflowName,
                workflowStep: step,
                nextItem: nextItem
            )
        }
    }

    ///  Return new workflow with step added
    /// - Parameter step: Workflow step to add
    /// - Returns: New workflow with step added
    public func addStep(_ step: WorkflowStep<Output, Void>) -> Workflow<Input, Void> {
        Workflow<Input, Void>(
            firstJobName: self.firstJobName
        ) { queue, workflowName, _ in
            self.registerJobs(queue, workflowName, .job(named: step.name, workflow: workflowName))
            queue.registerFinalWorkflowJob(
                workflowName: workflowName,
                workflowJob: step
            )
        }
    }

    ///  Return new workflow with child workflow added
    /// - Parameter step: Child workflow to add
    /// - Returns: New workflow with child workflow added
    public func addChildWorkflow<Output2>(_ childWorkflow: Workflow<Output, Output2>) -> Workflow<Input, Output2> {
        Workflow<Input, Output2>(
            firstJobName: self.firstJobName
        ) { queue, workflowName, nextItem in
            self.registerJobs(queue, workflowName, .job(named: childWorkflow.firstJobName, workflow: workflowName))
            childWorkflow.registerJobs(queue, workflowName, nextItem)
        }
    }

    public func `if`<Output2>(
        _ condition: @Sendable @escaping (Output) throws -> Bool,
        then thenWorkflowBuilder: (WorkflowBuilder<Output>) -> Workflow<Output, Output2>,
        else elseWorkflowBuilder: (WorkflowBuilder<Output>) -> Workflow<Output, Output2>
    ) -> Workflow<Input, Output2> {
        let thenWorkflow = thenWorkflowBuilder(WorkflowBuilder())
        let elseWorkflow = elseWorkflowBuilder(WorkflowBuilder())
        return Workflow<Input, Output2>(
            firstJobName: self.firstJobName
        ) { queue, workflowName, nextItem in
            self.registerJobs(
                queue,
                workflowName,
                .ifelse(
                    ifName: .job(named: thenWorkflow.firstJobName, workflow: workflowName),
                    elseName: .job(named: elseWorkflow.firstJobName, workflow: workflowName),
                    condition: condition
                )
            )
            thenWorkflow.registerJobs(queue, workflowName, nextItem)
            elseWorkflow.registerJobs(queue, workflowName, nextItem)
        }
    }

    public func `if`(
        _ condition: @Sendable @escaping (Output) throws -> Bool,
        then thenWorkflowBuilder: (WorkflowBuilder<Output>) -> Workflow<Output, Output>
    ) -> Workflow<Input, Output> {
        let thenWorkflow = thenWorkflowBuilder(WorkflowBuilder())
        return Workflow(
            firstJobName: self.firstJobName
        ) { queue, workflowName, nextItem in
            self.registerJobs(
                queue,
                workflowName,
                .ifelse(
                    ifName: .job(named: thenWorkflow.firstJobName, workflow: workflowName),
                    elseName: nextItem,
                    condition: condition
                )
            )
            thenWorkflow.registerJobs(queue, workflowName, nextItem)
        }
    }

    public func `if`(
        _ condition: @Sendable @escaping (Output) throws -> Bool,
        then thenWorkflowBuilder: (WorkflowBuilder<Output>) -> Workflow<Output, Void>
    ) -> Workflow<Input, Output> {
        let thenWorkflow = thenWorkflowBuilder(WorkflowBuilder())
        return Workflow(
            firstJobName: self.firstJobName
        ) { queue, workflowName, nextItem in
            self.registerJobs(
                queue,
                workflowName,
                .ifelse(
                    ifName: .job(named: thenWorkflow.firstJobName, workflow: workflowName),
                    elseName: nextItem,
                    condition: condition
                )
            )
            thenWorkflow.registerJobs(queue, workflowName, .none)
        }
    }
}

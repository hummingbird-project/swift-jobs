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
        ) { queue, workflowName, nextStep in
            self.registerJobs(queue, workflowName, .job(named: step.name, workflow: workflowName))
            queue.registerWorkflowJob(
                workflowName: workflowName,
                workflowStep: step,
                nextStep: nextStep
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

    ///  Create workflow and add a single step to the workflow
    /// - Parameter step: Workflow step to add
    /// - Returns: New workflow with step added
    public func addStep<Output2: Codable & Sendable>(
        name: String,
        retryStrategy: any JobRetryStrategy = .dontRetry,
        timeout: Duration? = nil,
        operation: @escaping @Sendable (Output, WorkflowExecutionContext) async throws -> Output2
    ) -> Workflow<Input, Output2> {
        Workflow<Input, Output2>(
            firstJobName: self.firstJobName
        ) { queue, workflowName, nextStep in
            self.registerJobs(queue, workflowName, .job(named: name, workflow: workflowName))
            queue.registerWorkflowJob(
                workflowName: workflowName,
                workflowStep: .init(name: name, parameters: Output.self, retryStrategy: retryStrategy, timeout: timeout, execute: operation),
                nextStep: nextStep
            )
        }
    }

    ///  Create workflow and add a single step to the workflow
    /// - Parameter step: Workflow step to add
    /// - Returns: New workflow with step added
    public func addStep(
        name: String,
        retryStrategy: any JobRetryStrategy = .dontRetry,
        timeout: Duration? = nil,
        operation: @escaping @Sendable (Output, WorkflowExecutionContext) async throws -> Void
    ) -> Workflow<Input, Void> {
        Workflow<Input, Void>(
            firstJobName: self.firstJobName
        ) { queue, workflowName, _ in
            self.registerJobs(queue, workflowName, .job(named: name, workflow: workflowName))
            queue.registerFinalWorkflowJob(
                workflowName: workflowName,
                workflowJob: .init(name: name, parameters: Output.self, retryStrategy: retryStrategy, timeout: timeout, execute: operation)
            )
        }
    }

    ///  Return new workflow with child workflow added
    /// - Parameter step: Child workflow to add
    /// - Returns: New workflow with child workflow added
    public func addChildWorkflow<Output2>(_ childWorkflow: Workflow<Output, Output2>) -> Workflow<Input, Output2> {
        Workflow<Input, Output2>(
            firstJobName: self.firstJobName
        ) { queue, workflowName, nextStep in
            self.registerJobs(queue, workflowName, .job(named: childWorkflow.firstJobName, workflow: workflowName))
            childWorkflow.registerJobs(queue, workflowName, nextStep)
        }
    }

    /// Run one or another workflow based off a condition
    /// - Parameters:
    ///   - condition: The closure holding the condition
    ///   - thenWorkflowBuilder: Closure building workflow for then clause
    ///   - elseWorkflowBuilder: Closure building workflow for else clause
    /// - Returns: New workflow with condition added
    public func `if`<Output2>(
        _ condition: @Sendable @escaping (Output) throws -> Bool,
        then thenWorkflowBuilder: (WorkflowBuilder<Output>) -> Workflow<Output, Output2>,
        else elseWorkflowBuilder: (WorkflowBuilder<Output>) -> Workflow<Output, Output2>
    ) -> Workflow<Input, Output2> {
        let thenWorkflow = thenWorkflowBuilder(WorkflowBuilder())
        let elseWorkflow = elseWorkflowBuilder(WorkflowBuilder())
        return Workflow<Input, Output2>(
            firstJobName: self.firstJobName
        ) { queue, workflowName, nextStep in
            self.registerJobs(
                queue,
                workflowName,
                .ifelse(
                    ifName: .job(named: thenWorkflow.firstJobName, workflow: workflowName),
                    elseName: .job(named: elseWorkflow.firstJobName, workflow: workflowName),
                    condition: condition
                )
            )
            thenWorkflow.registerJobs(queue, workflowName, nextStep)
            elseWorkflow.registerJobs(queue, workflowName, nextStep)
        }
    }

    /// Run a workflow based off a condition
    ///
    /// The workflow from the then clause is required to return the same output as if
    /// the condition failed.
    /// - Parameters:
    ///   - condition: The closure holding the condition
    ///   - thenWorkflowBuilder: Closure building workflow for then clause
    /// - Returns: New workflow with condition added
    public func `if`(
        _ condition: @Sendable @escaping (Output) throws -> Bool,
        then thenWorkflowBuilder: (WorkflowBuilder<Output>) -> Workflow<Output, Output>
    ) -> Workflow<Input, Output> {
        let thenWorkflow = thenWorkflowBuilder(WorkflowBuilder())
        return Workflow(
            firstJobName: self.firstJobName
        ) { queue, workflowName, nextStep in
            self.registerJobs(
                queue,
                workflowName,
                .ifelse(
                    ifName: .job(named: thenWorkflow.firstJobName, workflow: workflowName),
                    elseName: nextStep,
                    condition: condition
                )
            )
            thenWorkflow.registerJobs(queue, workflowName, nextStep)
        }
    }

    /// Run a workflow and end workflow based off a condition
    ///
    /// The workflow from the then clause is required to Void.
    /// - Parameters:
    ///   - condition: The closure holding the condition
    ///   - thenWorkflowBuilder: Closure building workflow for then clause
    /// - Returns: New workflow with condition added
    public func `if`(
        _ condition: @Sendable @escaping (Output) throws -> Bool,
        then thenWorkflowBuilder: (WorkflowBuilder<Output>) -> Workflow<Output, Void>
    ) -> Workflow<Input, Output> {
        let thenWorkflow = thenWorkflowBuilder(WorkflowBuilder())
        return Workflow(
            firstJobName: self.firstJobName
        ) { queue, workflowName, nextStep in
            self.registerJobs(
                queue,
                workflowName,
                .ifelse(
                    ifName: .job(named: thenWorkflow.firstJobName, workflow: workflowName),
                    elseName: nextStep,
                    condition: condition
                )
            )
            thenWorkflow.registerJobs(queue, workflowName, .none)
        }
    }

    /// Group workflows under a sub-workflow name to avoid name clashes
    /// - Parameters:
    ///   - name: Sub workflow name
    ///   - groupWorkflowBuilder: Closure building workflow
    /// - Returns: New workflow with group added
    public func group<Output2>(
        name: String,
        group groupWorkflowBuilder: (WorkflowBuilder<Output>) -> Workflow<Output, Output2>
    ) -> Workflow<Input, Output2> {
        let groupWorkflow = groupWorkflowBuilder(WorkflowBuilder())
        return Workflow<Input, Output2>(
            firstJobName: self.firstJobName
        ) { queue, workflowName, nextStep in
            let groupWorkflowName = "\(workflowName).\(name)"
            self.registerJobs(queue, workflowName, .job(named: groupWorkflow.firstJobName, workflow: groupWorkflowName))
            groupWorkflow.registerJobs(queue, groupWorkflowName, nextStep)
        }
    }

    public func dropResult() -> Workflow<Input, Void> {
        Workflow<Input, Void>(
            firstJobName: self.firstJobName
        ) { queue, workflowName, nextStep in
            self.registerJobs(queue, workflowName, .none)
        }
    }
}

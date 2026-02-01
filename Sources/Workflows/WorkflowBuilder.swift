//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

import Jobs

public struct WorkflowBuilder<Input: Codable & Sendable> {
    @inlinable
    public init() {}

    ///  Create workflow and add a single step to the workflow
    /// - Parameter step: Workflow step to add
    /// - Returns: New workflow with step added
    public func addStep<Output: Codable & Sendable>(_ step: WorkflowStep<Input, Output>) -> Workflow<Input, Output> {
        Workflow(
            firstJobName: step.name
        ) { queue, workflowName, nextStep in
            queue.registerWorkflowJob(
                workflowName: workflowName,
                workflowStep: step,
                nextStep: nextStep
            )
        }
    }

    ///  Create workflow and add a single step to the workflow
    /// - Parameter step: Workflow step to add
    /// - Returns: New workflow with step added
    public func addStep(
        _ job: WorkflowStep<Input, Void>
    ) -> Workflow<Input, Void> {
        Workflow(
            firstJobName: job.name
        ) { queue, workflowName, nextStep in
            queue.registerFinalWorkflowJob(
                workflowName: workflowName,
                workflowJob: job
            )
        }
    }

    ///  Create workflow and add a single step to the workflow
    /// - Parameter step: Workflow step to add
    /// - Returns: New workflow with step added
    public func addStep<Output: Codable & Sendable>(
        name: String,
        input: Input.Type = Input.self,
        retryStrategy: any JobRetryStrategy = .dontRetry,
        timeout: Duration? = nil,
        operation: @escaping @Sendable (Input, WorkflowExecutionContext) async throws -> Output
    ) -> Workflow<Input, Output> {
        Workflow(
            firstJobName: name
        ) { queue, workflowName, nextStep in
            queue.registerWorkflowJob(
                workflowName: workflowName,
                workflowStep: .init(name: name, parameters: Input.self, retryStrategy: retryStrategy, timeout: timeout, execute: operation),
                nextStep: nextStep
            )
        }
    }

    ///  Create workflow and add a single step to the workflow
    /// - Parameter step: Workflow step to add
    /// - Returns: New workflow with step added
    public func addStep(
        name: String,
        input: Input.Type = Input.self,
        retryStrategy: any JobRetryStrategy = .dontRetry,
        timeout: Duration? = nil,
        operation: @escaping @Sendable (Input, WorkflowExecutionContext) async throws -> Void
    ) -> Workflow<Input, Void> {
        Workflow(
            firstJobName: name
        ) { queue, workflowName, nextStep in
            queue.registerFinalWorkflowJob(
                workflowName: workflowName,
                workflowJob: .init(name: name, parameters: Input.self, retryStrategy: retryStrategy, timeout: timeout, execute: operation)
            )
        }
    }

    ///  Return new workflow with child workflow added
    /// - Parameter step: Child workflow to add
    /// - Returns: New workflow with child workflow added
    public func addChildWorkflow<Output>(_ childWorkflow: Workflow<Input, Output>) -> Workflow<Input, Output> {
        Workflow<Input, Output>(
            firstJobName: childWorkflow.firstJobName
        ) { queue, workflowName, nextStep in
            childWorkflow.registerJobs(queue, workflowName, nextStep)
        }
    }
}

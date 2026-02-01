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
    /// - Returns: workflow
    public func addStep<Output: Codable & Sendable>(_ step: WorkflowStep<Input, Output>) -> Workflow<Input, Output> {
        Workflow(
            firstJobName: step.name
        ) { queue, workflowName, nextItem in
            queue.registerWorkflowJob(
                workflowName: workflowName,
                workflowStep: step,
                nextItem: nextItem
            )
        }
    }

    ///  Create workflow and add a single step to the workflow
    /// - Parameter step: Workflow step to add
    /// - Returns: workflow
    public func addStep(
        _ job: WorkflowStep<Input, Void>
    ) -> Workflow<Input, Void> {
        Workflow(
            firstJobName: job.name
        ) { queue, workflowName, nextItem in
            queue.registerFinalWorkflowJob(
                workflowName: workflowName,
                workflowJob: job
            )
        }
    }
}

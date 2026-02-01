//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

import Jobs

/// Fully constructed workflow
public struct Workflow<Input: Codable & Sendable, Output> {
    /// name of first job to trigger workflow
    let firstJobName: String
    /// function to call to register all the workflow jobs
    let registerJobs: (any JobQueueProtocol, String, WorkflowNextJob<Output>) -> Void
}

extension Workflow where Output: Codable & Sendable {
    func addStep<Output2: Codable & Sendable>(_ workflowJob: WorkflowJob<Output, Output2>) -> Workflow<Input, Output2> {
        Workflow<Input, Output2>(
            firstJobName: self.firstJobName
        ) { queue, workflowName, nextItem in
            self.registerJobs(queue, workflowName, .job(named: workflowJob.name, workflow: workflowName))
            queue.registerWorkflowJob(
                workflowName: workflowName,
                workflowJob: workflowJob,
                nextItem: nextItem
            )
        }
    }

    func addStep(_ workflowJob: WorkflowJob<Output, Void>) -> Workflow<Input, Void> {
        Workflow<Input, Void>(
            firstJobName: self.firstJobName
        ) { queue, workflowName, _ in
            self.registerJobs(queue, workflowName, .job(named: workflowJob.name, workflow: workflowName))
            queue.registerFinalWorkflowJob(
                workflowName: workflowName,
                workflowJob: workflowJob
            )
        }
    }

    func addChildWorkflow<Output2>(_ childWorkflow: Workflow<Output, Output2>) -> Workflow<Input, Output2> {
        Workflow<Input, Output2>(
            firstJobName: self.firstJobName
        ) { queue, workflowName, nextItem in
            self.registerJobs(queue, workflowName, .job(named: childWorkflow.firstJobName, workflow: workflowName))
            childWorkflow.registerJobs(queue, workflowName, nextItem)
        }
    }
}

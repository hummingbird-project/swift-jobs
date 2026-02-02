//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

@resultBuilder
public struct WorkflowResultBuilder<WorkflowInput: Codable & Sendable, WorkflowOutput: Codable & Sendable> {

    /// First item with output
    public static func buildPartialBlock<Output: Codable & Sendable>(
        first job: WorkflowStep<WorkflowInput, Output>
    ) -> Workflow<WorkflowInput, Output> {
        WorkflowBuilder().addStep(job)
    }

    /// Item with previous and subsequents items
    public static func buildPartialBlock<Input: Codable & Sendable, Output: Codable & Sendable>(
        accumulated workflow: Workflow<WorkflowInput, Input>,
        next job: WorkflowStep<Input, Output>
    ) -> Workflow<WorkflowInput, Output> {
        workflow.addStep(job)
    }

    // Final result
    public static func buildFinalResult(
        _ workflow: Workflow<WorkflowInput, WorkflowOutput>
    ) -> Workflow<WorkflowInput, WorkflowOutput> {
        workflow
    }
}

//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//
extension WorkflowResultBuilder {
    /// First item
    public static func buildPartialBlock<Output>(
        first workflow: Workflow<WorkflowInput, Output>
    ) -> Workflow<WorkflowInput, Output> {
        workflow
    }

    /// Item with previous and subsequents items
    public static func buildPartialBlock<Input: Codable & Sendable, Output>(
        accumulated workflow: Workflow<WorkflowInput, Input>,
        next childWorkflow: Workflow<Input, Output>
    ) -> Workflow<WorkflowInput, Output> {
        workflow.addChildWorkflow(childWorkflow)
    }
}

//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//
import Jobs

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

struct WorkFlowJobInput<Input: Codable & Sendable>: Codable & Sendable {
    let input: Input
    let workflowID: String
}

extension JobQueueProtocol {
    func registerWorkflowJob<Input: Codable & Sendable, Output: Codable & Sendable>(
        workflowName: String,
        workflowStep: WorkflowStep<Input, Output>,
        nextStep: WorkflowNextStep<Output>
    ) {
        let jobName = "\(workflowName).\(workflowStep.name)"
        self.logger.info(
            "Registered Workflow Job",
            metadata: ["JobName": .stringConvertible(jobName), "WorkflowName": .stringConvertible(workflowName)]
        )
        let job = JobDefinition(
            name: .init(jobName),
            parameters: WorkFlowJobInput<Input>.self,
            retryStrategy: workflowStep.retryStrategy,
            timeout: workflowStep.timeout
        ) { parameters, context in
            var logger = context.logger
            logger[metadataKey: "WorkflowID"] = .string(parameters.workflowID)
            let workflowContext = WorkflowExecutionContext(
                workflowID: parameters.workflowID,
                jobID: context.jobID,
                logger: logger,
                attempt: context.attempt,
                queuedAt: context.queuedAt
            )
            let output = try await workflowStep._execute(parameters.input, workflowContext)
            // work out what the next workflow job is and push it to the queue
            if let outputJobName = try await nextStep.getNextWorkflowJob(output) {
                self.logger.debug(
                    "Triggering job",
                    metadata: ["JobName": .stringConvertible(outputJobName), "FromJob": .stringConvertible(jobName)]
                )
                try await self.pushWorkflowJob(.init(outputJobName), workflowID: parameters.workflowID, parameters: output)
            }
        }
        self.registerJob(job)
    }

    public func registerWorkflow<Input: Codable & Sendable>(
        name: String,
        input: Input.Type = Input.self,
        @WorkflowResultBuilder<Input, EmptyOutput> buildWorkflow: () -> Workflow<Input, EmptyOutput>
    ) -> WorkflowName<Input> {
        let workflow = buildWorkflow()
        workflow.registerJobs(self, name, .none)
        return WorkflowName("\(name).\(workflow.firstJobName)")
    }

    public func registerWorkflow<Input: Codable & Sendable>(
        name: String,
        input: Input.Type = Input.self,
        workflow: Workflow<Input, EmptyOutput>
    ) -> WorkflowName<Input> {
        workflow.registerJobs(self, name, .none)
        return WorkflowName("\(name).\(workflow.firstJobName)")
    }

    public func createChildWorkflow<Input: Codable & Sendable, Output>(
        input: Input.Type = Input.self,
        output: Output.Type = Output.self,
        @WorkflowResultBuilder<Input, Output> buildWorkflow: () -> Workflow<Input, Output>
    ) -> Workflow<Input, Output> {
        buildWorkflow()
    }

    public func pushWorkflow<Input: Sendable & Codable>(
        _ workflowName: WorkflowName<Input>,
        parameters: Input
    ) async throws -> String {
        let id = UUID().uuidString
        _ = try await self.push(.init(workflowName.name), parameters: WorkFlowJobInput(input: parameters, workflowID: id))
        return id
    }

    func pushWorkflowJob<Input: Sendable & Codable>(
        _ workflowName: WorkflowName<Input>,
        workflowID: String,
        parameters: Input
    ) async throws {
        _ = try await self.push(.init(workflowName.name), parameters: WorkFlowJobInput(input: parameters, workflowID: workflowID))
    }
}

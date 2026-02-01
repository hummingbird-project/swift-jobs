//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

/// Decision on what job to run next
indirect enum WorkflowNextStep<Output> {
    case none
    case job(named: String)
    case ifelse(ifName: WorkflowNextStep<Output>, elseName: WorkflowNextStep<Output>, condition: @Sendable (Output) async throws -> Bool)

    static func job(named: String, workflow: String) -> Self {
        .job(named: "\(workflow).\(named)")
    }

    func getNextWorkflowJob(_ output: Output) async throws -> String? {
        var current = self
        while true {
            switch current {
            case .none:
                return nil
            case .job(let outputName):
                return outputName
            case .ifelse(let ifName, let elseName, let condition):
                current =
                    if try await condition(output) {
                        ifName
                    } else {
                        elseName
                    }
            }
        }
    }
}

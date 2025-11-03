//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2025 the Hummingbird authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See hummingbird/CONTRIBUTORS.txt for the list of Hummingbird authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Jobs
import Logging
import ServiceLifecycle
import Testing

@testable import Workflows

/// Helper function for test a server
///
/// Creates test client, runs test function abd ensures everything is
/// shutdown correctly
public func testWorkflow<Value>(
    _ taskQueue: Service,
    _ test: () async throws -> Value
) async throws -> Value {
    try await withThrowingTaskGroup(of: Void.self) { group in
        let serviceGroup = ServiceGroup(
            configuration: .init(
                services: [taskQueue],
                gracefulShutdownSignals: [.sigterm, .sigint],
                logger: Logger(label: "WorkflowService")
            )
        )
        group.addTask {
            try await serviceGroup.run()
        }
        let value = try await test()
        await serviceGroup.triggerGracefulShutdown()
        return value
    }
}

/// Helper to wait for workflow completion using TestExpectation
public func waitForWorkflowCompletion(
    _ workflowId: WorkflowID,
    engine: WorkflowEngine<MemoryQueue>,
    expectedStatus: WorkflowStatus = .completed,
    timeout: Duration = .seconds(10),
    description: String = "workflow completion"
) async throws -> WorkflowExecutionStatus {
    let expectation = TestExpectation()

    let pollingTask = Task {
        while !Task.isCancelled {
            do {
                let status = try await engine.getWorkflowStatus(workflowId)
                if status.status == expectedStatus {
                    expectation.trigger()
                    return
                } else if status.status == .failed && expectedStatus != .failed {
                    expectation.trigger()
                    return
                }
                try await Task.sleep(for: .milliseconds(100))
            } catch {
                continue
            }
        }
    }

    defer { pollingTask.cancel() }

    try await expectation.wait(for: description, timeout: timeout)
    return try await engine.getWorkflowStatus(workflowId)
}

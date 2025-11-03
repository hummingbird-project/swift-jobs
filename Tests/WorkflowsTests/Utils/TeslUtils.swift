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

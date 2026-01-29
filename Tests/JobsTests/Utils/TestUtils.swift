//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

import Jobs
import Logging
import ServiceLifecycle

/// Helper function for test a server
///
/// Creates test client, runs test function abd ensures everything is
/// shutdown correctly
public func testJobQueue<Value>(
    _ jobQueue: Service,
    _ test: () async throws -> Value
) async throws -> Value {
    try await withThrowingTaskGroup(of: Void.self) { group in
        let serviceGroup = ServiceGroup(
            configuration: .init(
                services: [jobQueue],
                gracefulShutdownSignals: [.sigterm, .sigint],
                logger: Logger(label: "JobQueueService")
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

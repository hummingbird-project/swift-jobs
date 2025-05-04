//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2024 the Hummingbird authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See hummingbird/CONTRIBUTORS.txt for the list of Hummingbird authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import ServiceLifecycle

public struct LeadershipElectorService<Driver: JobQueueDriver>: Service, CustomStringConvertible {
    private let jobQueue: JobQueue<Driver>
    private let interval: Duration

    public init(jobQueue: JobQueue<Driver>, interval: Duration = .seconds(10)) {
        self.jobQueue = jobQueue
        self.interval = interval
    }

    public func run() async throws {
        try await jobQueue.initializationComplete.waitUntilTriggered()

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                while !Task.isShuttingDownGracefully {
                    try await jobQueue.queue.electLeaderShip()

                    try await Task.sleep(for: interval)
                }
            }
            try await group.waitForAll()
        }
    }

    public var description: String {
        "LeadershipElectorService(\(String(describing: jobQueue)))"
    }
}

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

import ServiceLifecycle

public protocol JobEventDriver: Sendable {
    func publishEvent(_ event: JobEvent) async throws
}

public struct JobEventQueue<Driver: JobEventDriver>: Service {
    let driver: Driver
    let continuation: AsyncStream<JobEvent>.Continuation
    let stream: AsyncStream<JobEvent>

    public init(_ driver: Driver, bufferSize: Int = 1000) {
        self.driver = driver
        (self.stream, self.continuation) = AsyncStream.makeStream(of: JobEvent.self, bufferingPolicy: .bufferingNewest(bufferSize))
    }

    public func publishEvent(_ event: JobEvent) {
        self.continuation.yield(event)
    }

    public func run() async throws {
        try await cancelWhenGracefulShutdown {
            for await event in stream {
                try await self.driver.publishEvent(event)
            }
        }
    }
}

//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

public protocol JobEventsDriver: Sendable {
    associatedtype EventStream: AsyncSequence<JobEvent, Never>
    /// publish event
    func publish(event: JobEvent) async
    /// subscribe to event type stream
    func subscribe<Value>(
        eventType: JobEvent.EventType,
        from: Date,
        _ operation: (_ events: EventStream) async throws -> Value
    ) async throws -> sending Value
    /// subscribe to events for a particular id
    func subscribe<Value>(id: String, from: Date, _ operation: (_ events: EventStream) async throws -> Value) async throws -> sending Value
}

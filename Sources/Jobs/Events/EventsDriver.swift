//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

public protocol JobEventsDriver: Sendable {
    associatedtype EventStream: AsyncSequence<JobEvent, Never>
    /// publish event
    func publish(event: JobEvent) async
    /// subscribe to event stream
    func subscribe<Value>(_ operation: (_ events: EventStream) async throws -> Value) async throws -> sending Value
}

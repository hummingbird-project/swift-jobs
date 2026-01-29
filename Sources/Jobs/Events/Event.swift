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

public struct JobEvent: Sendable {
    public struct EventType: Sendable, Equatable {
        let rawValue: Int

        static let jobPushed: EventType = .init(rawValue: "job-pushed".hashValue)
        static let jobStarted: EventType = .init(rawValue: "job-started".hashValue)
        static let jobFailed: EventType = .init(rawValue: "job-failed".hashValue)
        static let jobCompleted: EventType = .init(rawValue: "job-completed".hashValue)
    }

    public let type: EventType
    public let id: String
    public let time: Date
    public let data: [String: String]

    public init(type: JobEvent.EventType, id: String, time: Date, data: [String: String] = [:]) {
        self.type = type
        self.id = id
        self.time = time
        self.data = data
    }
}

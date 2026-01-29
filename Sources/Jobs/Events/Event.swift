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

        /// Job was pushed to queue
        public static let jobPushed: EventType = .init(rawValue: "job-pushed".hashValue)
        /// Job has started
        public static let jobStarted: EventType = .init(rawValue: "job-started".hashValue)
        /// This attempt at the Job failed
        public static let jobAttemptFailed: EventType = .init(rawValue: "job-attempt-failed".hashValue)
        /// The job has failed enough times it has been parked in the fail queue
        public static let jobFailed: EventType = .init(rawValue: "job-failed".hashValue)
        /// The job sucessfully completed
        public static let jobCompleted: EventType = .init(rawValue: "job-completed".hashValue)
    }

    public struct EventParameter: Sendable, Equatable, Hashable {
        let rawValue: Int

        public static let attempt: EventParameter = .init(rawValue: "attempt".hashValue)
    }

    public enum EventValue: Sendable, Equatable {
        case string(String)
        case int(Int)
        case double(Double)
        case bool(Bool)
    }

    public let type: EventType
    public let id: String
    public let time: Date
    public let parameters: [EventParameter: EventValue]

    public init(type: JobEvent.EventType, id: String, time: Date, parameters: [EventParameter: EventValue] = [:]) {
        self.type = type
        self.id = id
        self.time = time
        self.parameters = parameters
    }
}

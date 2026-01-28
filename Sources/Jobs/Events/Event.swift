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
    public struct EventType: Sendable {
        let rawValue: Int

        static let jobPushed: EventType = .init(rawValue: "job-pushed".hashValue)
        static let jobStarted: EventType = .init(rawValue: "job-started".hashValue)
        static let jobFailed: EventType = .init(rawValue: "job-failed".hashValue)
        static let jobCompleted: EventType = .init(rawValue: "job-completed".hashValue)
    }

    let type: EventType
    let id: String
    let time: Date
}

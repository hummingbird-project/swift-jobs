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

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

public struct JobEvent: Sendable, Encodable {
    struct Job: Codable {
        let jobName: String
        let jobID: String
        let queueName: String
    }

    struct SuccessfulJobParameters: Encodable {
        let job: Job
        let processingTime: UInt64
    }

    struct FailedJobParameters: Encodable {
        let job: Job
        let error: String
    }

    enum Event: Encodable {
        case pushJob(Job)
        case popJob(Job)
        case retryJob(Job)
        case successfulJob(SuccessfulJobParameters)
        case failedJob(FailedJobParameters)
    }

    let date: Date
    let id: String
    let event: Event

    init(_ event: Event) {
        self.event = event
        self.id = UUID().uuidString
        self.date = .now
    }

    public static func pushJob(name: String, id: String, queueName: String) -> Self {
        .init(.pushJob(.init(jobName: name, jobID: id, queueName: queueName)))
    }

    public static func retryJob(name: String, id: String, queueName: String) -> Self {
        .init(.retryJob(.init(jobName: name, jobID: id, queueName: queueName)))
    }

    public static func popJob(name: String, id: String, queueName: String) -> Self {
        .init(.popJob(.init(jobName: name, jobID: id, queueName: queueName)))
    }

    public static func successfulJob(name: String, id: String, queueName: String, processingTimeNanoseconds: UInt64) -> Self {
        .init(.successfulJob(.init(job: .init(jobName: name, jobID: id, queueName: queueName), processingTime: processingTimeNanoseconds)))
    }

    public static func failedJob(name: String, id: String, queueName: String, error: any Error) -> Self {
        .init(.failedJob(.init(job: .init(jobName: name, jobID: id, queueName: queueName), error: "\(error)")))
    }
}

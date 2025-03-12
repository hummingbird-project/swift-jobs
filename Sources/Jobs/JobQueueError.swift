//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2021-2024 the Hummingbird authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See hummingbird/CONTRIBUTORS.txt for the list of Hummingbird authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

/// Job Queue Error type
public struct JobQueueError: Error, Equatable {
    public struct ErrorCode: Equatable, Sendable, CustomStringConvertible {
        /// failed to decode job. Possibly because it hasn't been registered or data that was expected
        /// is not available
        public static var decodeJobFailed: Self { .init(.decodeJobFailed) }
        /// failed to decode job as the job id is not recognised
        public static var unrecognisedJobId: Self { .init(.unrecognisedJobId) }
        /// failed to get job from queue
        public static var dequeueError: Self { .init(.dequeueError) }
        /// job timed out
        public static var jobTimedOut: Self { .init(.jobTimedOut) }

        private enum _ErrorCode: String {
            case decodeJobFailed
            case unrecognisedJobId
            case dequeueError
            case jobTimedOut
        }

        private let code: _ErrorCode

        private init(_ code: _ErrorCode) {
            self.code = code
        }

        public var description: String {
            self.code.rawValue
        }
    }

    /// Error code
    public let code: ErrorCode
    /// Job that caused this error
    public let jobName: String?
    /// Any additional details that might help
    public let details: String?

    public init(code: ErrorCode, jobName: String?, details: String? = nil) {
        self.code = code
        self.jobName = jobName
        self.details = details
    }
}

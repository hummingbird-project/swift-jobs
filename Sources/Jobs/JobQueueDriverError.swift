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

/// Job Queue Driver error type
///
/// Error returned by job queue driver.
public struct JobQueueDriverError: Error {
    public struct ErrorCode: Equatable, Sendable, CustomStringConvertible {
        /// failed to connect to underlying driver
        public static var connectionError: Self { .init(.connectionError) }

        private enum _ErrorCode: String {
            case connectionError
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
    /// underlying error
    public let underlyingError: Error

    public init(_ code: ErrorCode, underlyingError: Error) {
        self.code = code
        self.underlyingError = underlyingError
    }
}

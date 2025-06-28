//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2021-2025 the Hummingbird authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See hummingbird/CONTRIBUTORS.txt for the list of Hummingbird authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

/// Identifier for a Job.
///
/// This should be unique as it is used to identify jobs when deserializing them.
///
/// `JobName` has an associated generic parameter to indicate the parameters associated with this job
public struct JobName<Parameters: Codable>: CustomStringConvertible, ExpressibleByStringLiteral, Sendable, Equatable {
    public let name: String

    /// Initialize JobName
    /// - Parameters:
    ///   - string: Job name
    ///   - parameters: Parameters associated with job
    public init(_ string: String, parameters: Parameters.Type = Parameters.self) {
        self.name = string
    }

    ///  Initialize JobName with string literal
    /// - Parameter string: job name
    public init(stringLiteral string: String) {
        self.name = string
    }

    /// JobName textual representation
    public var description: String {
        self.name
    }
}

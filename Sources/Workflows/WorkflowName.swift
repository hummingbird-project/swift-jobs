//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

/// Identifier for a Workflow.
///
/// This should be unique as it is used to identify workflow jobs when deserializing them.
///
/// `WorkflowName` has an associated generic parameter to indicate the parameters associated with this workflow
public struct WorkflowName<Input: Codable>: CustomStringConvertible, ExpressibleByStringLiteral, Sendable, Equatable {
    public let name: String

    /// Initialize WorkflowName
    /// - Parameters:
    ///   - string: Workflow name
    ///   - parameters: Parameters associated with job
    public init(_ string: String, parameters: Input.Type = Input.self) {
        self.name = string
    }

    ///  Initialize WorkflowName with string literal
    /// - Parameter string: workflow name
    public init(stringLiteral string: String) {
        self.name = string
    }

    /// WorkflowName textual representation
    public var description: String {
        self.name
    }
}

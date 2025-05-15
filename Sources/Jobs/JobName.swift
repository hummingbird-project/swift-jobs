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

public struct JobName<Parameters: Codable>: CustomStringConvertible, ExpressibleByStringLiteral, Sendable, Equatable {
    public let name: String

    public init(_ string: String, parameters: Parameters.Type = Parameters.self) {
        self.name = string
    }

    public init(stringLiteral string: String) {
        self.name = string
    }

    public var description: String {
        self.name
    }
}

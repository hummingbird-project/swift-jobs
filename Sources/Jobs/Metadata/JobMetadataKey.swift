//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

public struct JobMetadataKey<Value>: Sendable, Hashable, ExpressibleByStringLiteral, ExpressibleByStringInterpolation {
    let name: String
    /// Initialize a JobMetadataKey
    ///
    /// - Parameters:
    ///   - name: Unique name for key
    ///   - value: Value type associated with Metadata Key
    public init(_ name: String, value: Value.Type = Value.self) { self.name = name }

    /// Initialize a JobMetadataKey from a string literal
    ///
    /// ```swift
    /// extension JobMetadataKey where Value == MyValue {
    ///     static var myMetadataKey: Self { .init("myMetadata")}
    /// }
    /// This can only be used in a situation where the Value type is defined elsewhere
    /// - Parameter string:
    public init(stringLiteral string: String) {
        self.name = string
    }

    /// Initialize a JobMetadataKey from a string literal
    ///
    /// ```swift
    /// extension JobMetadataKey where Value == MyValue {
    ///     static var myMetadataKey: Self { .init("myMetadata")}
    /// }
    /// This can only be used in a situation where the Value type is defined elsewhere
    /// - Parameter string:
    public init(stringInterpolation string: String) {
        self.name = string
    }
}

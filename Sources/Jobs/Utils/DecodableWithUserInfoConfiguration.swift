//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2024 the Hummingbird authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See hummingbird/CONTRIBUTORS.txt for the list of Hummingbird authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import NIOCore
import NIOFoundationCompat

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Implementation of DecodableWithConfiguration which extracts the configuration from the userInfo array
///
/// This is necessary as Linux Foundation does not have support for setting DecodableWithConfiguration
/// configuration from the JSONDecoder
protocol DecodableWithUserInfoConfiguration: Decodable, DecodableWithConfiguration {}

/// Implement `init(from: Decoder)`` by extracting configuration from the userInfo dictionary.
extension DecodableWithUserInfoConfiguration {
    public init(from decoder: Decoder) throws {
        guard let configuration = decoder.userInfo[._jobConfiguration] as? DecodingConfiguration else {
            throw DecodingError.valueNotFound(
                DecodingConfiguration.self,
                .init(codingPath: decoder.codingPath, debugDescription: "Failed to find Decoding configuration")
            )
        }
        try self.init(from: decoder, configuration: configuration)
    }
}

extension CodingUserInfoKey {
    /// Coding UserInfo key used to store DecodableWithUserInfoConfiguration configuration
    public static var _jobConfiguration: Self { .init(rawValue: "_job_configuration_")! }
}

extension JSONDecoder {
    /// Version of JSONDecoder that sets up configuration userInfo for the DecodableWithUserInfoConfiguration
    /// protocol
    func decode<T>(
        _ type: T.Type,
        from buffer: ByteBuffer,
        userInfoConfiguration: T.DecodingConfiguration
    ) throws -> T where T: DecodableWithUserInfoConfiguration, T.DecodingConfiguration: Sendable {
        self.userInfo[._jobConfiguration] = userInfoConfiguration
        return try self.decode(type, from: buffer)
    }
}

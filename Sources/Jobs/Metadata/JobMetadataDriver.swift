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

import NIOCore

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

public protocol JobMetadataDriver {
    /// get job queue metadata
    func getMetadata(_ key: String) async throws -> ByteBuffer?
    /// set job queue metadata
    func setMetadata(key: String, value: ByteBuffer) async throws
}

extension JobMetadataDriver {
    /// Get JobQueue metadata
    func getMetadata<Value: Codable>(_ key: JobMetadataKey<Value>) async throws -> Value? {
        guard let buffer = try await self.getMetadata(key.name) else { return nil }
        return try JSONDecoder().decode(Value.self, from: buffer)
    }

    /// Set JobQueue metadata
    func setMetadata<Value: Codable>(key: JobMetadataKey<Value>, value: Value) async throws {
        let buffer = try JSONEncoder().encodeAsByteBuffer(value, allocator: ByteBufferAllocator())
        try await self.setMetadata(key: key.name, value: buffer)
    }
}

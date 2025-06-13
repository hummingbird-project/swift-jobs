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
    /// Acquire metadata lock
    func acquireMetadataLock(key: String, id: ByteBuffer, expiresIn: TimeInterval) async throws -> Bool
    /// Release metadata lock
    func releaseMetadataLock(key: String, id: ByteBuffer) async throws
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

    /// Acquire lock
    func acquireMetadataLock<ID: Codable>(key: JobMetadataKey<ID>, id: ID, expiresIn: TimeInterval) async throws -> Bool {
        let buffer = try JSONEncoder().encodeAsByteBuffer(id, allocator: ByteBufferAllocator())
        return try await self.acquireMetadataLock(key: key.name, id: buffer, expiresIn: expiresIn)
    }

    /// Release lock
    func releaseMetadataLock<ID: Codable>(key: JobMetadataKey<ID>, id: ID) async throws {
        let buffer = try JSONEncoder().encodeAsByteBuffer(id, allocator: ByteBufferAllocator())
        try await self.releaseMetadataLock(key: key.name, id: buffer)
    }

    /// Get JobQueue metadata
    func getMetadata(_ key: JobMetadataKey<ByteBuffer>) async throws -> ByteBuffer? {
        try await self.getMetadata(key.name)
    }

    /// Set JobQueue metadata
    func setMetadata(key: JobMetadataKey<ByteBuffer>, value: ByteBuffer) async throws {
        try await self.setMetadata(key: key.name, value: value)
    }

    /// Acquire lock
    func acquireMetadataLock(key: JobMetadataKey<ByteBuffer>, id: ByteBuffer, expiresIn: TimeInterval) async throws -> Bool {
        try await self.acquireMetadataLock(key: key.name, id: id, expiresIn: expiresIn)
    }

    /// Release lock
    func releaseMetadataLock(key: JobMetadataKey<ByteBuffer>, id: ByteBuffer) async throws {
        try await self.releaseMetadataLock(key: key.name, id: id)
    }
}

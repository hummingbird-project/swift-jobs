//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//

import NIOCore

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

public protocol JobMetadataDriver: Sendable {
    /// Get JobQueue metadata
    ///
    /// - Parameter key: Metadata key
    /// - Returns: Value associated with metadata key
    func getMetadata(_ key: String) async throws -> ByteBuffer?
    /// Set JobQueue metadata
    ///
    /// - Parameters:
    ///   - key: Metadata key
    ///   - value: Value associated with metadata key
    func setMetadata(key: String, value: ByteBuffer) async throws
    /// Acquire metadata lock
    ///
    /// - Parameters:
    ///   - key: Metadata key
    ///   - id: Lock identifier
    ///   - expiresIn: When lock will expire
    /// - Returns: If lock was acquired
    func acquireLock(key: String, id: ByteBuffer, expiresIn: TimeInterval) async throws -> Bool
    /// Release metadata lock
    ///
    /// - Parameters:
    ///   - key: Metadata key
    ///   - id: Lock identifier
    func releaseLock(key: String, id: ByteBuffer) async throws
}

extension JobMetadataDriver {
    /// Get JobQueue metadata
    ///
    /// - Parameter key: Metadata key
    /// - Returns: Value associated with metadata key
    public func getMetadata<Value: Codable>(_ key: JobMetadataKey<Value>) async throws -> Value? {
        guard let buffer = try await self.getMetadata(key.name) else { return nil }
        return try JSONDecoder().decode(Value.self, from: buffer)
    }

    /// Set JobQueue metadata
    ///
    /// - Parameters:
    ///   - key: Metadata key
    ///   - value: Value associated with metadata key
    public func setMetadata<Value: Codable>(key: JobMetadataKey<Value>, value: Value) async throws {
        let buffer = try JSONEncoder().encodeAsByteBuffer(value, allocator: ByteBufferAllocator())
        try await self.setMetadata(key: key.name, value: buffer)
    }

    /// Acquire metadata lock
    ///
    /// - Parameters:
    ///   - key: Metadata key
    ///   - id: Lock identifier
    ///   - expiresIn: When lock will expire
    /// - Returns: If lock was acquired
    public func acquireLock<ID: Codable>(key: JobMetadataKey<ID>, id: ID, expiresIn: TimeInterval) async throws -> Bool {
        let buffer = try JSONEncoder().encodeAsByteBuffer(id, allocator: ByteBufferAllocator())
        return try await self.acquireLock(key: key.name, id: buffer, expiresIn: expiresIn)
    }

    /// Release metadata lock
    ///
    /// - Parameters:
    ///   - key: Metadata key
    ///   - id: Lock identifier
    public func releaseLock<ID: Codable>(key: JobMetadataKey<ID>, id: ID) async throws {
        let buffer = try JSONEncoder().encodeAsByteBuffer(id, allocator: ByteBufferAllocator())
        try await self.releaseLock(key: key.name, id: buffer)
    }

    /// Get JobQueue metadata
    ///
    /// - Parameter key: Metadata key
    /// - Returns: Value associated with metadata key
    public func getMetadata(_ key: JobMetadataKey<ByteBuffer>) async throws -> ByteBuffer? {
        try await self.getMetadata(key.name)
    }

    /// Set JobQueue metadata
    ///
    /// - Parameters:
    ///   - key: Metadata key
    ///   - value: Value associated with metadata key
    public func setMetadata(key: JobMetadataKey<ByteBuffer>, value: ByteBuffer) async throws {
        try await self.setMetadata(key: key.name, value: value)
    }

    /// Acquire metadata lock
    ///
    /// - Parameters:
    ///   - key: Metadata key
    ///   - id: Lock identifier
    ///   - expiresIn: When lock will expire
    /// - Returns: If lock was acquired
    public func acquireLock(key: JobMetadataKey<ByteBuffer>, id: ByteBuffer, expiresIn: TimeInterval) async throws -> Bool {
        try await self.acquireLock(key: key.name, id: id, expiresIn: expiresIn)
    }

    /// Release metadata lock
    ///
    /// - Parameters:
    ///   - key: Metadata key
    ///   - id: Lock identifier
    public func releaseLock(key: JobMetadataKey<ByteBuffer>, id: ByteBuffer) async throws {
        try await self.releaseLock(key: key.name, id: id)
    }
}

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
import NIOFoundationCompat

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Utility for deterministic hashing that produces stable results across app restarts
///
/// Unlike Swift's default `hashValue` which is intentionally non-deterministic for security reasons,
/// this utility provides consistent hash values that remain the same across application restarts.
/// This is essential for scenarios requiring stable hashing such as:
/// - Job queue fairness tie-breaking
/// - Unique job identification
/// - Sticky sessions or consistent routing
/// - Distributed system coordination
public struct DeterministicHasher {

    /// Deterministic hash function based on the FNV-1a algorithm
    ///
    /// This function provides consistent hash values across app restarts and is suitable
    /// for non-cryptographic purposes where deterministic behavior is required.
    ///
    /// ## Algorithm Details
    /// Uses the FNV-1a hash algorithm (Fowler-Noll-Vo, 1991) which is:
    /// - Fast and efficient
    /// - Excellent distribution and avalanche properties
    /// - 64-bit output drastically reduces collision probability
    /// - Deterministic across platforms and runs
    /// - Not cryptographically secure (use CryptoKit for security-sensitive hashing)
    ///
    /// ## Examples
    /// ```swift
    /// let hash1 = DeterministicHasher.hash("user-123")  // Always returns same value
    /// let hash2 = DeterministicHasher.hash("user-456")  // Different from hash1
    /// ```
    ///
    /// - Parameter string: The string to hash
    /// - Returns: A deterministic hash value that remains consistent across runs
    public static func hash(_ string: String) -> UInt64 {
        hash(initialHash: 14_695_981_039_346_656_037, string)
    }

    /// Hash a string with an initial hash value for composable hashing
    ///
    /// - Parameters:
    ///   - initialHash: Initial hash value to start from
    ///   - string: The string to hash
    /// - Returns: A deterministic hash value that remains consistent across runs
    public static func hash(initialHash: UInt64, _ string: String) -> UInt64 {
        // FNV-1a 64-bit constants
        let fnvPrime: UInt64 = 1_099_511_628_211

        var hash = initialHash
        for byte in string.utf8 {
            hash ^= UInt64(byte)
            hash = hash &* fnvPrime
        }
        return hash
    }

    /// Hash a collection of bytes deterministically
    ///
    /// Uses the FNV-1a hash algorithm to generate deterministic hash values
    /// from binary data. This allows working directly with encoded data
    /// without string conversion assumptions.
    ///
    /// - Parameter bytes: The bytes to hash
    /// - Returns: A deterministic hash value that remains consistent across runs
    public static func hashBytes<Bytes: Collection>(_ bytes: Bytes) -> UInt64 where Bytes.Element == UInt8 {
        hashBytes(initialHash: 14_695_981_039_346_656_037, bytes)
    }

    /// Hash a collection of bytes with an initial hash value for composable hashing
    ///
    /// - Parameters:
    ///   - initialHash: Initial hash value to start from
    ///   - bytes: The bytes to hash
    /// - Returns: A deterministic hash value that remains consistent across runs
    public static func hashBytes<Bytes: Collection>(initialHash: UInt64, _ bytes: Bytes) -> UInt64 where Bytes.Element == UInt8 {
        // FNV-1a 64-bit constants
        let fnvPrime: UInt64 = 1_099_511_628_211

        var hash = initialHash
        for byte in bytes {
            hash ^= UInt64(byte)
            hash = hash &* fnvPrime
        }
        return hash
    }

    /// Hash exactly three byte sequences efficiently (specialized version for better performance)
    ///
    /// Optimized version for the common case of hashing exactly three sequences
    /// (jobName + separator + parameters) without type erasure overhead.
    ///
    /// - Parameters:
    ///   - seq1: First byte sequence
    ///   - seq2: Second byte sequence
    ///   - seq3: Third byte sequence
    /// - Returns: A deterministic hash value that remains consistent across runs
    private static func hashThreeSequences<S1: Collection, S2: Collection, S3: Collection>(
        _ seq1: S1,
        _ seq2: S2,
        _ seq3: S3
    ) -> UInt64
    where S1.Element == UInt8, S2.Element == UInt8, S3.Element == UInt8 {
        hashThreeSequences(initialHash: 14_695_981_039_346_656_037, seq1, seq2, seq3)
    }

    /// Hash exactly three byte sequences with an initial hash value for composable hashing
    ///
    /// - Parameters:
    ///   - initialHash: Initial hash value to start from
    ///   - seq1: First byte sequence
    ///   - seq2: Second byte sequence
    ///   - seq3: Third byte sequence
    /// - Returns: A deterministic hash value that remains consistent across runs
    private static func hashThreeSequences<S1: Collection, S2: Collection, S3: Collection>(
        initialHash: UInt64,
        _ seq1: S1,
        _ seq2: S2,
        _ seq3: S3
    ) -> UInt64
    where S1.Element == UInt8, S2.Element == UInt8, S3.Element == UInt8 {
        // FNV-1a 64-bit constants
        let fnvPrime: UInt64 = 1_099_511_628_211

        var hash = initialHash

        // Hash all sequences in order without intermediate allocations
        for byte in seq1 {
            hash ^= UInt64(byte)
            hash = hash &* fnvPrime
        }
        for byte in seq2 {
            hash ^= UInt64(byte)
            hash = hash &* fnvPrime
        }
        for byte in seq3 {
            hash ^= UInt64(byte)
            hash = hash &* fnvPrime
        }

        return hash
    }

    /// Hash a UUID string deterministically
    ///
    /// Convenience method for hashing UUID strings, commonly used for job IDs.
    ///
    /// - Parameter uuid: The UUID to hash
    /// - Returns: A deterministic hash value that remains consistent across runs
    public static func hash(_ uuid: UUID) -> UInt64 {
        hash(uuid.uuidString)
    }

    /// Hash arbitrary data deterministically
    ///
    /// Hashes the UTF-8 string representation of the data.
    ///
    /// - Parameter data: The data to hash
    /// - Returns: A deterministic hash value that remains consistent across runs
    public static func hash<T: CustomStringConvertible>(_ data: T) -> UInt64 {
        hash(data.description)
    }

    /// Generate a hash-based unique identifier for job deduplication
    ///
    /// Creates a deterministic hash from job parameters that can be used for
    /// unique job identification and deduplication across restarts.
    ///
    /// ## Usage
    /// ```swift
    /// let uniqueID = DeterministicHasher.uniqueJobHash(
    ///     jobName: "send-email",
    ///     parameters: ["userId": "123", "template": "welcome"]
    /// )
    /// ```
    ///
    /// - Parameters:
    ///   - jobName: The name/type of the job
    ///   - parameters: Dictionary of job parameters
    /// - Returns: A deterministic hash suitable for unique job identification
    public static func uniqueJobHash(jobName: String, parameters: [String: Any]) -> String {
        // Sort parameters by key to ensure consistent ordering
        let sortedKeys = parameters.keys.sorted()
        var combinedString = jobName

        for key in sortedKeys {
            combinedString += "|\(key):\(parameters[key] ?? "")"
        }

        let hashValue = hash(combinedString)
        return "\(jobName)-\(hashValue)"
    }

    /// Generate a hash-based unique identifier for job deduplication (Codable version)
    ///
    /// Creates a deterministic hash from job parameters that can be used for
    /// unique job identification. This version works with Codable parameters.
    ///
    /// - Parameters:
    ///   - jobName: The name/type of the job
    ///   - parameters: Codable job parameters
    /// - Returns: A deterministic hash suitable for unique job identification
    /// - Throws: EncodingError if parameters cannot be encoded
    public static func uniqueJobHash<T: Codable>(jobName: String, parameters: T) throws -> String {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]  // Ensure consistent key ordering
        let parametersBuffer = try encoder.encodeAsByteBuffer(parameters, allocator: ByteBufferAllocator())

        // Efficiently hash three byte sequences without intermediate allocations
        let hashValue = hashThreeSequences(
            jobName.utf8,
            "|".utf8,
            parametersBuffer.readableBytesView
        )
        return "\(jobName)-\(hashValue)"
    }

    /// Generate a hash-based unique identifier for JobParameters
    ///
    /// Creates a deterministic hash from JobParameters that can be used for
    /// unique job identification. This method automatically extracts the job name
    /// from the JobParameters type.
    ///
    /// ## Usage
    /// ```swift
    /// struct EmailJob: JobParameters {
    ///     static let jobName = "send-email"
    ///     let userId: String
    ///     let template: String
    /// }
    ///
    /// let emailParams = EmailJob(userId: "123", template: "welcome")
    /// let uniqueID = try DeterministicHasher.uniqueJobHash(parameters: emailParams)
    /// // Returns: "send-email-<hash>"
    /// ```
    ///
    /// - Parameter parameters: JobParameters instance to hash
    /// - Returns: A deterministic hash suitable for unique job identification
    /// - Throws: EncodingError if parameters cannot be encoded
    public static func uniqueJobHash<T: JobParameters>(parameters: T) throws -> String {
        try uniqueJobHash(jobName: T.jobName, parameters: parameters)
    }
}

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

    /// Simple deterministic hash function based on the djb2 algorithm
    ///
    /// This function provides consistent hash values across app restarts and is suitable
    /// for non-cryptographic purposes where deterministic behavior is required.
    ///
    /// ## Algorithm Details
    /// Uses the djb2 hash algorithm (Dan J. Bernstein, 1991) which is:
    /// - Fast and simple
    /// - Good distribution for typical string inputs
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
    /// - Returns: A positive integer hash value that remains consistent across runs
    public static func hash(_ string: String) -> Int {
        var hash: UInt32 = 5381
        for byte in string.utf8 {
            hash = ((hash << 5) &+ hash) &+ UInt32(byte)
        }
        return Int(hash & 0x7FFF_FFFF)  // Ensure positive result
    }

    /// Hash a UUID string deterministically
    ///
    /// Convenience method for hashing UUID strings, commonly used for job IDs.
    ///
    /// - Parameter uuid: The UUID to hash
    /// - Returns: A positive integer hash value that remains consistent across runs
    public static func hash(_ uuid: UUID) -> Int {
        hash(uuid.uuidString)
    }

    /// Hash arbitrary data deterministically
    ///
    /// Hashes the UTF-8 string representation of the data.
    ///
    /// - Parameter data: The data to hash
    /// - Returns: A positive integer hash value that remains consistent across runs
    public static func hash<T: CustomStringConvertible>(_ data: T) -> Int {
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
        let data = try encoder.encode(parameters)
        let paramString = String(data: data, encoding: .utf8) ?? ""

        let combinedString = "\(jobName)|\(paramString)"
        let hashValue = hash(combinedString)
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

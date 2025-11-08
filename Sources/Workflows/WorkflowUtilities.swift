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

/// Utility functions for workflow operations
package struct WorkflowUtilities {

    /// Create a deterministic hash for replay safety
    /// This replaces Swift's Hasher which is non-deterministic across restarts
    /// Uses FNV-1a hash algorithm for deterministic hashing
    ///
    /// - Parameters:
    ///   - buffer: The data to hash
    ///   - activityName: Additional context string to include in hash
    /// - Returns: A deterministic 8-character hexadecimal hash string
    package static func deterministicHash(buffer: ByteBuffer, activityName: String) -> String {
        var hash: UInt64 = 14_695_981_039_346_656_037  // FNV-1a offset basis
        let prime: UInt64 = 1_099_511_628_211  // FNV-1a prime

        // Hash the buffer bytes
        let bytes = buffer.readableBytesView
        for byte in bytes {
            hash ^= UInt64(byte)
            hash = hash &* prime
        }

        // Hash the activity name
        let activityNameData = activityName.data(using: .utf8) ?? Data()
        for byte in activityNameData {
            hash ^= UInt64(byte)
            hash = hash &* prime
        }

        // Return first 8 characters as hex
        return String(format: "%08x", hash & 0xFFFF_FFFF)
    }

    /// Create a deterministic hash from string data
    /// Convenience method for string inputs
    ///
    /// - Parameters:
    ///   - string: The string to hash
    ///   - context: Additional context string to include in hash
    /// - Returns: A deterministic 8-character hexadecimal hash string
    package static func deterministicHash(string: String, context: String = "", allocator: ByteBufferAllocator = ByteBufferAllocator()) -> String {
        let buffer = allocator.buffer(string: string)
        return deterministicHash(buffer: buffer, activityName: context)
    }

    /// Generate a deterministic ID for workflows or activities
    /// Combines workflow ID, type name, and input hash for uniqueness
    ///
    /// - Parameters:
    ///   - workflowId: The parent workflow ID
    ///   - typeName: The workflow or activity type name
    ///   - inputBuffer: Serialized input data
    ///   - prefix: Optional prefix for the ID
    /// - Returns: A deterministic unique ID string
    internal static func generateDeterministicId(
        workflowId: WorkflowID,
        typeName: String,
        inputBuffer: ByteBuffer,
        prefix: String? = nil
    ) -> String {
        let inputHash = deterministicHash(buffer: inputBuffer, activityName: typeName)
        let components = [
            workflowId.runId,
            prefix,
            typeName,
            inputHash,
        ].compactMap { $0 }

        return components.joined(separator: ":")
    }

    /// Format duration in a human-readable way
    /// Useful for logging and monitoring
    ///
    /// - Parameter duration: The duration to format
    /// - Returns: A formatted string (e.g., "1.5s", "2m 30s", "1h 15m")
    internal static func formatDuration(_ duration: TimeInterval) -> String {
        if duration < 1 {
            return String(format: "%.0fms", duration * 1000)
        } else if duration < 60 {
            return String(format: "%.1fs", duration)
        } else if duration < 3600 {
            let minutes = Int(duration / 60)
            let seconds = Int(duration.truncatingRemainder(dividingBy: 60))
            if seconds == 0 {
                return "\(minutes)m"
            }
            return "\(minutes)m \(seconds)s"
        } else {
            let hours = Int(duration / 3600)
            let minutes = Int((duration.truncatingRemainder(dividingBy: 3600)) / 60)
            if minutes == 0 {
                return "\(hours)h"
            }
            return "\(hours)h \(minutes)m"
        }
    }

    /// Validate workflow or activity name
    /// Ensures names follow conventions and don't contain problematic characters
    ///
    /// - Parameter name: The name to validate
    /// - Returns: True if valid, false otherwise
    internal static func isValidWorkflowName(_ name: String) -> Bool {
        // Check basic requirements
        guard !name.isEmpty && name.count <= 100 else { return false }

        // Must start with letter or underscore
        guard let firstChar = name.first,
            firstChar.isLetter || firstChar == "_"
        else { return false }

        // Can only contain letters, numbers, underscores, and hyphens
        let allowedCharacters = CharacterSet.alphanumerics.union(CharacterSet(charactersIn: "_-"))
        return name.unicodeScalars.allSatisfy { allowedCharacters.contains($0) }
    }

    /// Create a safe workflow name from a potentially unsafe string
    /// Sanitizes input to create a valid workflow name
    ///
    /// - Parameter input: The input string to sanitize
    /// - Returns: A safe workflow name
    internal static func sanitizeWorkflowName(_ input: String) -> String {
        // Remove or replace invalid characters
        let sanitized =
            input
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .replacingOccurrences(of: " ", with: "_")
            .replacingOccurrences(of: ".", with: "_")
            .components(separatedBy: CharacterSet.alphanumerics.union(CharacterSet(charactersIn: "_-")).inverted)
            .joined()

        // Ensure it starts with a valid character
        let result = sanitized.isEmpty ? "workflow" : sanitized
        if let firstChar = result.first, !firstChar.isLetter && firstChar != "_" {
            return "_" + result
        }

        // Limit length
        return String(result.prefix(100))
    }

    /// Extract workflow type from a full workflow name
    /// Useful for parsing composite workflow names
    ///
    /// - Parameter fullName: The full workflow name (e.g., "MyWorkflow_v2_prod")
    /// - Returns: The base type name (e.g., "MyWorkflow")
    internal static func extractWorkflowType(from fullName: String) -> String {
        // Split on common delimiters and take the first part
        let delimiters = CharacterSet(charactersIn: "_-.")
        return fullName.components(separatedBy: delimiters).first ?? fullName
    }

    /// Merge metadata dictionaries with conflict resolution
    /// Useful for combining workflow and activity metadata
    ///
    /// - Parameters:
    ///   - base: The base metadata dictionary
    ///   - overlay: The overlay metadata dictionary
    ///   - conflictResolution: How to handle conflicts (.keepBase, .keepOverlay, .merge)
    /// - Returns: The merged metadata dictionary
    package static func mergeMetadata(
        base: [String: String],
        overlay: [String: String],
        conflictResolution: MetadataConflictResolution = .keepOverlay
    ) -> [String: String] {
        var result = base

        for (key, value) in overlay {
            switch conflictResolution {
            case .keepBase:
                if result[key] == nil {
                    result[key] = value
                }
            case .keepOverlay:
                result[key] = value
            case .merge:
                if let existing = result[key] {
                    result[key] = "\(existing),\(value)"
                } else {
                    result[key] = value
                }
            }
        }

        return result
    }
}

// MARK: - Duration Convenience Extensions

extension Duration {
    /// Create a Duration from minutes
    /// - Parameter minutes: Number of minutes
    /// - Returns: Duration representing the specified minutes
    public static func minutes(_ minutes: Int) -> Duration {
        .seconds(minutes * 60)
    }

    /// Create a Duration from minutes (Double)
    /// - Parameter minutes: Number of minutes
    /// - Returns: Duration representing the specified minutes
    public static func minutes(_ minutes: Double) -> Duration {
        .seconds(Int(minutes * 60))
    }

    /// Create a Duration from hours
    /// - Parameter hours: Number of hours
    /// - Returns: Duration representing the specified hours
    public static func hours(_ hours: Int) -> Duration {
        .seconds(hours * 3600)
    }

    /// Create a Duration from hours (Double)
    /// - Parameter hours: Number of hours
    /// - Returns: Duration representing the specified hours
    public static func hours(_ hours: Double) -> Duration {
        .seconds(Int(hours * 3600))
    }

    /// Create a Duration from days
    /// - Parameter days: Number of days
    /// - Returns: Duration representing the specified days
    public static func days(_ days: Int) -> Duration {
        .seconds(days * 86400)
    }

    /// Create a Duration from days (Double)
    /// - Parameter days: Number of days
    /// - Returns: Duration representing the specified days
    public static func days(_ days: Double) -> Duration {
        .seconds(Int(days * 86400))
    }

    /// Create a Duration from weeks
    /// - Parameter weeks: Number of weeks
    /// - Returns: Duration representing the specified weeks
    public static func weeks(_ weeks: Int) -> Duration {
        .seconds(weeks * 604800)
    }

    /// Create a Duration from weeks (Double)
    /// - Parameter weeks: Number of weeks
    /// - Returns: Duration representing the specified weeks
    public static func weeks(_ weeks: Double) -> Duration {
        .seconds(Int(weeks * 604800))
    }

    /// Get the time interval in seconds as a Double
    var timeInterval: TimeInterval {
        Double(self.components.seconds) + Double(self.components.attoseconds) / 1_000_000_000_000_000_000
    }

    /// Get nanoseconds as UInt64
    var nanoseconds: UInt64 {
        UInt64(self.components.seconds) * 1_000_000_000 + UInt64(self.components.attoseconds / 1_000_000_000)
    }
}

/// How to resolve conflicts when merging metadata
package enum MetadataConflictResolution {
    /// Keep the value from the base dictionary
    case keepBase
    /// Keep the value from the overlay dictionary
    case keepOverlay
    /// Merge values with comma separation
    case merge
}

/// ISO 8601 date formatting utilities
extension Date {
    /// Format date as ISO 8601 string with fractional seconds
    public var iso8601StringWithFractionalSeconds: String {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter.string(from: self)
    }

    /// Format date as ISO 8601 string without fractional seconds
    public var iso8601String: String {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime]
        return formatter.string(from: self)
    }
}

/// ByteBuffer utilities for workflows
extension ByteBuffer {
    /// Get the string representation of the buffer
    public var stringValue: String? {
        self.getString(at: 0, length: self.readableBytes)
    }
}

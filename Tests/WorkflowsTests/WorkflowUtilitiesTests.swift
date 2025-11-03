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

import Foundation
import NIOCore
import Testing
import Workflows

@Suite("WorkflowUtilitiesTests")
struct WorkflowUtilitiesTests {

    @Test("WorkflowUtilities deterministic hash consistency")
    func testDeterministicHashConsistency() async throws {
        // Test that the same input always produces the same hash
        let testString = "test_input_123"
        let context = "activity_name"

        let hash1 = WorkflowUtilities.deterministicHash(string: testString, context: context)
        let hash2 = WorkflowUtilities.deterministicHash(string: testString, context: context)
        let hash3 = WorkflowUtilities.deterministicHash(string: testString, context: context)

        #expect(hash1 == hash2)
        #expect(hash2 == hash3)
        #expect(hash1.count == 8)  // Should be 8-character hex string
    }

    @Test("WorkflowUtilities deterministic hash uniqueness")
    func testDeterministicHashUniqueness() async throws {
        // Test that different inputs produce different hashes
        let testCases = [
            ("input1", "context1"),
            ("input1", "context2"),
            ("input2", "context1"),
            ("input2", "context2"),
            ("", "context"),
            ("input", ""),
            ("", ""),
            ("very_long_input_string_with_lots_of_content_12345", "long_context_name"),
        ]

        var hashes = Set<String>()

        for (input, context) in testCases {
            let hash = WorkflowUtilities.deterministicHash(string: input, context: context)

            // Each hash should be unique
            #expect(!hashes.contains(hash), "Hash collision detected for input: '\(input)', context: '\(context)'")

            // Hash should be valid 8-character hex string
            #expect(hash.count == 8)
            #expect(hash.allSatisfy { $0.isHexDigit })

            hashes.insert(hash)
        }
    }

    @Test("WorkflowUtilities ByteBuffer hash consistency")
    func testByteBufferHashConsistency() async throws {
        // Test that ByteBuffer and String versions produce same results
        let testString = "test_workflow_input_data"
        let context = "TestActivity"

        let stringHash = WorkflowUtilities.deterministicHash(string: testString, context: context)

        let buffer = ByteBufferAllocator().buffer(string: testString)
        let bufferHash = WorkflowUtilities.deterministicHash(buffer: buffer, activityName: context)

        #expect(stringHash == bufferHash)
    }

    @Test("WorkflowUtilities metadata merging")
    func testMetadataMerging() async throws {
        let base = ["key1": "value1", "key2": "value2", "shared": "base_value"]
        let overlay = ["key3": "value3", "key4": "value4", "shared": "overlay_value"]

        // Test keepOverlay (default)
        let merged1 = WorkflowUtilities.mergeMetadata(
            base: base,
            overlay: overlay,
            conflictResolution: .keepOverlay
        )
        #expect(merged1["key1"] == "value1")
        #expect(merged1["key2"] == "value2")
        #expect(merged1["key3"] == "value3")
        #expect(merged1["key4"] == "value4")
        #expect(merged1["shared"] == "overlay_value")

        // Test keepBase
        let merged2 = WorkflowUtilities.mergeMetadata(
            base: base,
            overlay: overlay,
            conflictResolution: .keepBase
        )
        #expect(merged2["shared"] == "base_value")

        // Test merge (concatenation)
        let merged3 = WorkflowUtilities.mergeMetadata(
            base: base,
            overlay: overlay,
            conflictResolution: .merge
        )
        #expect(merged3["shared"] == "base_value,overlay_value")
    }

    @Test("WorkflowUtilities hash performance and stability")
    func testHashPerformanceAndStability() async throws {
        // Test that hash function works with various edge cases
        let edgeCases = [
            "",
            " ",
            "\n",
            "\t",
            "🎯",
            "简体中文",
            String(repeating: "a", count: 10000),  // Large string
            String(repeating: "🚀", count: 1000),  // Large unicode string
        ]

        for testCase in edgeCases {
            let hash = WorkflowUtilities.deterministicHash(string: testCase, context: "test")

            // Should produce valid hash for all inputs
            #expect(hash.count == 8)
            #expect(hash.allSatisfy { $0.isHexDigit })

            // Should be consistent across calls
            let hash2 = WorkflowUtilities.deterministicHash(string: testCase, context: "test")
            #expect(hash == hash2)
        }
    }
}

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
import Testing

@testable import Jobs

#if canImport(FoundationEssentials)
import FoundationEssentials
#endif

struct DeterministicHasherTests {

    // MARK: - Test Job Parameters

    struct EmailJob: JobParameters {
        static let jobName = "send-email"
        let userId: String
        let template: String
        let priority: Int?
    }

    struct NotificationJob: JobParameters {
        static let jobName = "send-notification"
        let recipientId: String
        let message: String
        let channel: String
    }

    struct DataProcessingJob: JobParameters {
        static let jobName = "process-data"
        let batchId: UUID
        let filters: [String: String]
        let timestamp: Date
    }

    // MARK: - Basic Hash Function Tests

    @Test func testHashConsistency() {
        let testInput = "test-string-12345"
        let hash1 = DeterministicHasher.hash(testInput)
        let hash2 = DeterministicHasher.hash(testInput)

        #expect(hash1 == hash2, "Same input should produce same hash")
        #expect(hash1 >= 0, "Hash should be positive")
    }

    @Test func testHashDistribution() {
        let inputs = [
            "input-1",
            "input-2",
            "input-3",
            "different-string",
            "another-test-value",
            "uuid-like-string-12345-abcdef",
        ]

        let hashes = inputs.map { DeterministicHasher.hash($0) }
        let uniqueHashes = Set(hashes)

        // All hashes should be different (very likely with good hash function)
        #expect(uniqueHashes.count == hashes.count, "Different inputs should produce different hashes")

        // All hashes should be positive
        for hash in hashes {
            #expect(hash >= 0, "All hashes should be positive")
        }
    }

    @Test func testHashEmptyString() {
        let hash = DeterministicHasher.hash("")
        #expect(hash >= 0, "Empty string hash should be positive")
        #expect(hash == DeterministicHasher.hash(""), "Empty string should hash consistently")
    }

    @Test func testHashUnicodeStrings() {
        let unicodeInputs = [
            "Hello 世界",
            "café",
            "🚀 rocket emoji",
            "مرحبا بالعالم",
            "Здравствуй мир",
        ]

        for input in unicodeInputs {
            let hash1 = DeterministicHasher.hash(input)
            let hash2 = DeterministicHasher.hash(input)

            #expect(hash1 == hash2, "Unicode string '\(input)' should hash consistently")
            #expect(hash1 >= 0, "Unicode string hash should be positive")
        }
    }

    // MARK: - UUID Hash Tests

    @Test func testHashUUID() {
        let uuid = UUID()
        let hash1 = DeterministicHasher.hash(uuid)
        let hash2 = DeterministicHasher.hash(uuid)

        #expect(hash1 == hash2, "Same UUID should produce same hash")
        #expect(hash1 >= 0, "UUID hash should be positive")

        // Test with UUID string directly
        let stringHash = DeterministicHasher.hash(uuid.uuidString)
        #expect(hash1 == stringHash, "UUID and UUID string should produce same hash")
    }

    @Test func testHashDifferentUUIDs() {
        let uuid1 = UUID()
        let uuid2 = UUID()

        let hash1 = DeterministicHasher.hash(uuid1)
        let hash2 = DeterministicHasher.hash(uuid2)

        #expect(hash1 != hash2, "Different UUIDs should produce different hashes")
    }

    // MARK: - Generic Hash Tests

    @Test func testHashCustomStringConvertible() {
        struct TestStruct: CustomStringConvertible {
            let value: String
            var description: String { value }
        }

        let test1 = TestStruct(value: "test-value")
        let test2 = TestStruct(value: "test-value")
        let test3 = TestStruct(value: "different-value")

        let hash1 = DeterministicHasher.hash(test1)
        let hash2 = DeterministicHasher.hash(test2)
        let hash3 = DeterministicHasher.hash(test3)

        #expect(hash1 == hash2, "Same description should produce same hash")
        #expect(hash1 != hash3, "Different descriptions should produce different hashes")
    }

    // MARK: - Unique Job Hash Tests (Dictionary Version)

    @Test func testUniqueJobHashDictionary() {
        let parameters1: [String: Any] = [
            "userId": "123",
            "template": "welcome",
        ]

        let parameters2: [String: Any] = [
            "template": "welcome",
            "userId": "123",  // Different order
        ]

        let parameters3: [String: Any] = [
            "userId": "456",
            "template": "welcome",
        ]

        let hash1 = DeterministicHasher.uniqueJobHash(jobName: "send-email", parameters: parameters1)
        let hash2 = DeterministicHasher.uniqueJobHash(jobName: "send-email", parameters: parameters2)
        let hash3 = DeterministicHasher.uniqueJobHash(jobName: "send-email", parameters: parameters3)

        #expect(hash1 == hash2, "Same parameters in different order should produce same hash")
        #expect(hash1 != hash3, "Different parameters should produce different hashes")
        #expect(hash1.hasPrefix("send-email-"), "Hash should start with job name")
    }

    @Test func testUniqueJobHashEmptyParameters() {
        let emptyParams: [String: Any] = [:]
        let hash1 = DeterministicHasher.uniqueJobHash(jobName: "empty-job", parameters: emptyParams)
        let hash2 = DeterministicHasher.uniqueJobHash(jobName: "empty-job", parameters: emptyParams)

        #expect(hash1 == hash2, "Empty parameters should hash consistently")
        #expect(hash1.hasPrefix("empty-job-"), "Hash should start with job name")
    }

    // MARK: - Unique Job Hash Tests (Codable Version)

    @Test func testUniqueJobHashCodable() throws {
        let emailJob1 = EmailJob(userId: "123", template: "welcome", priority: 5)
        let emailJob2 = EmailJob(userId: "123", template: "welcome", priority: 5)
        let emailJob3 = EmailJob(userId: "456", template: "welcome", priority: 5)

        let hash1 = try DeterministicHasher.uniqueJobHash(jobName: "send-email", parameters: emailJob1)
        let hash2 = try DeterministicHasher.uniqueJobHash(jobName: "send-email", parameters: emailJob2)
        let hash3 = try DeterministicHasher.uniqueJobHash(jobName: "send-email", parameters: emailJob3)

        #expect(hash1 == hash2, "Same parameters should produce same hash")
        #expect(hash1 != hash3, "Different parameters should produce different hashes")
        #expect(hash1.hasPrefix("send-email-"), "Hash should start with job name")
    }

    @Test func testUniqueJobHashWithOptionalFields() throws {
        let job1 = EmailJob(userId: "123", template: "welcome", priority: nil)
        let job2 = EmailJob(userId: "123", template: "welcome", priority: 5)

        let hash1 = try DeterministicHasher.uniqueJobHash(jobName: "send-email", parameters: job1)
        let hash2 = try DeterministicHasher.uniqueJobHash(jobName: "send-email", parameters: job2)

        #expect(hash1 != hash2, "Optional fields should affect hash")
    }

    @Test func testUniqueJobHashWithComplexTypes() throws {
        let batchId = UUID()
        let timestamp = Date(timeIntervalSince1970: 1_640_995_200)  // Fixed timestamp for consistency

        let job1 = DataProcessingJob(
            batchId: batchId,
            filters: ["status": "active", "region": "us-east"],
            timestamp: timestamp
        )

        let job2 = DataProcessingJob(
            batchId: batchId,
            filters: ["region": "us-east", "status": "active"],  // Different order
            timestamp: timestamp
        )

        let hash1 = try DeterministicHasher.uniqueJobHash(jobName: "process-data", parameters: job1)
        let hash2 = try DeterministicHasher.uniqueJobHash(jobName: "process-data", parameters: job2)

        #expect(hash1 == hash2, "Same parameters should produce same hash regardless of dictionary order")
    }

    // MARK: - JobParameters Integration Tests

    @Test func testUniqueJobHashWithJobParameters() throws {
        let emailJob = EmailJob(userId: "123", template: "welcome", priority: 5)
        let notificationJob = NotificationJob(recipientId: "123", message: "Hello", channel: "email")

        let emailHash = try DeterministicHasher.uniqueJobHash(parameters: emailJob)
        let notificationHash = try DeterministicHasher.uniqueJobHash(parameters: notificationJob)

        #expect(emailHash.hasPrefix("send-email-"), "Email job hash should start with job name")
        #expect(notificationHash.hasPrefix("send-notification-"), "Notification job hash should start with job name")
        #expect(emailHash != notificationHash, "Different job types should produce different hashes")
    }

    @Test func testJobParametersConsistency() throws {
        let job = EmailJob(userId: "123", template: "welcome", priority: 5)

        let hash1 = try DeterministicHasher.uniqueJobHash(parameters: job)
        let hash2 = try DeterministicHasher.uniqueJobHash(parameters: job)

        #expect(hash1 == hash2, "Same JobParameters should produce consistent hash")
    }

    @Test func testJobParametersVsManualHash() throws {
        let job = EmailJob(userId: "123", template: "welcome", priority: 5)

        let autoHash = try DeterministicHasher.uniqueJobHash(parameters: job)
        let manualHash = try DeterministicHasher.uniqueJobHash(jobName: EmailJob.jobName, parameters: job)

        #expect(autoHash == manualHash, "JobParameters method should produce same result as manual method")
    }

    // MARK: - Edge Cases and Error Handling

    @Test func testLongStrings() {
        let longString = String(repeating: "a", count: 10000)
        let hash1 = DeterministicHasher.hash(longString)
        let hash2 = DeterministicHasher.hash(longString)

        #expect(hash1 == hash2, "Long strings should hash consistently")
        #expect(hash1 >= 0, "Long string hash should be positive")
    }

    @Test func testSpecialCharacters() {
        let specialChars = "!@#$%^&*()_+-=[]{}|;':\",./<>?"
        let hash1 = DeterministicHasher.hash(specialChars)
        let hash2 = DeterministicHasher.hash(specialChars)

        #expect(hash1 == hash2, "Special characters should hash consistently")
        #expect(hash1 >= 0, "Special characters hash should be positive")
    }

    @Test func testHashBounds() {
        // Test that hash values are within expected bounds (positive 31-bit integers)
        let testInputs = [
            "short",
            "medium-length-string",
            "very-long-string-that-contains-lots-of-characters-and-should-still-hash-correctly",
            "with-numbers-123456789",
            "with-symbols-!@#$%^&*()",
        ]

        for input in testInputs {
            let hash = DeterministicHasher.hash(input)
            #expect(hash >= 0, "Hash should be non-negative")
            #expect(hash <= Int.max, "Hash should be within Int range")
            #expect(hash <= 0x7FFF_FFFF, "Hash should fit in positive 31-bit range")
        }
    }

    // MARK: - Performance and Collision Tests

    @Test func testHashCollisionResistance() {
        // Generate many similar strings and check for collisions
        var hashes: Set<Int> = []
        let baseString = "test-job-"

        for i in 0..<1000 {
            let input = "\(baseString)\(i)"
            let hash = DeterministicHasher.hash(input)
            hashes.insert(hash)
        }

        // We should have very few or no collisions with a good hash function
        let collisionRate = Double(1000 - hashes.count) / 1000.0
        #expect(collisionRate < 0.01, "Collision rate should be less than 1%")
    }

    // MARK: - Cross-Platform Consistency Tests

    @Test func testKnownHashValues() {
        // Test against known hash values to ensure consistency across platforms/versions
        struct KnownHash {
            let input: String
            let expectedHash: Int
        }

        let knownHashes = [
            KnownHash(input: "test", expectedHash: 2_090_756_197),
            KnownHash(input: "hello-world", expectedHash: 1_403_312_366),
            KnownHash(input: "swift-jobs", expectedHash: 1_358_617_293),
            KnownHash(input: "", expectedHash: 5381),
        ]

        for known in knownHashes {
            let actualHash = DeterministicHasher.hash(known.input)
            #expect(
                actualHash == known.expectedHash,
                "Hash for '\(known.input)' should be \(known.expectedHash) but got \(actualHash)"
            )
        }
    }
}

// MARK: - Integration Tests

struct DeterministicHasherIntegrationTests {

    @Test func testUniqueJobScenario() throws {
        // Simulate a real-world scenario with unique job identification
        struct UserRegistrationJob: JobParameters {
            static let jobName = "user-registration"
            let email: String
            let referralCode: String?
            let registrationTime: Date
        }

        let fixedDate = Date(timeIntervalSince1970: 1_640_995_200)

        let job1 = UserRegistrationJob(
            email: "user@example.com",
            referralCode: "FRIEND123",
            registrationTime: fixedDate
        )

        let job2 = UserRegistrationJob(
            email: "user@example.com",
            referralCode: "FRIEND123",
            registrationTime: fixedDate
        )

        let job3 = UserRegistrationJob(
            email: "different@example.com",
            referralCode: "FRIEND123",
            registrationTime: fixedDate
        )

        let hash1 = try DeterministicHasher.uniqueJobHash(parameters: job1)
        let hash2 = try DeterministicHasher.uniqueJobHash(parameters: job2)
        let hash3 = try DeterministicHasher.uniqueJobHash(parameters: job3)

        #expect(hash1 == hash2, "Duplicate jobs should have same hash for deduplication")
        #expect(hash1 != hash3, "Different jobs should have different hashes")

        // Verify the hash format
        #expect(hash1.hasPrefix("user-registration-"), "Hash should include job name prefix")
    }
}

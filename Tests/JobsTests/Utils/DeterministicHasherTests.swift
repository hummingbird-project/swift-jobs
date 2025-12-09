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
        }
    }

    // MARK: - UUID Hash Tests

    @Test func testHashUUID() {
        let uuid = UUID()
        let hash1 = DeterministicHasher.hash(uuid)
        let hash2 = DeterministicHasher.hash(uuid)

        #expect(hash1 == hash2, "Same UUID should produce same hash")

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
    }

    @Test func testSpecialCharacters() {
        let specialChars = "!@#$%^&*()_+-=[]{}|;':\",./<>?"
        let hash1 = DeterministicHasher.hash(specialChars)
        let hash2 = DeterministicHasher.hash(specialChars)

        #expect(hash1 == hash2, "Special characters should hash consistently")
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
            #expect(hash <= UInt.max, "Hash should be within UInt range")
        }

    }

    @Test func testByteBufferHashConsistency() throws {
        // Test that the ByteBuffer approach produces consistent results
        struct TestParams: Codable {
            let userId: String
            let value: Int
            let flag: Bool
        }

        let params = TestParams(userId: "test-123", value: 42, flag: true)

        // Generate hash using the new ByteBuffer approach
        let hash1 = try DeterministicHasher.uniqueJobHash(jobName: "TestJob", parameters: params)
        let hash2 = try DeterministicHasher.uniqueJobHash(jobName: "TestJob", parameters: params)

        // Should be identical
        #expect(hash1 == hash2)

        // Should start with job name
        #expect(hash1.hasPrefix("TestJob-"))

        // Different parameters should produce different hashes
        let params2 = TestParams(userId: "test-456", value: 42, flag: true)
        let hash3 = try DeterministicHasher.uniqueJobHash(jobName: "TestJob", parameters: params2)

        #expect(hash1 != hash3)
    }

    @Test func testBinaryDataHandling() throws {
        // Test that ByteBuffer approach handles binary data correctly without UTF-8 assumptions
        struct BinaryParams: Codable {
            let data: Data
        }

        // Create data that might not be valid UTF-8
        let binaryData = Data([0xFF, 0xFE, 0x00, 0x01, 0x80, 0x90])
        let params = BinaryParams(data: binaryData)

        // Should not crash or lose data
        let hash1 = try DeterministicHasher.uniqueJobHash(jobName: "BinaryJob", parameters: params)
        let hash2 = try DeterministicHasher.uniqueJobHash(jobName: "BinaryJob", parameters: params)

        #expect(hash1 == hash2)
        #expect(hash1.hasPrefix("BinaryJob-"))
    }

    @Test func testPerformanceComparison() throws {
        // Test with realistic JobParameters that mirror actual user patterns
        struct ProcessDocumentJob: JobParameters {
            static let jobName = "ProcessDocumentJob"
            let documentId: UUID
            let workspaceId: UUID
            let userId: String
            let processingOptions: ProcessingOptions
        }

        struct ProcessingOptions: Codable {
            let extractText: Bool
            let generateThumbnail: Bool
            let ocrLanguages: [String]
            let metadata: [String: String]
        }

        struct SendEmailJob: JobParameters {
            static let jobName = "SendEmailJob"
            let recipients: [String]
            let templateId: String
            let workspaceId: UUID
            let variables: [String: String]
            let scheduledFor: Date?
        }

        let documentJob = ProcessDocumentJob(
            documentId: UUID(),
            workspaceId: UUID(),
            userId: "user-12345",
            processingOptions: ProcessingOptions(
                extractText: true,
                generateThumbnail: true,
                ocrLanguages: ["en", "es", "fr"],
                metadata: [
                    "priority": "high",
                    "department": "engineering",
                    "project": "document-processor-v2",
                ]
            )
        )

        let emailJob = SendEmailJob(
            recipients: ["user1@example.com", "user2@example.com"],
            templateId: "welcome-email-template",
            workspaceId: UUID(),
            variables: [
                "userName": "John Doe",
                "companyName": "Acme Corp",
                "activationLink": "https://app.example.com/activate/abc123",
            ],
            scheduledFor: Date()
        )

        // Test hash consistency for realistic job parameters
        let hash1 = try DeterministicHasher.uniqueJobHash(parameters: documentJob)
        let hash2 = try DeterministicHasher.uniqueJobHash(parameters: documentJob)
        #expect(hash1 == hash2)
        #expect(hash1.hasPrefix("ProcessDocumentJob-"))

        let hash3 = try DeterministicHasher.uniqueJobHash(parameters: emailJob)
        let hash4 = try DeterministicHasher.uniqueJobHash(parameters: emailJob)
        #expect(hash3 == hash4)
        #expect(hash3.hasPrefix("SendEmailJob-"))

        // Different job types should produce different hashes
        #expect(hash1 != hash3)

        // Same job type with different parameters should produce different hashes
        let documentJob2 = ProcessDocumentJob(
            documentId: UUID(),  // Different UUID
            workspaceId: documentJob.workspaceId,
            userId: documentJob.userId,
            processingOptions: documentJob.processingOptions
        )
        let hash5 = try DeterministicHasher.uniqueJobHash(parameters: documentJob2)
        #expect(hash1 != hash5)
    }

    @Test func testRealisticWorkload() throws {
        // Test performance characteristics with typical job patterns that users create
        struct DataProcessingJob: JobParameters {
            static let jobName = "DataProcessingJob"
            let batchId: UUID
            let workspaceId: UUID
            let sourceFiles: [String]
            let config: ProcessingConfig
        }

        struct ProcessingConfig: Codable, Sendable {
            let format: String
            let compression: Bool
            let filters: [String: String]

            init(format: String, compression: Bool, filters: [String: String]) {
                self.format = format
                self.compression = compression
                self.filters = filters
            }
        }

        // Create realistic job instances that mirror production workloads
        let jobs = (0..<100).map { index in
            DataProcessingJob(
                batchId: UUID(),
                workspaceId: UUID(),
                sourceFiles: ["file\(index).csv", "data\(index).json"],
                config: ProcessingConfig(
                    format: "parquet",
                    compression: true,
                    filters: [
                        "startDate": "2024-01-\(String(format: "%02d", index % 28 + 1))",
                        "category": "batch_\(index)",
                        "priority": "\(index % 3)",
                    ]
                )
            )
        }

        // Test that all jobs produce unique, deterministic hashes
        var hashes = Set<String>()
        for job in jobs {
            let hash1 = try DeterministicHasher.uniqueJobHash(parameters: job)
            let hash2 = try DeterministicHasher.uniqueJobHash(parameters: job)

            // Hash should be deterministic
            #expect(hash1 == hash2)
            #expect(hash1.hasPrefix("DataProcessingJob-"))

            // Each job should have a unique hash
            #expect(!hashes.contains(hash1), "Duplicate hash found: \(hash1)")
            hashes.insert(hash1)
        }

        // Verify we got 100 unique hashes
        #expect(hashes.count == 100, "Expected 100 unique hashes, got \(hashes.count)")
    }

    @Test func testComposableHashing() {
        // Test the new composable hashing feature
        let initial = DeterministicHasher.hash("test")
        let composed = DeterministicHasher.hash(initialHash: initial, "this")

        // Composable hashing should produce the same result as concatenated string
        let directHash = DeterministicHasher.hash("testthis")
        #expect(composed == directHash)

        // Test composition with bytes
        let byteInitial = DeterministicHasher.hashBytes([0x01, 0x02, 0x03])
        let byteComposed = DeterministicHasher.hashBytes(initialHash: byteInitial, [0x04, 0x05])

        // Should be deterministic
        let byteComposed2 = DeterministicHasher.hashBytes(initialHash: byteInitial, [0x04, 0x05])
        #expect(byteComposed == byteComposed2)

        // Different sequences should produce different results
        let differentComposed = DeterministicHasher.hashBytes(initialHash: byteInitial, [0x04, 0x06])
        #expect(byteComposed != differentComposed)

        // Test that hash functions now return UInt64 instead of Int
        let hashResult: UInt64 = DeterministicHasher.hash("test")
        #expect(hashResult > 0)

        let uuidResult: UInt64 = DeterministicHasher.hash(UUID())
        #expect(uuidResult > 0)

        let customResult: UInt64 = DeterministicHasher.hash(42)
        #expect(customResult > 0)

        // Example of the improved composable API - no type conversions needed!
        var composableHash = DeterministicHasher.hash("user")
        composableHash = DeterministicHasher.hash(initialHash: composableHash, "123")
        composableHash = DeterministicHasher.hash(initialHash: composableHash, "session")

        let expectedHash = DeterministicHasher.hash("user123session")
        #expect(composableHash == expectedHash)
    }

    // MARK: - Performance and Collision Tests

    @Test func testHashCollisionResistance() {
        // Generate many similar strings and check for collisions
        var hashes: Set<UInt64> = []
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
            let expectedHash: UInt64
        }

        let knownHashes = [
            KnownHash(input: "test", expectedHash: 18_007_334_074_686_647_077),
            KnownHash(input: "hello-world", expectedHash: 12_777_936_878_424_084_750),
            KnownHash(input: "swift-jobs", expectedHash: 17_411_439_091_264_771_263),
            KnownHash(input: "", expectedHash: 14_695_981_039_346_656_037),
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

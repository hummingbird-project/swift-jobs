//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-jobs workflow system
//
// Copyright (c) 2025 the swift-jobs authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Foundation
import Jobs

/// Retry policy for workflow steps (activities, child workflows, signals, etc.)
///
/// This enum provides a Codable way to configure retry behavior for workflow steps
/// while maintaining compatibility with the underlying JobRetryStrategy system.
public enum StepRetryPolicy: Codable, Sendable {
    /// Never retry failed steps
    case dontRetry

    /// Retry with exponential backoff and jitter
    case exponentialJitter(maxAttempts: Int, maxBackoff: Duration = .seconds(120), minJitter: Double = -0.5, maxJitter: Double = 0.5)

    /// Simple retry with fixed attempt count (uses exponential jitter internally)
    case fixedAttempts(maxAttempts: Int)

    // MARK: - JobRetryStrategy Conversion

    /// Convert this policy to a JobRetryStrategy that respects non-retryable errors
    public var strategy: any JobRetryStrategy {
        let underlyingStrategy: any JobRetryStrategy

        switch self {
        case .dontRetry:
            underlyingStrategy = NeverRetryStrategy()

        case .exponentialJitter(let maxAttempts, let maxBackoff, let minJitter, let maxJitter):
            underlyingStrategy = ExponentialJitterJobRetryStrategy(
                maxAttempts: maxAttempts,
                maxBackoff: maxBackoff,
                minJitter: minJitter,
                maxJitter: maxJitter
            )

        case .fixedAttempts(let maxAttempts):
            underlyingStrategy = ExponentialJitterJobRetryStrategy(maxAttempts: maxAttempts)
        }

        // Wrap with workflow-aware behavior that respects ApplicationError.isNonRetryable
        return WorkflowAwareJobRetryStrategy(underlyingStrategy: underlyingStrategy)
    }

    // MARK: - Convenience Static Methods

    /// Don't retry steps on failure
    public static var never: StepRetryPolicy { .dontRetry }

    /// Default retry policy with reasonable settings
    public static var `default`: StepRetryPolicy { .exponentialJitter(maxAttempts: 4) }

    /// Aggressive retry policy for critical operations
    public static var aggressive: StepRetryPolicy { .exponentialJitter(maxAttempts: 10) }

    /// Conservative retry policy for non-critical operations
    public static var conservative: StepRetryPolicy { .exponentialJitter(maxAttempts: 2) }

    // MARK: - Codable Implementation

    private enum CodingKeys: String, CodingKey {
        case type, maxAttempts, maxBackoff, minJitter, maxJitter
    }

    private enum PolicyType: String, Codable {
        case dontRetry, exponentialJitter, fixedAttempts
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let type = try container.decode(PolicyType.self, forKey: .type)

        switch type {
        case .dontRetry:
            self = .dontRetry

        case .exponentialJitter:
            let maxAttempts = try container.decode(Int.self, forKey: .maxAttempts)
            let maxBackoff = try container.decodeIfPresent(Duration.self, forKey: .maxBackoff) ?? .seconds(120)
            let minJitter = try container.decodeIfPresent(Double.self, forKey: .minJitter) ?? -0.5
            let maxJitter = try container.decodeIfPresent(Double.self, forKey: .maxJitter) ?? 0.5
            self = .exponentialJitter(maxAttempts: maxAttempts, maxBackoff: maxBackoff, minJitter: minJitter, maxJitter: maxJitter)

        case .fixedAttempts:
            let maxAttempts = try container.decode(Int.self, forKey: .maxAttempts)
            self = .fixedAttempts(maxAttempts: maxAttempts)
        }
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)

        switch self {
        case .dontRetry:
            try container.encode(PolicyType.dontRetry, forKey: .type)

        case .exponentialJitter(let maxAttempts, let maxBackoff, let minJitter, let maxJitter):
            try container.encode(PolicyType.exponentialJitter, forKey: .type)
            try container.encode(maxAttempts, forKey: .maxAttempts)
            try container.encode(maxBackoff, forKey: .maxBackoff)
            try container.encode(minJitter, forKey: .minJitter)
            try container.encode(maxJitter, forKey: .maxJitter)

        case .fixedAttempts(let maxAttempts):
            try container.encode(PolicyType.fixedAttempts, forKey: .type)
            try container.encode(maxAttempts, forKey: .maxAttempts)
        }
    }
}

// MARK: - Workflow-Aware Retry Strategy

/// Retry strategy that combines user-configured retry policies with non-retryable error handling
///
/// This strategy first checks if an error is marked as non-retryable (like business logic errors),
/// and if so, fails immediately without retrying. For retryable errors, it delegates to the
/// user's configured retry strategy, ensuring their retry count and backoff preferences are respected.
public struct WorkflowAwareJobRetryStrategy: JobRetryStrategy {
    private let underlyingStrategy: any JobRetryStrategy

    public init(underlyingStrategy: any JobRetryStrategy) {
        self.underlyingStrategy = underlyingStrategy
    }

    public func shouldRetry(attempt: Int, error: Error) -> Bool {
        // Step 1: Check for non-retryable ApplicationError first
        if let appError = error as? ApplicationError, appError.isNonRetryable {
            return false  // Business logic errors should never retry
        }

        // Step 2: Check for specific WorkflowError cases that shouldn't retry
        if let workflowError = error as? WorkflowError {
            switch workflowError {
            case .validationFailed, .invalidInputType, .invalidActivityInput, .invalidActivityOutput:
                return false  // Validation errors are non-retryable
            case .unknownWorkflowType, .unknownActivity:
                return false  // Configuration errors are non-retryable
            case .workflowCancelled:
                return false  // Cancelled operations shouldn't retry
            default:
                break  // Other workflow errors can be retried based on user policy
            }
        }

        // Step 3: For retryable errors, use the user's configured retry strategy
        return underlyingStrategy.shouldRetry(attempt: attempt, error: error)
    }

    public func calculateBackoff(attempt: Int) -> Duration {
        // Always use the user's backoff strategy
        underlyingStrategy.calculateBackoff(attempt: attempt)
    }
}

// MARK: - Custom Retry Strategies

/// Never retry failed jobs - Workflows module implementation
private struct NeverRetryStrategy: JobRetryStrategy {
    func shouldRetry(attempt: Int, error: Error) -> Bool {
        false
    }

    func calculateBackoff(attempt: Int) -> Duration {
        .seconds(0)
    }
}

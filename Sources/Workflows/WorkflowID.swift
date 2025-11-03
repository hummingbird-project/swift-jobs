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

/// Unique identifier for a workflow execution
/// Combines workflowId (business identifier) with runId (execution identifier)
public struct WorkflowID: Sendable, Hashable, CustomStringConvertible, Codable {
    /// Business workflow identifier (user-provided or generated)
    public let workflowId: String
    /// Unique execution run identifier
    public let runId: String

    /// Combined unique identifier string
    public var value: String { "\(workflowId):\(runId)" }

    /// Create a new unique workflow ID with generated IDs
    public init() {
        self.workflowId = UUID().uuidString
        self.runId = UUID().uuidString
    }

    /// Create a workflow ID with a specific business ID and generated run ID
    /// - Parameter workflowId: The business workflow identifier
    public init(workflowId: String) {
        self.workflowId = workflowId
        self.runId = UUID().uuidString
    }

    /// Create a workflow ID with specific workflow and run IDs
    /// - Parameters:
    ///   - workflowId: The business workflow identifier
    ///   - runId: The execution run identifier
    internal init(workflowId: String, runId: String) {
        self.workflowId = workflowId
        self.runId = runId
    }

    /// Create a workflow ID from a combined value string
    /// - Parameter value: The combined workflow:run ID string
    public init(_ value: String) {
        let components = value.split(separator: ":", maxSplits: 1)
        if components.count == 2 {
            self.workflowId = String(components[0])
            self.runId = String(components[1])
        } else {
            // Fallback for single ID - treat as workflow ID with generated run ID
            self.workflowId = value
            self.runId = UUID().uuidString
        }
    }

    /// String representation of the workflow ID
    public var description: String { value }
}

extension WorkflowID: ExpressibleByStringLiteral {
    public init(stringLiteral value: String) {
        self.init(value)
    }
}

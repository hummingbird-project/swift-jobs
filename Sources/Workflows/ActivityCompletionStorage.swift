//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2026 the Hummingbird authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See hummingbird/CONTRIBUTORS.txt for the list of Hummingbird authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Jobs
import NIOCore
import Synchronization

/// Shared storage for activity completions
final class ActivityCompletionStorage: Sendable {
    private let completions: Mutex<[String: CheckedContinuation<Void, Error>]> = .init([:])

    func waitForActivity(activityId: String) async throws {
        // Wait for middleware notification that activity completed
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            completions.withLock { activities in
                activities[activityId] = continuation
            }
        }
    }

    func completeActivity(activityId: String) {
        completions.withLock { activities in
            if let continuation = activities.removeValue(forKey: activityId) {
                continuation.resume()
            }
        }
    }

    func failActivity(activityId: String, error: Error) {
        completions.withLock { activities in
            if let continuation = activities.removeValue(forKey: activityId) {
                continuation.resume(throwing: error)
            }
        }
    }
}

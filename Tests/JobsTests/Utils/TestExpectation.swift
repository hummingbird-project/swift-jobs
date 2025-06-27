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

/// Allows for asynchronous testing that code is run
struct TestExpectation {
    struct Timeout: Error, CustomStringConvertible {
        let waitingOn: String?

        var description: String {
            if let waitingOn {
                "Timed out waiting on \(waitingOn)"
            } else {
                "Timed out"
            }
        }
    }

    let stream: AsyncStream<Void>
    let cont: AsyncStream<Void>.Continuation

    init() {
        (self.stream, self.cont) = AsyncStream.makeStream()
    }

    func trigger() {
        cont.yield()
    }

    func wait(for: String? = nil, count: Int = 1, timeout: Duration = .seconds(10)) async throws {
        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                var iterator = self.stream.makeAsyncIterator()
                for _ in 0..<count {
                    await iterator.next()
                }
            }
            group.addTask {
                try await Task.sleep(for: timeout)
                throw Timeout(waitingOn: `for`)
            }

            try await group.next()
            group.cancelAll()
        }
    }
}

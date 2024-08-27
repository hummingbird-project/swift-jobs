//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2024 the Hummingbird authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See hummingbird/CONTRIBUTORS.txt for the list of Hummingbird authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Atomics
import NIOConcurrencyHelpers

class Trigger: @unchecked Sendable {
    typealias Suspension = CheckedContinuation<Void, Error>

    enum State {
        case waiting([Suspension])
        case completed
        case failed(Error)
    }

    let lock = NIOLock()
    var state: State = .waiting([])

    func waitUntilTriggered() async throws {
        self.lock.lock()
        switch self.state {
        case .waiting(var continuations):
            return try await withCheckedThrowingContinuation { cont in
                continuations.append(cont)
                self.state = .waiting(continuations)
                self.lock.unlock()
            }
        case .completed:
            self.lock.unlock()
            return
        case .failed(let error):
            self.lock.unlock()
            throw error
        }
    }

    func trigger() {
        self.lock.withLock {
            switch self.state {
            case .waiting(let continuations):
                for cont in continuations {
                    cont.resume()
                }
                self.state = .completed
            case .completed:
                break
            case .failed:
                preconditionFailure("Cannot set it has completed after having set it has failed")
            }
        }
    }

    func failed(_ error: Error) {
        self.lock.withLock {
            switch self.state {
            case .waiting(let continuations):
                for cont in continuations {
                    cont.resume(throwing: error)
                }
                self.state = .failed(error)
            case .completed:
                preconditionFailure("Cannot set it has failed after having set it has completed")
            case .failed(let error):
                self.state = .failed(error)
            }
        }
    }
}

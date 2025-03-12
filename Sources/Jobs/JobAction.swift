//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2021-2025 the Hummingbird authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See hummingbird/CONTRIBUTORS.txt for the list of Hummingbird authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

/// Option to perform on a job
public struct JobAction: Sendable, Equatable {

    enum _Action {
        case cancel
        case pause
        case resume
    }

    let rawValue: _Action
    /// Cancel action
    public static func cancel() -> JobAction {
        JobAction(rawValue: .cancel)
    }
    /// Pause Action
    public static func pause() -> JobAction {
        JobAction(rawValue: .pause)
    }
    /// Resume Action
    public static func resume() -> JobAction {
        JobAction(rawValue: .resume)
    }
}

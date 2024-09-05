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

import struct Foundation.Date

/// Job execution type
public struct JobExecutionOptions: ExpressibleByArrayLiteral, Sendable {
    /// Job execution options
    public struct Option: Sendable {
        internal enum _Option {
            case delayed(until: Date)
        }

        internal let raw: _Option
        /// Delay a job for later execution
        public static func delay(until date: Date) -> Option {
            Option(raw: .delayed(until: date))
        }
    }

    private let _options: [Option]

    public init(arrayLiteral options: Option...) {
        self._options = options
    }

    /// Options  a job was queued with
    public func options() -> [Option] {
        return self._options
    }

    /// Time for when a job should run
    public func delayedUntil() -> Date? {
        for option in self._options {
            switch option.raw {
            case .delayed(let date):
                return date
            }
        }
        return nil
    }
}

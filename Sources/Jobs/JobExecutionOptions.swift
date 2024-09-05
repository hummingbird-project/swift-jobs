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
            case unknown
        }

        internal let raw: _Option
        /// Delay a job for later execution
        public static func delay(until date: Date) -> Option {
            Option(raw: .delayed(until: date))
        }

        /// Default job option
        public static func unknown() -> Option {
            Option(raw: .unknown)
        }

        /// Check if a job is delayed or not
        public func isDelayed() -> Bool {
            switch self.raw {
            case .delayed:
                return true
            default:
                return false
            }
        }

        /// Default state for execution
        public func isUnknown() -> Bool {
            switch self.raw {
            case .unknown:
                return true
            default:
                return false
            }
        }

        /// Return time for execution
        public func delayedUntil() -> Date? {
            switch self.raw {
            case .delayed(until: let date):
                return date
            default:
                return nil
            }
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
        return self._options.first(where: {
            $0.isDelayed()
        })?.delayedUntil()
    }
}

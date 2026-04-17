//
// This source file is part of the Hummingbird server framework project
// Copyright (c) the Hummingbird authors
//
// See LICENSE.txt for license information
// SPDX-License-Identifier: Apache-2.0
//
#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

struct JobSleepError: Error {
    let until: Date
}

extension JobQueue {
    /// Quit job and retry after specified period
    ///
    /// Throws an error that is caught by the job queue processor.
    /// - Parameter delay: How long to wait before retrying job
    public static func sleep(for delay: Duration) throws {
        throw JobSleepError(until: Date.now._advanced(by: delay))
    }

    ///  Quit job and retry at specified date
    ///
    /// Throws an error that is caught by the job queue processor
    /// - Parameter date: How long to wait until before retrying job
    public static func sleep(until date: Date) throws {
        throw JobSleepError(until: date)
    }
}

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

import Foundation
import Metrics
import MetricsTestKit

final class TaskUniqueTestMetrics: MetricsFactory {
    @TaskLocal static var current: TestMetrics = .init()
    func withUnique<Value: Sendable>(
        _ operation: () async throws -> Value
    ) async throws -> Value {
        try await TaskUniqueTestMetrics.$current.withValue(TestMetrics()) {
            try await operation()
        }
    }

    func makeCounter(label: String, dimensions: [(String, String)]) -> any CoreMetrics.CounterHandler {
        TaskUniqueTestMetrics.current.makeCounter(label: label, dimensions: dimensions)
    }

    public func makeMeter(label: String, dimensions: [(String, String)]) -> MeterHandler {
        TaskUniqueTestMetrics.current.makeMeter(label: label, dimensions: dimensions)
    }

    func makeRecorder(label: String, dimensions: [(String, String)], aggregate: Bool) -> any CoreMetrics.RecorderHandler {
        TaskUniqueTestMetrics.current.makeRecorder(label: label, dimensions: dimensions, aggregate: aggregate)
    }

    func makeTimer(label: String, dimensions: [(String, String)]) -> any CoreMetrics.TimerHandler {
        TaskUniqueTestMetrics.current.makeTimer(label: label, dimensions: dimensions)
    }

    func destroyCounter(_ handler: any CoreMetrics.CounterHandler) {
        TaskUniqueTestMetrics.current.destroyCounter(handler)
    }

    func destroyMeter(_ handler: MeterHandler) {
        TaskUniqueTestMetrics.current.destroyMeter(handler)
    }

    func destroyRecorder(_ handler: any CoreMetrics.RecorderHandler) {
        TaskUniqueTestMetrics.current.destroyRecorder(handler)
    }

    func destroyTimer(_ handler: any CoreMetrics.TimerHandler) {
        TaskUniqueTestMetrics.current.destroyTimer(handler)
    }

    public var counters: [TestCounter] { TaskUniqueTestMetrics.current.counters }
    public var meters: [TestMeter] { TaskUniqueTestMetrics.current.meters }
    public var recorders: [TestRecorder] { TaskUniqueTestMetrics.current.recorders }
    public var timers: [TestTimer] { TaskUniqueTestMetrics.current.timers }
}

extension TestCounter {
    subscript(dimension: String) -> String? {
        self.dimensions.first { $0.0 == dimension }?.1
    }
}

extension TestMeter {
    subscript(dimension: String) -> String? {
        self.dimensions.first { $0.0 == dimension }?.1
    }
}

extension TestTimer {
    subscript(dimension: String) -> String? {
        self.dimensions.first { $0.0 == dimension }?.1
    }
}

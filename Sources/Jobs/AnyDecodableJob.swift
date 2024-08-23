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

/// Add codable support for decoding any JobInstance
internal struct AnyDecodableJob: DecodableWithUserInfoConfiguration, Sendable {
    typealias DecodingConfiguration = JobRegistry

    init(from decoder: Decoder, configuration register: DecodingConfiguration) throws {
        let container = try decoder.container(keyedBy: _JobCodingKey.self)
        let key = container.allKeys.first!
        let childDecoder = try container.superDecoder(forKey: key)
        self.job = try register.decode(jobName: key.stringValue, from: childDecoder)
    }

    /// Job data
    let job: any JobInstanceProtocol

    /// Initialize a queue job
    init(_ job: any JobInstanceProtocol) {
        self.job = job
    }

    private enum CodingKeys: String, CodingKey {
        case job
    }
}

internal struct _JobCodingKey: CodingKey {
    var stringValue: String
    var intValue: Int?

    init?(stringValue: String) {
        self.stringValue = stringValue
        self.intValue = nil
    }

    init?(intValue: Int) {
        self.stringValue = "\(intValue)"
        self.intValue = intValue
    }

    init(stringValue: String, intValue: Int?) {
        self.stringValue = stringValue
        self.intValue = intValue
    }

    internal init(index: Int) {
        self.stringValue = "Index \(index)"
        self.intValue = index
    }
}

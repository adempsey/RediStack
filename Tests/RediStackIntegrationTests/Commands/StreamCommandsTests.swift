//===----------------------------------------------------------------------===//
//
// This source file is part of the RediStack open source project
//
// Copyright (c) 2019 RediStack project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of RediStack project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

@testable import RediStack
import RediStackTestUtils
import XCTest

final class StreamCommandsTests: RediStackIntegrationTestCase {

    func test_xadd() throws {
        let entry_id = try connection.xadd(["foo": "bar"], to: #function).wait()
        XCTAssertNotNil(entry_id)
    }

    func test_xlen() throws {
        XCTAssertEqual(try connection.xlen(of: #function).wait(), 0)
        _ = try connection.xadd(["foo": "bar"], to: #function).wait()
        XCTAssertEqual(try connection.xlen(of: #function).wait(), 1)
        _ = try connection.xadd(["baz": "qux", "corge": "grault"], to: #function).wait()
        XCTAssertEqual(try connection.xlen(of: #function).wait(), 2)
    }

    func test_xdel() throws {
        let entry_id = try connection.xadd(["foo": "bar"], to: #function).wait()
        XCTAssertEqual(try connection.xlen(of: #function).wait(), 1)
        _ = try connection.xdel(entry_id, from: #function).wait()
        XCTAssertEqual(try connection.xlen(of: #function).wait(), 0)
    }

}

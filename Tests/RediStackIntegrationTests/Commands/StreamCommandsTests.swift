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

public typealias XReadResult = [(String, [(String, [String: String])])]

final class StreamCommandsTests: RediStackIntegrationTestCase {

    func test_xadd() throws {
        var streamLength: Int = try connection.send(command: "XLEN", with: [#function.convertedToRESPValue()]).wait().int!
        XCTAssertEqual(streamLength, 0)
        let entry_id = try connection.xadd(["foo": "bar"], to: #function).wait()
        streamLength = try connection.send(command: "XLEN", with: [#function.convertedToRESPValue()]).wait().int!
        XCTAssertEqual(streamLength, 1)
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
        let entryID = try connection.xadd(["foo": "bar"], to: #function).wait()
        XCTAssertEqual(try connection.xlen(of: #function).wait(), 1)
        _ = try connection.xdel(entryID, from: #function).wait()
        XCTAssertEqual(try connection.xlen(of: #function).wait(), 0)
    }

    func test_xtrim() throws {
        for _ in 1...3 {
        _ = try connection.xadd(["foo": "bar"], to: #function).wait()
        }
        XCTAssertEqual(try connection.xlen(of: #function).wait(), 3)
        let numDeleted = try connection.xtrim(to: 1, from: #function).wait()
        XCTAssertEqual(try connection.xlen(of: #function).wait(), 1)
        XCTAssertEqual(numDeleted, 2)
    }

    func test_xrange() throws {
        for _ in 1...3 {
        _ = try connection.xadd(["foo": "bar", "baz": "qux"], to: #function).wait()
        }
        var response = try connection.xrange(from: "-", to: "+", from: #function).wait()
        XCTAssertEqual(response.count, 3)
        XCTAssertEqual(response[0].1.count, 2)

        response = try connection.xrange(from: "-", to: "+", from: #function, 1).wait()
        XCTAssertEqual(response.count, 1)
    }

    func test_xread() throws {
        for _ in 1...3 {
        _ = try connection.xadd(["foo": "bar", "baz": "qux"], to: #function).wait()
        }
        let response: XReadResult = try connection.xread(from: [#function: "0-0"]).wait()

        // Verify we get results for one stream
        XCTAssertEqual(response.count, 1)
        XCTAssertEqual(response[0].0, #function)

        // Verify we get the three entries back that we added
        let entries: [(String, [String : String])] = response[0].1
        XCTAssertEqual(entries.count, 3)
        XCTAssert(entries.allSatisfy {
            $0.1["foo"] == "bar" && $0.1["baz"] == "qux"
        })
    }
}

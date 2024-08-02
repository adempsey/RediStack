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

import NIOCore

// MARK: General

extension RedisClient {

    @inlinable
    public func xadd<Value: RESPValueConvertible>(_ elements: [Value: Value], to key: RedisKey) -> EventLoopFuture<String> {
        guard elements.count > 0 else { return self.eventLoop.makeSucceededFuture("") }

        var elementList = [Value]()
        for (key, value) in elements {
            elementList.append(key)
            elementList.append(value)
        }

        var args: [RESPValue] = [.init(from: key)]
        args.append(RESPValue(from: "*"))
        args.append(convertingContentsOf: elementList)

        return send(command: "XADD", with: args)
            .tryConverting()
    }


    /// Gets the number of entries inside a stream.
    ///
    /// See [https://redis.io/commands/xlen](https://redis.io/commands/xlen)
    /// - Parameter key: The key of the stream.
    /// - Returns: The total count of entries in the stream.
    public func xlen(of key: RedisKey) -> EventLoopFuture<Int> {
        let args = [RESPValue(from: key)]
        return send(command: "XLEN", with: args).tryConverting()
    }


    /// Removes the specified entries from a stream, and returns the number
    /// of entries deleted. This number may be less than the number of IDs
    /// passed to the command in the case where some of the specified IDs
    /// do not exist in the stream.
    ///
    /// See [https://redis.io/commands/xdel](https://redis.io/commands/xdel)
    /// - Parameters:
    ///     - fields: The list of entry IDs that should be removed from the stream.
    ///     - key: The key of the stream.
    /// - Returns: The number of entries that were deleted.
    public func xdel(_ fields: String..., from key: RedisKey) -> EventLoopFuture<Int> {
        guard fields.count > 0 else { return self.eventLoop.makeSucceededFuture(0) }

        var args: [RESPValue] = [.init(from: key)]
        args.append(convertingContentsOf: fields)

        return send(command: "XDEL", with: args)
            .tryConverting()
    }

    /// Trims the stream by evicting older entries (entries with lower IDs) if needed.
    /// Will evict entries as long as the stream's length exceeds the specified threshold.
    ///
    /// See [https://redis.io/commands/xtrim](https://redis.io/commands/xtrim)
    /// - Parameters:
    ///     - threshold: The maximum length of the stream to remain after evicting entries.
    ///     - key: The key of the stream.
    /// - Returns: The number of entries that were deleted.
    public func xtrim(to threshold: Int, from key: RedisKey) -> EventLoopFuture<Int> {
        assert(threshold >= 0, "Stream cannot be trimmed to a negative length")
        guard threshold >= 0 else { return self.eventLoop.makeSucceededFuture(0) }

        let args: [RESPValue] = [
            .init(from: key),
            .init(bulk: "MAXLEN"),
            .init(bulk: threshold)
        ]
        return send(command: "XTRIM", with: args)
            .tryConverting()
    }

}

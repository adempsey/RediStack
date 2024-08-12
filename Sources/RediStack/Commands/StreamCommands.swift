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

    /// The command returns the stream entries matching a given range of IDs.
    /// The range is specified by a minimum and maximum ID. All the entries
    /// having an ID between the two specified or exactly one of the two IDs
    /// specified (closed interval) are returned.
    ///
    /// See [https://redis.io/commands/xrange](https://redis.io/commands/xrange)
    /// - Parameters:
    ///     - start: The entry ID to begin reading from.
    ///     - end: The entry ID to stop reading at.
    ///     - key: The stream to read entries from.
    /// - Returns: A list of 2-tuples, the first element of which is the entry
    ///            ID, and the second element of which is a dictionary mapping 
    ///            entry field keys to values.
    ///            For example:
    ///            [
    ///                ("id-1234", ["foo": "bar"]),
    ///                ("id-5678", ["baz": "qux"]),
    ///                ("id-1234", ["foo": "bar"]),
    ///                ("id-5678", ["baz": "qux"]),
    ///            ]
    public func xrange(
        from start: String, 
        to end: String, 
        from key: RedisKey, 
        _ count: UInt? = nil
    ) -> EventLoopFuture<[(String, [String:String])]> {
        var args: [RESPValue] = [
            .init(from: key),
            .init(from: start),
            .init(from: end)
        ]

        if count != nil {
            args.append(RESPValue(from: "COUNT"))
            args.append(convertingContentsOf: [count])
        }
        
        return send(command: "XRANGE", with: args)
            .map { (resultRESP: RESPValue) in
                guard let results: [RESPValue] = resultRESP.array else { return [] } 
                return results.map { (result: RESPValue) -> (String, [String:String]) in
                    guard let entries: [RESPValue] = result.array else { return ("", [:])}
                    guard let entryID: String = entries[0].string else { return ("", [:])}
                    guard let fields: [RESPValue] = entries[1].array else { return (entryID, [:])}
                    var fieldsDict: [String: String] = [:]
                    for i in stride(from: 0, to: fields.count, by: 2) {
                        if i + 1 < fields.count {
                            fieldsDict[fields[i].string!] = fields[i+1].string
                        }
                    }
                    return (entryID, fieldsDict)
                }
            }
        }
    

    public typealias XReadResult = [(String, [(String, [String: String])])]

    /// Read data from one or multiple streams, only returning entries with
    /// an ID greater than the last received ID reported by the caller.
    ///
    /// See [https://redis.io/commands/xread](https://redis.io/commands/xread)
    /// - Parameters:
    ///     - streams: Dictionary mapping stream keys to the entry IDs
    ///       to begin reading after.
    ///     - count: The maximum number of entries to return for each stream.
    /// - Returns: A list of 2-tuples, the first element of which is the name
    ///            of a stream, and the second element of which is a list of
    ///            the stream's entries. Each entry is a 2-tuple, the first
    ///            element of which is the entry ID and the second of which
    ///            is a dictionary mapping entry field keys to values.
    ///            For example:
    ///            [
    ///                 ("stream1", [
    ///                     ("id-1234", ["foo": "bar"]),
    ///                     ("id-5678", ["baz": "qux"]),
    ///                 ]),
    ///                 ("stream2", [
    ///                     ("id-1234", ["foo": "bar"]),
    ///                     ("id-5678", ["baz": "qux"]),
    ///                 ]),
    ///            ]
    public func xread<Value: RESPValueConvertible>(
        from streams: [Value: Value],
        _ count: UInt = 0
        ) -> EventLoopFuture<XReadResult> {

            var streamList = [Value]()
            for (key, value) in streams {
                streamList.append(key)
                streamList.append(value)
            }
            var args: [RESPValue] = [
                .init(bulk: "COUNT"),
                .init(from: count),
                .init(bulk: "STREAMS"),
            ]
            args.append(convertingContentsOf: streamList)

            return send(command: "XREAD", with: args)
                .map { (resultRESP: RESPValue) in
                    guard let results: [RESPValue] = Array(fromRESP: resultRESP) else { return [] }
                    return results.map { (result: RESPValue) -> (String,[(String, [String: String])]) in
                        guard let streamResults: [RESPValue]  = result.array else { return ("", []) }
                        guard let stream: String = streamResults[0].string else { return ("", []) }
                        guard let entries: [RESPValue] = streamResults[1].array else { return (stream, [])}
                        let decodedEntries: [(String, [String: String])] = entries.map { (entry: RESPValue) in
                            guard let decodedEntry: [RESPValue] = entry.array else { return ("", [:]) }
                            guard let entryID: String = decodedEntry[0].string else { return ("", [:])}

                            guard let fieldArray: [RESPValue] = decodedEntry[1].array else { return (entryID, [:])}
                            var fields: [String: String] = [:]
                            for i in stride(from: 0, to: fieldArray.count, by: 2) {
                                if i + 1 < fieldArray.count {
                                    fields[fieldArray[i].string!] = fieldArray[i+1].string
                                }
                            }
                            return (entryID, fields)
                        }
                        return (stream, decodedEntries)
                    }
                }
    }
}

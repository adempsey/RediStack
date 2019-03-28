import NIO
import NIOConcurrencyHelpers

public final class RedisDriver {
    /// The threading model to use for asynchronous tasks.
    ///
    /// Using `.eventLoopGroup` will allow an external provider to handle the lifetime of the `EventLoopGroup`,
    /// while using `spawnThreads` will cause this `NIORedis` instance to handle the lifetime of a new `EventLoopGroup`.
    public enum ThreadOwnershipModel {
        case `internal`(threadCount: Int)
        case external(EventLoopGroup)
    }

    public let eventLoopGroup: EventLoopGroup

    private let ownershipModel: ThreadOwnershipModel

    /// Is the driver available for operations?
    public var isRunning: Bool { return _isRunning.load() }
    private let _isRunning = Atomic<Bool>(value: true)

    deinit { assert(!_isRunning.load(), "Redis driver was not properly shut down!") }

    /// Creates a driver instance to create connections to a Redis.
    /// - Important: Call `terminate()` before deinitializing to properly cleanup resources.
    /// - Parameter ownershipModel: The model to use for handling connection resources.
    public init(ownershipModel model: ThreadOwnershipModel) {
        self.ownershipModel = model

        switch model {
        case .internal(let count):
            self.eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: count)
        case .external(let group):
            self.eventLoopGroup = group
        }
    }

    /// Handles the proper shutdown of managed resources.
    /// - Important: This method should always be called, even when running in `.eventLoopGroup` mode.
    public func terminate() throws {
        guard _isRunning.exchange(with: false) else { return }

        switch ownershipModel {
        case .internal: try self.eventLoopGroup.syncShutdownGracefully()
        case .external: return
        }
    }

    /// Creates a new `RedisConnection` with the parameters provided.
    public func makeConnection(
        hostname: String = "localhost",
        port: Int = 6379,
        password: String? = nil
    ) -> EventLoopFuture<RedisConnection> {
        let bootstrap = ClientBootstrap.makeForRedis(using: eventLoopGroup)

        return bootstrap.connect(host: hostname, port: port)
            .map { return RedisConnection(channel: $0) }
            .flatMap { connection in
                guard let pw = password else {
                    return self.eventLoopGroup.next().makeSucceededFuture(connection)
                }

                return connection.authorize(with: pw).map { _ in return connection }
            }
    }
}

private extension ChannelPipeline {
    func addHandlers(_ handlers: ChannelHandler...) -> EventLoopFuture<Void> {
        return .andAllSucceed(handlers.map { addHandler($0) }, on: eventLoop)
    }
}

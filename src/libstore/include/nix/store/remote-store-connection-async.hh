#pragma once
/**
 * @file
 *
 * @brief Async extensions for RemoteStore connections.
 *
 * This provides async versions of RemoteStore::Connection and
 * RemoteStore::ConnectionHandle that use kj-async for non-blocking I/O.
 */

#include "nix/store/remote-store.hh"
#include "nix/store/remote-store-connection.hh"
#include "nix/store/worker-protocol.hh"
#include "nix/store/worker-protocol-impl.hh"
#include "nix/store/worker-protocol-connection.hh"
#include "nix/util/async.hh"
#include "nix/util/async-io.hh"
#include "nix/util/async-signal.hh"
#include "nix/util/pool.hh"
#include "nix/util/result.hh"
#include "nix/util/serialise.hh"

#include <kj/async.h>
#include <tuple>
#include <type_traits>
#include <utility>

namespace nix {

/**
 * Async extension methods for RemoteStore::Connection.
 *
 * These are provided as free functions rather than modifying the
 * Connection class directly, to allow incremental migration.
 */
namespace async {

/**
 * Wrapper type for remote errors because `Result<std::exception_ptr>`
 * does not work very well and `Result<Result<void>>` is too confusing.
 */
struct [[nodiscard]] RemoteError
{
    std::exception_ptr e;
};

/**
 * Process stderr messages from the daemon asynchronously.
 *
 * @param conn The connection to read from
 * @param stream The async stream for I/O
 * @return A promise that resolves to any remote error, or success
 */
kj::Promise<Result<RemoteError>> processStderrAsync(RemoteStore::Connection & conn, AsyncFdIoStream & stream);

/**
 * Async connection handle that provides async sendCommand methods.
 *
 * This wraps a Pool<RemoteStore::Connection>::Handle and provides
 * async versions of the command sending methods.
 */
class AsyncConnectionHandle
{
    Pool<RemoteStore::Connection>::Handle handle;
    const StoreDirConfig & store;

public:
    AsyncConnectionHandle(Pool<RemoteStore::Connection>::Handle && h, const StoreDirConfig & s)
        : handle(std::move(h))
        , store(s)
    {
    }

    AsyncConnectionHandle(AsyncConnectionHandle &&) = default;

    // Pool::Handle doesn't support move assignment, so neither do we
    AsyncConnectionHandle & operator=(AsyncConnectionHandle &&) = delete;

    RemoteStore::Connection & operator*()
    {
        return *handle;
    }

    RemoteStore::Connection * operator->()
    {
        return &*handle;
    }

    /**
     * Get the file descriptor for async I/O.
     */
    int getFD()
    {
        return handle->to.fd;
    }

    /**
     * Get the store configuration.
     */
    const StoreDirConfig & getStore() const
    {
        return store;
    }

    /**
     * Mark the connection as bad (will be closed when returned to pool).
     */
    void markBad()
    {
        handle.markBad();
    }

    /**
     * Process stderr messages asynchronously.
     */
    kj::Promise<Result<void>> processStderr(AsyncFdIoStream & stream);

    /**
     * Execute a callback with a framed async output stream.
     */
    kj::Promise<Result<void>> withFramedStream(
        AsyncFdIoStream & stream, std::function<kj::Promise<Result<void>>(AsyncOutputStream & stream)> fun);

    /**
     * Send a command to the daemon and get a response (uninterruptible).
     *
     * This serializes all arguments, sends them to the daemon, processes
     * stderr, and deserializes the response.
     *
     * @tparam R The return type to deserialize
     * @tparam Args The argument types to serialize
     * @param args The arguments to send
     * @return A promise that resolves to the response
     */
    template<typename R = void, typename... Args>
    kj::Promise<Result<R>> sendCommandUninterruptible(Args &&... args);

    /**
     * Send a command to the daemon (interruptible by Ctrl-C).
     *
     * Same as sendCommandUninterruptible but can be cancelled by SIGINT.
     */
    template<typename R = void, typename... Args>
    kj::Promise<Result<R>> sendCommand(Args &&... args)
    {
        return makeInterruptible(sendCommandUninterruptible<R>(std::forward<Args>(args)...));
    }
};

namespace detail {

// Helper to serialize a single argument using WorkerProto
template<typename T>
void serializeArg(const StoreDirConfig & store, WorkerProto::WriteConn conn, T && arg)
{
    // For Op codes and primitive types, use operator<<
    if constexpr (std::is_same_v<std::decay_t<T>, WorkerProto::Op>) {
        conn.to << std::forward<T>(arg);
    } else if constexpr (std::is_integral_v<std::decay_t<T>>) {
        conn.to << std::forward<T>(arg);
    } else {
        // For complex types, use WorkerProto serialization
        WorkerProto::write(store, conn, std::forward<T>(arg));
    }
}

} // namespace detail

// Template implementation
template<typename R, typename... Args>
kj::Promise<Result<R>> AsyncConnectionHandle::sendCommandUninterruptible(Args &&... args)
try {
    // Get the file descriptor for async I/O
    int fd = getFD();

    // Invalidate this connection if we're cancelled early
    auto invalidateOnCancel = kj::defer([this] {
        if (std::uncaught_exceptions() > 0) {
            handle.markBad();
        }
    });

    AsyncFdIoStream stream{AsyncFdIoStream::shared_fd{}, fd};

    // Serialize all arguments using WorkerProto and send them
    try {
        // Flush any existing data first
        handle->to.flush();

        // Serialize each argument
        WorkerProto::WriteConn writeConn{handle->to, handle->protoVersion};
        (detail::serializeArg(store, writeConn, std::forward<Args>(args)), ...);

        // Flush the serialized data
        handle->to.flush();
    } catch (...) {
        handle.markBad();
        throw;
    }

    // Process stderr from daemon (synchronously for now)
    // TODO: Make this async
    auto ex = handle->processStderrReturn();
    if (ex) {
        std::rethrow_exception(ex);
    }

    if constexpr (std::is_void_v<R>) {
        invalidateOnCancel.cancel();
        co_return result::success();
    } else {
        try {
            // Read the response synchronously for now
            // TODO: Implement async deserialization
            auto res =
                WorkerProto::Serialise<R>::read(store, WorkerProto::ReadConn{handle->from, handle->protoVersion});
            invalidateOnCancel.cancel();
            co_return res;
        } catch (...) {
            handle.markBad();
            throw;
        }
    }
} catch (...) {
    co_return result::current_exception();
}

/**
 * Helper function to create an AsyncConnectionHandle from a RemoteStore.
 *
 * Usage:
 * @code
 * auto conn = async::getAsyncConnection(store, store.connections->get());
 * auto valid = co_await conn.sendCommand<bool>(WorkerProto::Op::IsValidPath, path);
 * @endcode
 */
inline AsyncConnectionHandle
getAsyncConnection(const StoreDirConfig & store, Pool<RemoteStore::Connection>::Handle && h)
{
    return AsyncConnectionHandle(std::move(h), store);
}

} // namespace async

/**
 * Async version of store operations.
 *
 * These functions provide async versions of common RemoteStore operations
 * that can be used with the kj event loop.
 *
 * Note: These are example implementations. For a complete asyncification,
 * all store operations would need similar async variants.
 */
namespace async_ops {

/**
 * Async version of isValidPath.
 *
 * @param conn An async connection handle
 * @param path The path to check
 * @return A promise that resolves to true if the path is valid
 */
kj::Promise<Result<bool>> isValidPathAsync(async::AsyncConnectionHandle & conn, const StorePath & path);

/**
 * Async version of queryAllValidPaths.
 *
 * @param conn An async connection handle
 * @return A promise that resolves to the set of all valid paths
 */
kj::Promise<Result<StorePathSet>> queryAllValidPathsAsync(async::AsyncConnectionHandle & conn);

} // namespace async_ops

} // namespace nix

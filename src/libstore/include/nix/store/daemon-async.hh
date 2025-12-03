#pragma once
/**
 * @file
 *
 * @brief Async extensions for the Nix daemon.
 *
 * This provides async-aware versions of daemon functionality that can
 * be used with the kj event loop. It allows the daemon to handle
 * multiple connections efficiently using non-blocking I/O.
 */

#include "nix/store/daemon.hh"
#include "nix/store/store-api.hh"
#include "nix/util/async.hh"
#include "nix/util/async-io.hh"
#include "nix/util/result.hh"
#include "nix/util/serialise.hh"

#include <kj/async.h>
#include <kj/async-io.h>
#include <kj/async-unix.h>

namespace nix::daemon::async {

/**
 * Information about a connected peer.
 */
struct PeerInfo
{
    bool pidKnown = false;
    pid_t pid = 0;
    bool uidKnown = false;
    uid_t uid = 0;
    bool gidKnown = false;
    gid_t gid = 0;
};

/**
 * Get peer information from a Unix domain socket.
 */
PeerInfo getPeerInfo(int fd);

/**
 * Async connection handler for daemon connections.
 *
 * This wraps the connection handling logic to work with async I/O.
 */
class AsyncDaemonConnection
{
    ref<Store> store;
    AutoCloseFD fd;
    TrustedFlag trusted;
    daemon::RecursiveFlag recursive;

public:
    AsyncDaemonConnection(
        ref<Store> store, AutoCloseFD fd, TrustedFlag trusted, daemon::RecursiveFlag recursive = daemon::NotRecursive)
        : store(store)
        , fd(std::move(fd))
        , trusted(trusted)
        , recursive(recursive)
    {
    }

    /**
     * Process the connection asynchronously.
     *
     * This handles the protocol handshake and command loop using
     * async I/O primitives.
     */
    kj::Promise<Result<void>> run();

    /**
     * Get the file descriptor for this connection.
     */
    int getFD() const
    {
        return fd.get();
    }
};

/**
 * Async listener for daemon connections.
 *
 * This listens on a Unix domain socket and accepts connections
 * asynchronously using the kj event loop.
 */
class AsyncDaemonListener
{
    ref<Store> store;
    AutoCloseFD listenFd;
    kj::UnixEventPort::FdObserver observer;
    TrustedFlag defaultTrust;

public:
    /**
     * Create a listener on an existing socket fd.
     */
    AsyncDaemonListener(ref<Store> store, AutoCloseFD listenFd, TrustedFlag defaultTrust = Trusted);

    /**
     * Create a listener by creating and binding a new socket.
     */
    static AsyncDaemonListener create(ref<Store> store, const Path & socketPath, TrustedFlag defaultTrust = Trusted);

    /**
     * Accept a connection asynchronously.
     *
     * Returns when a new connection is available.
     */
    kj::Promise<Result<AsyncDaemonConnection>> accept();

    /**
     * Run the daemon loop, accepting and handling connections.
     *
     * This forks a child process for each connection (like the
     * traditional Nix daemon) or can be configured to handle
     * connections in-process.
     *
     * @param forkPerConnection If true, fork for each connection
     */
    kj::Promise<Result<void>> run(bool forkPerConnection = true);

    int getFD() const
    {
        return listenFd.get();
    }
};

/**
 * Process a connection on a file descriptor asynchronously.
 *
 * This is the async equivalent of daemon::processConnection().
 *
 * @param store The store to use
 * @param fd The file descriptor for the connection (will be consumed)
 * @param trusted Whether the client is trusted
 * @param recursive Whether this is a recursive call
 */
kj::Promise<Result<void>> processConnectionAsync(
    ref<Store> store, AutoCloseFD fd, TrustedFlag trusted, daemon::RecursiveFlag recursive = daemon::NotRecursive);

/**
 * Run the daemon loop asynchronously.
 *
 * This creates a socket at the given path and accepts connections.
 * It's the async equivalent of the main daemon loop.
 *
 * @param store The store to use
 * @param socketPath Path for the Unix domain socket
 * @param defaultTrust Default trust level for clients
 */
kj::Promise<Result<void>> runDaemonAsync(ref<Store> store, const Path & socketPath, TrustedFlag defaultTrust = Trusted);

} // namespace nix::daemon::async

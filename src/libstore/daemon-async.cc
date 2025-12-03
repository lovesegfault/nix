#include "nix/store/daemon-async.hh"
#include "nix/store/daemon.hh"
#include "nix/store/worker-protocol.hh"
#include "nix/store/worker-protocol-connection.hh"
#include "nix/store/globals.hh"
#include "nix/util/signals.hh"
#include "nix/util/finally.hh"
#include "nix/util/unix-domain-socket.hh"

#include <sys/socket.h>
#include <sys/un.h>
#include <fcntl.h>
#include <unistd.h>

#if defined(__APPLE__) || defined(__FreeBSD__)
#  include <sys/ucred.h>
#endif

namespace nix::daemon::async {

PeerInfo getPeerInfo(int fd)
{
    PeerInfo info;

#if defined(SO_PEERCRED)
    // Linux
    struct ucred cred;
    socklen_t len = sizeof(cred);
    if (getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &cred, &len) == 0) {
        info.pidKnown = true;
        info.pid = cred.pid;
        info.uidKnown = true;
        info.uid = cred.uid;
        info.gidKnown = true;
        info.gid = cred.gid;
    }
#elif defined(LOCAL_PEERCRED)
    // macOS, FreeBSD
    struct xucred cred;
    socklen_t len = sizeof(cred);
    if (getsockopt(fd, SOL_LOCAL, LOCAL_PEERCRED, &cred, &len) == 0) {
        info.uidKnown = true;
        info.uid = cred.cr_uid;
        info.gidKnown = true;
        info.gid = cred.cr_gid;
    }
#  if defined(LOCAL_PEERPID)
    // macOS
    pid_t pid;
    len = sizeof(pid);
    if (getsockopt(fd, SOL_LOCAL, LOCAL_PEERPID, &pid, &len) == 0) {
        info.pidKnown = true;
        info.pid = pid;
    }
#  endif
#endif

    return info;
}

kj::Promise<Result<void>> AsyncDaemonConnection::run()
try {
    // For now, delegate to the synchronous implementation
    // by wrapping it in a coroutine that yields periodically
    FdSource from(fd.get());
    FdSink to(fd.get());

    // Call the synchronous processConnection
    daemon::processConnection(store, std::move(from), std::move(to), trusted, recursive);

    co_return result::success();
} catch (...) {
    co_return result::current_exception();
}

AsyncDaemonListener::AsyncDaemonListener(ref<Store> store_, AutoCloseFD listenFd_, TrustedFlag defaultTrust_)
    : store(store_)
    , listenFd(std::move(listenFd_))
    , observer(AIO().unixEventPort, listenFd.get(), kj::UnixEventPort::FdObserver::OBSERVE_READ)
    , defaultTrust(defaultTrust_)
{
    // Set the socket to non-blocking mode
    int flags = fcntl(listenFd.get(), F_GETFL, 0);
    fcntl(listenFd.get(), F_SETFL, flags | O_NONBLOCK);
}

AsyncDaemonListener AsyncDaemonListener::create(ref<Store> store, const Path & socketPath, TrustedFlag defaultTrust)
{
    createDirs(dirOf(socketPath));
    auto fd = createUnixDomainSocket(socketPath, 0666);
    return AsyncDaemonListener(store, std::move(fd), defaultTrust);
}

kj::Promise<Result<AsyncDaemonConnection>> AsyncDaemonListener::accept()
try {
    while (true) {
        struct sockaddr_un remoteAddr;
        socklen_t remoteAddrLen = sizeof(remoteAddr);

        int remote = ::accept(listenFd.get(), reinterpret_cast<struct sockaddr *>(&remoteAddr), &remoteAddrLen);

        if (remote >= 0) {
            // Got a connection
            AutoCloseFD remoteFd(remote);

            // Make it blocking (it may inherit non-blocking from parent on macOS)
            int flags = fcntl(remote, F_GETFL, 0);
            fcntl(remote, F_SETFL, flags & ~O_NONBLOCK);

            // Set close-on-exec
            fcntl(remote, F_SETFD, FD_CLOEXEC);

            // Get peer info to determine trust level
            auto peer = getPeerInfo(remote);
            TrustedFlag trusted = defaultTrust;

            // Log the connection
            if (peer.pidKnown) {
                printInfo("accepted connection from pid %d", peer.pid);
            } else {
                printInfo("accepted connection from unknown peer");
            }

            co_return AsyncDaemonConnection(store, std::move(remoteFd), trusted);
        }

        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            // No connection available, wait for one
            co_await observer.whenBecomesReadable();
            checkInterrupt();
            continue;
        }

        if (errno == EINTR) {
            checkInterrupt();
            continue;
        }

        throw SysError("accepting connection");
    }
} catch (...) {
    co_return result::current_exception();
}

kj::Promise<Result<void>> AsyncDaemonListener::run(bool forkPerConnection)
try {
    while (true) {
        checkInterrupt();

        auto connResult = TRY_AWAIT(accept());

        if (forkPerConnection) {
            // Fork a child to handle the connection
            pid_t pid = fork();
            if (pid == -1) {
                throw SysError("forking to handle connection");
            }

            if (pid == 0) {
                // Child process
                try {
                    // Close the listening socket in the child
                    listenFd.close();

                    // Process the connection synchronously
                    FdSource from(connResult.getFD());
                    FdSink to(connResult.getFD());
                    daemon::processConnection(store, std::move(from), std::move(to), Trusted, daemon::NotRecursive);
                    _exit(0);
                } catch (...) {
                    _exit(1);
                }
            }

            // Parent continues to accept more connections
            // The child's connection fd will be closed when connResult goes out of scope
        } else {
            // Handle connection in-process (not forking)
            // This blocks the accept loop until the connection is done
            TRY_AWAIT(connResult.run());
        }
    }

    co_return result::success();
} catch (...) {
    co_return result::current_exception();
}

kj::Promise<Result<void>>
processConnectionAsync(ref<Store> store, AutoCloseFD fd, TrustedFlag trusted, daemon::RecursiveFlag recursive)
{
    AsyncDaemonConnection conn(store, std::move(fd), trusted, recursive);
    return conn.run();
}

kj::Promise<Result<void>> runDaemonAsync(ref<Store> store, const Path & socketPath, TrustedFlag defaultTrust)
{
    auto listener = AsyncDaemonListener::create(store, socketPath, defaultTrust);
    return listener.run(true);
}

} // namespace nix::daemon::async

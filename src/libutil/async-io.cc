#include "nix/util/async-io.hh"
#include "nix/util/file-descriptor.hh"

#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

namespace nix {

// AsyncInputStream implementations

kj::Promise<Result<std::optional<size_t>>> AsyncInputStream::readRange(void * buffer, size_t min, size_t max)
try {
    size_t total = 0;
    auto bufferC = static_cast<char *>(buffer);
    while (total < min) {
        if (auto got = TRY_AWAIT(read(bufferC + total, max - total)); !got) {
            co_return std::nullopt;
        } else {
            total += *got;
        }
    }
    co_return total;
} catch (...) {
    co_return result::current_exception();
}

kj::Promise<Result<void>> AsyncInputStream::drainInto(Sink & sink)
try {
    std::array<char, 64 * 1024> buffer;
    while (true) {
        auto n = TRY_AWAIT(read(buffer.data(), buffer.size()));
        if (!n)
            break;
        sink(std::string_view(buffer.data(), *n));
    }
    co_return result::success();
} catch (...) {
    co_return result::current_exception();
}

kj::Promise<Result<void>> AsyncInputStream::drainInto(AsyncOutputStream & stream)
try {
    std::array<char, 64 * 1024> buffer;
    while (true) {
        auto n = TRY_AWAIT(read(buffer.data(), buffer.size()));
        if (!n)
            break;
        TRY_AWAIT(stream.writeFull(buffer.data(), *n));
    }
    co_return result::success();
} catch (...) {
    co_return result::current_exception();
}

kj::Promise<Result<std::string>> AsyncInputStream::drain()
try {
    std::string result;
    std::array<char, 64 * 1024> buffer;
    while (true) {
        auto n = TRY_AWAIT(read(buffer.data(), buffer.size()));
        if (!n)
            break;
        result.append(buffer.data(), *n);
    }
    co_return result;
} catch (...) {
    co_return result::current_exception();
}

// AsyncSourceInputStream

kj::Promise<Result<std::optional<size_t>>> AsyncSourceInputStream::read(void * buffer, size_t size)
try {
    if (size == 0)
        co_return size_t{0};

    try {
        size_t n = inner.read(static_cast<char *>(buffer), size);
        if (n == 0)
            co_return std::nullopt;
        co_return n;
    } catch (EndOfFile &) {
        co_return std::nullopt;
    }
} catch (...) {
    co_return result::current_exception();
}

// AsyncStringInputStream

kj::Promise<Result<std::optional<size_t>>> AsyncStringInputStream::read(void * buffer, size_t size)
{
    if (s.empty()) {
        return {std::nullopt};
    }

    size_t n = std::min(size, s.size());
    std::memcpy(buffer, s.data(), n);
    s = s.substr(n);
    return {n};
}

// AsyncTeeInputStream

kj::Promise<Result<std::optional<size_t>>> AsyncTeeInputStream::read(void * buffer, size_t size)
try {
    auto n = TRY_AWAIT(inner.read(buffer, size));
    if (n) {
        sink(std::string_view(static_cast<char *>(buffer), *n));
    }
    co_return n;
} catch (...) {
    co_return result::current_exception();
}

// Note: AsyncGeneratorInputStream implementation was removed as it requires
// the Generator type which is not present in Nix (it's a Lix addition).

// AsyncBufferedInputStream

kj::Promise<Result<std::optional<size_t>>> AsyncBufferedInputStream::read(void * data, size_t size)
try {
    if (size == 0)
        co_return size_t{0};

    // Return from buffer if we have data
    if (buffer->available() > 0) {
        size_t n = std::min(size, buffer->available());
        std::memcpy(data, buffer->readable().data(), n);
        buffer->consume(n);
        co_return n;
    }

    // Buffer is empty, fill it
    buffer->reset();
    auto n = TRY_AWAIT(inner.read(buffer->writable(), buffer->freeSpace()));
    if (!n)
        co_return std::nullopt;

    buffer->produce(*n);
    size_t result = std::min(size, buffer->available());
    std::memcpy(data, buffer->readable().data(), result);
    buffer->consume(result);
    co_return result;
} catch (...) {
    co_return result::current_exception();
}

// AsyncBufferedOutputStream

kj::Promise<Result<size_t>> AsyncBufferedOutputStream::write(const void * src, size_t size)
try {
    // If data fits in buffer, just buffer it
    if (size <= buffer->freeSpace()) {
        std::memcpy(buffer->writable(), src, size);
        buffer->produce(size);
        co_return size;
    }

    // Flush buffer first if it has data
    if (buffer->available() > 0) {
        TRY_AWAIT(flush());
    }

    // Write directly if large
    if (size > buffer->size() / 2) {
        co_return TRY_AWAIT(inner.write(src, size));
    }

    // Otherwise buffer
    std::memcpy(buffer->writable(), src, size);
    buffer->produce(size);
    co_return size;
} catch (...) {
    co_return result::current_exception();
}

kj::Promise<Result<void>> AsyncBufferedOutputStream::flush()
try {
    if (buffer->available() > 0) {
        TRY_AWAIT(inner.writeFull(buffer->readable().data(), buffer->available()));
        buffer->reset();
    }
    co_return result::success();
} catch (...) {
    co_return result::current_exception();
}

// AsyncFdIoStream

static FdBlockingState setNonBlocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    FdBlockingState oldState = (flags & O_NONBLOCK) ? FdBlockingState::NonBlocking : FdBlockingState::Blocking;
    if (!(flags & O_NONBLOCK)) {
        fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    }
    return oldState;
}

static void restoreBlocking(int fd, FdBlockingState state)
{
    if (state == FdBlockingState::Blocking) {
        int flags = fcntl(fd, F_GETFL, 0);
        fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
    }
}

AsyncFdIoStream::AsyncFdIoStream(AutoCloseFD fd_)
    : fd(fd_.get())
    , oldState(setNonBlocking(fd))
    , ownedFd(std::move(fd_))
    , observer(AIO().unixEventPort, fd, kj::UnixEventPort::FdObserver::OBSERVE_READ_WRITE)
{
}

AsyncFdIoStream::AsyncFdIoStream(shared_fd, int fd_)
    : fd(fd_)
    , oldState(setNonBlocking(fd))
    , observer(AIO().unixEventPort, fd, kj::UnixEventPort::FdObserver::OBSERVE_READ_WRITE)
{
}

AsyncFdIoStream::~AsyncFdIoStream() noexcept(false)
{
    restoreBlocking(fd, oldState);
}

kj::Promise<Result<std::optional<size_t>>> AsyncFdIoStream::read(void * tgt, size_t size)
try {
    while (true) {
        ssize_t n = ::read(fd, tgt, size);
        if (n > 0) {
            co_return static_cast<size_t>(n);
        } else if (n == 0) {
            co_return std::nullopt;
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            co_await observer.whenBecomesReadable();
        } else if (errno == EINTR) {
            checkInterrupt();
        } else {
            throw SysError("reading from fd %d", fd);
        }
    }
} catch (...) {
    co_return result::current_exception();
}

kj::Promise<Result<size_t>> AsyncFdIoStream::write(const void * src, size_t size)
try {
    while (true) {
        ssize_t n = ::write(fd, src, size);
        if (n >= 0) {
            co_return static_cast<size_t>(n);
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            co_await observer.whenBecomesWritable();
        } else if (errno == EINTR) {
            checkInterrupt();
        } else {
            throw SysError("writing to fd %d", fd);
        }
    }
} catch (...) {
    co_return result::current_exception();
}

// AsyncFramedInputStream

AsyncFramedInputStream::~AsyncFramedInputStream()
{
    // Best effort: try to drain remaining frames
}

kj::Promise<Result<void>> AsyncFramedInputStream::finish()
try {
    // Read until we see the end frame (size 0)
    while (!eof) {
        uint64_t frameSize;
        auto n = TRY_AWAIT(from.readRange(&frameSize, sizeof(frameSize), sizeof(frameSize)));
        if (!n) {
            throw Error("unexpected EOF in framed stream");
        }

        if (frameSize == 0) {
            eof = true;
            break;
        }

        // Skip the frame data
        pending.resize(frameSize);
        auto dataRead = TRY_AWAIT(from.readRange(pending.data(), frameSize, frameSize));
        if (!dataRead) {
            throw Error("unexpected EOF reading frame data");
        }
        pending.clear();
    }
    co_return result::success();
} catch (...) {
    co_return result::current_exception();
}

kj::Promise<Result<std::optional<size_t>>> AsyncFramedInputStream::read(void * buffer, size_t size)
try {
    if (eof)
        co_return std::nullopt;

    // Return from pending buffer if we have data
    if (pos < pending.size()) {
        size_t n = std::min(size, pending.size() - pos);
        std::memcpy(buffer, pending.data() + pos, n);
        pos += n;
        if (pos == pending.size()) {
            pending.clear();
            pos = 0;
        }
        co_return n;
    }

    // Read next frame header
    uint64_t frameSize;
    auto n = TRY_AWAIT(from.readRange(&frameSize, sizeof(frameSize), sizeof(frameSize)));
    if (!n) {
        eof = true;
        co_return std::nullopt;
    }

    if (frameSize == 0) {
        eof = true;
        co_return std::nullopt;
    }

    // Read frame data
    pending.resize(frameSize);
    pos = 0;
    auto dataRead = TRY_AWAIT(from.readRange(pending.data(), frameSize, frameSize));
    if (!dataRead) {
        throw Error("unexpected EOF reading frame data");
    }

    size_t result = std::min(size, pending.size());
    std::memcpy(buffer, pending.data(), result);
    pos = result;
    if (pos == pending.size()) {
        pending.clear();
        pos = 0;
    }
    co_return result;
} catch (...) {
    co_return result::current_exception();
}

// AsyncFramedOutputStream

kj::Promise<Result<void>> AsyncFramedOutputStream::finish()
try {
    // Write end frame (size 0)
    uint64_t zero = 0;
    TRY_AWAIT(to.writeFull(&zero, sizeof(zero)));
    co_return result::success();
} catch (...) {
    co_return result::current_exception();
}

kj::Promise<Result<size_t>> AsyncFramedOutputStream::write(const void * src, size_t size)
try {
    if (size == 0)
        co_return size_t{0};

    // Write frame header
    uint64_t frameSize = size;
    TRY_AWAIT(to.writeFull(&frameSize, sizeof(frameSize)));

    // Write frame data
    TRY_AWAIT(to.writeFull(src, size));

    co_return size;
} catch (...) {
    co_return result::current_exception();
}

} // namespace nix

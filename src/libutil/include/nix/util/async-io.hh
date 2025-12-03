#pragma once
/**
 * @file
 *
 * @brief Async I/O stream abstractions using kj-async.
 *
 * Provides async input/output stream classes that integrate with
 * the kj event loop for non-blocking I/O operations.
 */

#include "nix/util/async.hh"
#include "nix/util/file-descriptor.hh"
#include "nix/util/ref.hh"
#include "nix/util/result.hh"
#include "nix/util/serialise.hh"

#include <kj/async-io.h>
#include <kj/async-unix.h>
#include <kj/async.h>
#include <kj/common.h>
#include <memory>
#include <string_view>
#include <vector>

namespace nix {

class AsyncOutputStream;

/**
 * I/O buffer for async stream operations.
 *
 * Provides a shared buffer that can be used by both sync and async
 * stream implementations to avoid allocation overhead.
 */
class IoBuffer
{
    std::vector<char> buffer;
    size_t readPos = 0;
    size_t writePos = 0;

public:
    explicit IoBuffer(size_t size = 32 * 1024)
        : buffer(size)
    {
    }

    char * data()
    {
        return buffer.data();
    }

    const char * data() const
    {
        return buffer.data();
    }

    size_t size() const
    {
        return buffer.size();
    }

    size_t available() const
    {
        return writePos - readPos;
    }

    size_t freeSpace() const
    {
        return buffer.size() - writePos;
    }

    void consume(size_t n)
    {
        readPos += n;
    }

    void produce(size_t n)
    {
        writePos += n;
    }

    void compact()
    {
        if (readPos > 0) {
            std::memmove(buffer.data(), buffer.data() + readPos, available());
            writePos -= readPos;
            readPos = 0;
        }
    }

    void reset()
    {
        readPos = 0;
        writePos = 0;
    }

    std::string_view readable() const
    {
        return std::string_view(buffer.data() + readPos, available());
    }

    char * writable()
    {
        return buffer.data() + writePos;
    }
};

/**
 * Abstract base class for async input streams.
 *
 * Not derived from kj's AsyncInputStream because our read semantics
 * differ slightly and we need Result-based error handling.
 */
class AsyncInputStream : private kj::AsyncObject
{
public:
    virtual ~AsyncInputStream() noexcept(false) {}

    /**
     * Read up to `size` bytes into `buffer`.
     *
     * @return The number of bytes read, or nullopt on EOF.
     *         Returns nullopt only on EOF or when size=0.
     */
    virtual kj::Promise<Result<std::optional<size_t>>> read(void * buffer, size_t size) = 0;

    /**
     * Read between `min` and `max` bytes.
     *
     * @param buffer Destination buffer
     * @param min Minimum bytes to read
     * @param max Maximum bytes to read
     * @return Number of bytes read, or nullopt if stream ended before min bytes
     */
    kj::Promise<Result<std::optional<size_t>>> readRange(void * buffer, size_t min, size_t max);

    /**
     * Read all remaining data and write to a sink.
     */
    kj::Promise<Result<void>> drainInto(Sink & sink);

    /**
     * Read all remaining data and write to an async output stream.
     */
    kj::Promise<Result<void>> drainInto(AsyncOutputStream & stream);

    /**
     * Read all remaining data into a string.
     */
    kj::Promise<Result<std::string>> drain();
};

/**
 * Async input stream wrapping a synchronous Source.
 *
 * This allows using legacy synchronous sources in async contexts.
 * The reads are not truly async - they block the event loop.
 */
class AsyncSourceInputStream : public AsyncInputStream
{
    Source & inner;
    std::unique_ptr<Source> owned;

public:
    explicit AsyncSourceInputStream(Source & inner)
        : inner(inner)
    {
    }

    explicit AsyncSourceInputStream(std::unique_ptr<Source> inner)
        : inner(*inner)
        , owned(std::move(inner))
    {
    }

    kj::Promise<Result<std::optional<size_t>>> read(void * buffer, size_t size) override;
};

/**
 * Async input stream backed by a string.
 */
class AsyncStringInputStream : public AsyncInputStream
{
    std::string_view s;

public:
    explicit AsyncStringInputStream(std::string_view s)
        : s(s)
    {
    }

    kj::Promise<Result<std::optional<size_t>>> read(void * buffer, size_t size) override;
};

/**
 * Async input stream that tees data to a sink while reading.
 */
class AsyncTeeInputStream : public AsyncInputStream
{
    AsyncInputStream & inner;
    Sink & sink;

public:
    AsyncTeeInputStream(AsyncInputStream & inner, Sink & sink)
        : inner(inner)
        , sink(sink)
    {
    }

    kj::Promise<Result<std::optional<size_t>>> read(void * buffer, size_t size) override;
};

// Note: AsyncGeneratorInputStream was removed as it requires the Generator type
// which is not present in Nix (it's a Lix addition). If needed, this can be
// added once Generator<T> is ported from Lix.

/**
 * Buffered async input stream.
 */
class AsyncBufferedInputStream : public AsyncInputStream
{
    AsyncInputStream & inner;
    ref<IoBuffer> buffer;

public:
    AsyncBufferedInputStream(AsyncInputStream & inner, ref<IoBuffer> buffer)
        : inner(inner)
        , buffer(buffer)
    {
    }

    AsyncBufferedInputStream(AsyncInputStream & inner, size_t bufSize = 32 * 1024)
        : AsyncBufferedInputStream(inner, make_ref<IoBuffer>(bufSize))
    {
    }

    AsyncBufferedInputStream(const AsyncBufferedInputStream &) = delete;
    AsyncBufferedInputStream & operator=(const AsyncBufferedInputStream &) = delete;

    kj::Promise<Result<std::optional<size_t>>> read(void * data, size_t size) override;

    IoBuffer & getBuffer()
    {
        return *buffer;
    }
};

/**
 * Abstract base class for async output streams.
 */
class AsyncOutputStream : private kj::AsyncObject
{
public:
    virtual ~AsyncOutputStream() noexcept(false) {}

    /**
     * Write up to `size` bytes from `src`.
     *
     * @return The number of bytes written
     */
    virtual kj::Promise<Result<size_t>> write(const void * src, size_t size) = 0;

    /**
     * Write exactly `size` bytes from `src`.
     */
    kj::Promise<Result<void>> writeFull(const void * src, size_t size)
    {
        return write(src, size).then([this, src, size](Result<size_t> wrote) -> kj::Promise<Result<void>> {
            if (!wrote.has_value()) {
                return {wrote.error()};
            } else if (wrote.value() == size) {
                return {result::success()};
            } else {
                return writeFull(static_cast<const char *>(src) + wrote.value(), size - wrote.value());
            }
        });
    }
};

/**
 * Buffered async output stream.
 */
class AsyncBufferedOutputStream : public AsyncOutputStream
{
    AsyncOutputStream & inner;
    ref<IoBuffer> buffer;

public:
    AsyncBufferedOutputStream(AsyncOutputStream & inner, ref<IoBuffer> buffer)
        : inner(inner)
        , buffer(buffer)
    {
    }

    AsyncBufferedOutputStream(AsyncOutputStream & inner, size_t bufSize = 32 * 1024)
        : AsyncBufferedOutputStream(inner, make_ref<IoBuffer>(bufSize))
    {
    }

    AsyncBufferedOutputStream(const AsyncBufferedOutputStream &) = delete;
    AsyncBufferedOutputStream & operator=(const AsyncBufferedOutputStream &) = delete;

    kj::Promise<Result<size_t>> write(const void * src, size_t size) override;
    kj::Promise<Result<void>> flush();

    IoBuffer & getBuffer()
    {
        return *buffer;
    }
};

/**
 * Bidirectional async stream (both input and output).
 */
class AsyncStream : public AsyncInputStream, public AsyncOutputStream
{};

/**
 * File descriptor blocking state for restoration.
 */
enum class FdBlockingState { Blocking, NonBlocking };

/**
 * Async stream backed by a file descriptor.
 *
 * Uses kj::UnixEventPort::FdObserver for async I/O notifications.
 * The FD is set to non-blocking mode and restored on destruction.
 */
class AsyncFdIoStream : public AsyncStream
{
    int fd;
    FdBlockingState oldState;
    AutoCloseFD ownedFd;
    kj::UnixEventPort::FdObserver observer;

public:
    /**
     * Tag type indicating the FD is shared (not owned).
     */
    struct shared_fd
    {};

    /**
     * Create stream from an owned FD (will be closed on destruction).
     */
    explicit AsyncFdIoStream(AutoCloseFD fd);

    /**
     * Create stream from a shared FD (will not be closed).
     */
    AsyncFdIoStream(shared_fd, int fd);

    ~AsyncFdIoStream() noexcept(false);

    int getFD() const
    {
        return fd;
    }

    kj::Promise<Result<std::optional<size_t>>> read(void * tgt, size_t size) override;
    kj::Promise<Result<size_t>> write(const void * src, size_t size) override;
};

/**
 * Stream that reads chunked/framed data back into logical form.
 *
 * Use with AsyncFramedOutputStream to guarantee known stream state
 * even in error cases.
 */
class AsyncFramedInputStream : public AsyncInputStream
{
private:
    AsyncInputStream & from;
    bool eof = false;
    std::vector<char> pending;
    size_t pos = 0;

public:
    explicit AsyncFramedInputStream(AsyncInputStream & from)
        : from(from)
    {
    }

    ~AsyncFramedInputStream();

    kj::Promise<Result<void>> finish();

    kj::Promise<Result<std::optional<size_t>>> read(void * buffer, size_t size) override;
};

/**
 * Stream that writes data as chunks/frames.
 *
 * The framing format allows the receiver to know when the logical
 * stream ends and recover the underlying stream state.
 */
class AsyncFramedOutputStream : public AsyncOutputStream
{
    AsyncOutputStream & to;

public:
    explicit AsyncFramedOutputStream(AsyncOutputStream & to)
        : to(to)
    {
    }

    kj::Promise<Result<void>> finish();

    kj::Promise<Result<size_t>> write(const void * src, size_t size) override;
};

} // namespace nix

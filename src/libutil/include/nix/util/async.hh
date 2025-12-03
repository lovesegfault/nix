#pragma once
/**
 * @file
 *
 * @brief Core async infrastructure using kj-async.
 *
 * This provides the foundational abstractions for stackless coroutines:
 * - AsyncContext: Thread-local async I/O context
 * - AsyncIoRoot: Event loop initialization and blocking bridge
 * - TRY_AWAIT: Error propagation macro with async stack traces
 */

#include "nix/util/error.hh"
#include "nix/util/result.hh"
#include "nix/util/signals.hh"

#include <future>
#include <kj/async-io.h>
#include <kj/async.h>
#include <kj/time.h>
#include <optional>
#include <source_location>
#include <type_traits>

namespace nix {

/**
 * Thread-local async I/O context.
 *
 * Each thread that performs async I/O operations must have an AsyncContext.
 * The context provides access to the kj async I/O providers and event port.
 *
 * Access the current thread's context via the AIO() function.
 */
struct AsyncContext
{
    /**
     * Pointer to the current thread's AsyncContext, or nullptr if none.
     */
    static inline thread_local AsyncContext * current = nullptr;

    kj::AsyncIoProvider & provider;
    kj::LowLevelAsyncIoProvider & lowLevelProvider;
    kj::UnixEventPort & unixEventPort;

    explicit AsyncContext(kj::AsyncIoContext & aio)
        : provider(*aio.provider)
        , lowLevelProvider(*aio.lowLevelProvider)
        , unixEventPort(aio.unixEventPort)
    {
        assert(current == nullptr);
        current = this;
    }

    ~AsyncContext()
    {
        current = nullptr;
    }

    AsyncContext(const AsyncContext &) = delete;
    AsyncContext & operator=(const AsyncContext &) = delete;
    AsyncContext(AsyncContext &&) = delete;
    AsyncContext & operator=(AsyncContext &&) = delete;

    /**
     * Wrap a promise in a timeout.
     *
     * For Result<void> promises, returns Result<bool> where true means
     * the wrapped promise ran to completion and false means it timed out.
     *
     * For other promises, returns Result<std::optional<T>> where nullopt
     * indicates a timeout.
     *
     * @param timeout The duration after which to timeout
     * @param p The promise to wrap
     * @return A promise that resolves when either p completes or times out
     */
    template<typename T>
    auto timeoutAfter(kj::Duration timeout, kj::Promise<Result<T>> && p)
    {
        using RetT = std::conditional_t<std::is_void_v<T>, bool, std::optional<T>>;
        return p
            .then([](Result<T> r) -> Result<RetT> {
                if (r.has_value()) {
                    if constexpr (std::is_void_v<T>) {
                        return true;
                    } else {
                        return std::move(r.value());
                    }
                } else {
                    return r.error();
                }
            })
            .exclusiveJoin(provider.getTimer().afterDelay(timeout).then([]() -> Result<RetT> { return RetT{}; }));
    }
};

/**
 * Root of the async I/O system for a thread.
 *
 * Create one of these to initialize the kj event loop and async context.
 * Use blockOn() to synchronously wait for async operations to complete.
 */
struct AsyncIoRoot
{
    kj::AsyncIoContext kj;
    AsyncContext context;

    AsyncIoRoot()
        : kj(kj::setupAsyncIo())
        , context(kj)
    {
    }

    AsyncIoRoot(const AsyncIoRoot &) = delete;
    AsyncIoRoot & operator=(const AsyncIoRoot &) = delete;
    AsyncIoRoot(AsyncIoRoot &&) = delete;
    AsyncIoRoot & operator=(AsyncIoRoot &&) = delete;

    /**
     * Synchronously block on a promise, waiting for it to complete.
     *
     * This is the bridge between sync and async code. It runs the event
     * loop until the promise is fulfilled, then returns the result.
     *
     * @note This checks for user interrupts before waiting.
     *
     * @param promise The promise to wait for
     * @param call_site Source location for async trace (auto-captured)
     * @return The result of the promise
     */
    template<typename T>
    auto blockOn(kj::Promise<T> && promise, std::source_location call_site = std::source_location::current());
};

/**
 * Get the current thread's AsyncContext.
 *
 * @pre An AsyncContext must exist for the current thread
 * @return Reference to the current AsyncContext
 */
inline AsyncContext & AIO()
{
    assert(AsyncContext::current != nullptr);
    return *AsyncContext::current;
}

namespace detail {

inline void materializeResult(Result<void> r)
{
    r.value();
}

template<typename T>
inline T materializeResult(Result<T> r)
{
    return std::move(r.value());
}

template<typename T>
T runAsyncUnwrap(T t)
{
    return t;
}

inline void runAsyncUnwrap(void) {}

template<typename T>
T runAsyncUnwrap(Result<T> t)
{
    return std::move(t).value();
}

inline void runAsyncUnwrap(Result<void> t)
{
    t.value();
}

} // namespace detail

} // namespace nix

/**
 * Await an async operation with error handling and async trace support.
 *
 * This macro:
 * 1. co_awaits the expression
 * 2. If the result is an error, adds async trace info and rethrows
 * 3. Otherwise, returns the unwrapped value
 *
 * @param _l_ctx A function returning optional context description
 * @param _l_map A function to transform the awaited result
 * @param ... The expression to await
 */
#define NIX_TRY_AWAIT_CONTEXT_MAP(_l_ctx, _l_map, ...)                         \
    ({                                                                         \
        auto _nix_awaited = (_l_map) (co_await (__VA_ARGS__));                 \
        if (_nix_awaited.has_error()) {                                        \
            try {                                                              \
                _nix_awaited.value();                                          \
            } catch (::nix::BaseError & e) {                                   \
                e.addAsyncTrace(::std::source_location::current(), _l_ctx());  \
                throw;                                                         \
            } catch (::kj::Exception & e) {                                    \
                ::nix::Error fe{e.getDescription().cStr()};                    \
                fe.addAsyncTrace(::std::source_location::current(), _l_ctx()); \
                throw fe;                                                      \
            } catch (...) {                                                    \
                ::nix::Error fe{"unknown async error"};                        \
                fe.addAsyncTrace(::std::source_location::current(), _l_ctx()); \
                throw fe;                                                      \
            }                                                                  \
        }                                                                      \
        ::nix::detail::materializeResult(std::move(_nix_awaited));             \
    })

/**
 * Await an async operation with error handling.
 *
 * @param _l_ctx A function returning optional context description
 * @param ... The expression to await
 */
#define NIX_TRY_AWAIT_CONTEXT(_l_ctx, ...) NIX_TRY_AWAIT_CONTEXT_MAP(_l_ctx, (std::identity{}), __VA_ARGS__)

/**
 * Default context function for TRY_AWAIT.
 *
 * This name is looked up in local scope by TRY_AWAIT to provide
 * optional additional context for async trace frames. Override
 * this in your function to provide custom context.
 */
static constexpr std::optional<std::string> nixAsyncTaskContext()
{
    return std::nullopt;
}

/**
 * Await an async operation with default context.
 *
 * This is the primary macro for awaiting async operations.
 * It handles error propagation and async stack traces automatically.
 *
 * Usage:
 * @code
 * auto result = TRY_AWAIT(someAsyncOperation());
 * @endcode
 */
#define TRY_AWAIT(...) NIX_TRY_AWAIT_CONTEXT(nixAsyncTaskContext, __VA_ARGS__)

namespace nix {

template<typename T>
inline auto AsyncIoRoot::blockOn(kj::Promise<T> && promise, std::source_location call_site)
try {
    // Always check for user interrupts. Since this is C++ we must always be
    // prepared for exceptions, which is why RAII is so important. Interruptions
    // are also exceptions, so all exception-safe (and for promises, cancellation-
    // safe) code is automatically interruption-safe.
    checkInterrupt();
    if constexpr (std::is_void_v<T>) {
        promise.wait(kj.waitScope);
    } else {
        return detail::runAsyncUnwrap(promise.wait(kj.waitScope));
    }
} catch (BaseError & e) {
    e.addAsyncTrace(call_site);
    throw;
} catch (...) {
    Error fe{"unknown error in blockOn"};
    fe.addAsyncTrace(call_site);
    throw fe;
}

} // namespace nix

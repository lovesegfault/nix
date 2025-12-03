#pragma once
/**
 * @file
 *
 * @brief Async semaphore for limiting concurrent operations.
 *
 * Provides an async-aware semaphore that can be used to limit the
 * number of concurrent async operations (e.g., parallel builds,
 * substitutions, network connections).
 */

#include <cassert>
#include <kj/async.h>
#include <kj/common.h>
#include <kj/exception.h>
#include <kj/list.h>
#include <kj/source-location.h>
#include <memory>
#include <optional>

namespace nix {

/**
 * An async semaphore for limiting concurrency.
 *
 * This semaphore integrates with the kj event loop, allowing coroutines
 * to await permits without blocking a thread.
 *
 * Usage:
 * @code
 * AsyncSemaphore sem(5);  // Allow 5 concurrent operations
 *
 * auto token = co_await sem.acquire();
 * // ... do work with the acquired permit ...
 * // Token automatically releases when destroyed
 * @endcode
 */
class AsyncSemaphore
{
public:
    /**
     * RAII token representing an acquired semaphore permit.
     *
     * When destroyed, the permit is automatically released back to
     * the semaphore, potentially waking a waiting coroutine.
     */
    class [[nodiscard("destroying a semaphore token releases the permit immediately")]] Token
    {
        struct Release
        {
            void operator()(AsyncSemaphore * sem) const
            {
                sem->unsafeRelease();
            }
        };

        std::unique_ptr<AsyncSemaphore, Release> parent;

    public:
        Token() = default;

        Token(AsyncSemaphore & parent, kj::Badge<AsyncSemaphore>)
            : parent(&parent)
        {
        }

        /**
         * Check if this token holds a valid permit.
         */
        bool valid() const
        {
            return parent != nullptr;
        }
    };

private:
    struct Waiter
    {
        kj::PromiseFulfiller<Token> & fulfiller;
        kj::ListLink<Waiter> link;
        kj::List<Waiter, &Waiter::link> & list;

        Waiter(kj::PromiseFulfiller<Token> & fulfiller, kj::List<Waiter, &Waiter::link> & list)
            : fulfiller(fulfiller)
            , list(list)
        {
            list.add(*this);
        }

        ~Waiter()
        {
            if (link.isLinked()) {
                list.remove(*this);
            }
        }
    };

    const unsigned capacity_;
    unsigned used_ = 0;
    kj::List<Waiter, &Waiter::link> waiters;

    void unsafeRelease()
    {
        used_ -= 1;
        while (used_ < capacity_ && !waiters.empty()) {
            used_ += 1;
            auto & w = waiters.front();
            w.fulfiller.fulfill(Token{*this, {}});
            waiters.remove(w);
        }
    }

public:
    /**
     * Create a semaphore with the given capacity.
     *
     * @param capacity Maximum number of concurrent permits
     */
    explicit AsyncSemaphore(unsigned capacity)
        : capacity_(capacity)
    {
    }

    AsyncSemaphore(const AsyncSemaphore &) = delete;
    AsyncSemaphore & operator=(const AsyncSemaphore &) = delete;

    ~AsyncSemaphore()
    {
        assert(waiters.empty() && "destroyed a semaphore with active waiters");
    }

    /**
     * Try to acquire a permit without waiting.
     *
     * @return A Token if a permit was available, nullopt otherwise
     */
    std::optional<Token> tryAcquire()
    {
        if (used_ < capacity_) {
            used_ += 1;
            return Token{*this, {}};
        } else {
            return {};
        }
    }

    /**
     * Acquire a permit, waiting if necessary.
     *
     * @return A promise that resolves to a Token when a permit is available
     */
    kj::Promise<Token> acquire()
    {
        if (auto t = tryAcquire()) {
            return std::move(*t);
        } else {
            return kj::newAdaptedPromise<Token, Waiter>(waiters);
        }
    }

    /**
     * Get the maximum number of permits.
     */
    unsigned capacity() const
    {
        return capacity_;
    }

    /**
     * Get the number of currently held permits.
     */
    unsigned used() const
    {
        return used_;
    }

    /**
     * Get the number of available permits.
     */
    unsigned available() const
    {
        return capacity_ - used_;
    }
};

} // namespace nix

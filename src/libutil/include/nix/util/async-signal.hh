#pragma once
/**
 * @file
 *
 * @brief Integration between async operations and signal handling.
 *
 * Provides makeInterruptible() which wraps a promise to be cancellable
 * when the user sends an interrupt signal (Ctrl-C / SIGINT).
 */

#include "nix/util/async.hh"
#include "nix/util/result.hh"
#include "nix/util/signals.hh"

#include <kj/async.h>

namespace nix {

/**
 * Wrap a promise to make it cancellable on user interrupt (Ctrl-C).
 *
 * When the user interrupts the process (e.g., via SIGINT), the returned
 * promise will be rejected with an Interrupted exception, and the wrapped
 * promise will be cancelled.
 *
 * This should be used for long-running async operations that the user
 * should be able to cancel.
 *
 * @param promise The promise to wrap
 * @return A promise that can be interrupted by SIGINT
 *
 * Usage:
 * @code
 * auto result = TRY_AWAIT(makeInterruptible(someLongOperation()));
 * @endcode
 */
template<typename T>
kj::Promise<Result<T>> makeInterruptible(kj::Promise<Result<T>> promise)
{
    // Create a cross-thread fulfiller that can be triggered from the signal handler
    auto paf = kj::newPromiseAndCrossThreadFulfiller<Result<T>>();

    // Register an interrupt callback that rejects the promise
    auto callback = createInterruptCallback([fulfiller = paf.fulfiller.get()]() {
        try {
            throw Interrupted("interrupted by user");
        } catch (...) {
            fulfiller->fulfill(result::current_exception());
        }
    });

    // Race the original promise against the interrupt promise
    return promise.exclusiveJoin(std::move(paf.promise)).attach(std::move(paf.fulfiller)).attach(std::move(callback));
}

/**
 * Wrap a void-returning promise to make it cancellable on user interrupt.
 */
inline kj::Promise<Result<void>> makeInterruptible(kj::Promise<Result<void>> promise)
{
    auto paf = kj::newPromiseAndCrossThreadFulfiller<Result<void>>();

    auto callback = createInterruptCallback([fulfiller = paf.fulfiller.get()]() {
        try {
            throw Interrupted("interrupted by user");
        } catch (...) {
            fulfiller->fulfill(result::current_exception());
        }
    });

    return promise.exclusiveJoin(std::move(paf.promise)).attach(std::move(paf.fulfiller)).attach(std::move(callback));
}

} // namespace nix

#pragma once
/**
 * @file
 *
 * @brief Utilities for collecting results from multiple async operations.
 *
 * Provides AsyncCollect for gathering promise results in completion order
 * with fail-fast semantics, plus helper functions for common patterns.
 */

#include "nix/util/result.hh"

#include <concepts>
#include <kj/async.h>
#include <kj/common.h>
#include <kj/vector.h>
#include <list>
#include <optional>
#include <type_traits>

namespace nix {

/**
 * Collects results from multiple promises in completion order.
 *
 * This class manages a set of promises and allows iterating over their
 * results as they complete. If any promise fails (throws an exception),
 * all remaining promises are cancelled and the exception is propagated.
 *
 * @tparam K Key type to identify which promise completed
 * @tparam V Value type returned by the promises
 */
template<typename K, typename V>
class AsyncCollect
{
public:
    using Item = std::conditional_t<std::is_void_v<V>, K, std::pair<K, V>>;

private:
    kj::ForkedPromise<void> allPromises;
    std::list<Item> results;
    size_t remaining;

    kj::ForkedPromise<void> signal;
    kj::Maybe<kj::Own<kj::PromiseFulfiller<void>>> notify;

    void oneDone(Item item)
    {
        results.emplace_back(std::move(item));
        remaining -= 1;
        KJ_IF_MAYBE (n, notify) {
            (*n)->fulfill();
            notify = nullptr;
        }
    }

    kj::Promise<void> collectorFor(K key, kj::Promise<V> promise)
    {
        if constexpr (std::is_void_v<V>) {
            return promise.then([this, key{std::move(key)}] { oneDone(std::move(key)); });
        } else {
            return promise.then([this, key{std::move(key)}](V v) { oneDone(Item{std::move(key), std::move(v)}); });
        }
    }

    kj::ForkedPromise<void> waitForAll(kj::Array<std::pair<K, kj::Promise<V>>> & promises)
    {
        kj::Vector<kj::Promise<void>> wrappers;
        for (auto & [key, promise] : promises) {
            wrappers.add(collectorFor(std::move(key), std::move(promise)));
        }

        return kj::joinPromisesFailFast(wrappers.releaseAsArray()).fork();
    }

public:
    explicit AsyncCollect(kj::Array<std::pair<K, kj::Promise<V>>> && promises)
        : allPromises(waitForAll(promises))
        , remaining(promises.size())
        , signal{nullptr}
    {
    }

    // oneDone promises capture `this`
    AsyncCollect(const AsyncCollect &) = delete;
    AsyncCollect & operator=(const AsyncCollect &) = delete;

    /**
     * Get the next completed result.
     *
     * @return The next completed item, or nullopt if all promises are done
     */
    kj::Promise<std::optional<Item>> next()
    {
        if (remaining == 0 && results.empty()) {
            return {std::nullopt};
        }

        if (!results.empty()) {
            auto result = std::move(results.front());
            results.pop_front();
            return {{std::move(result)}};
        }

        if (notify == nullptr) {
            auto pair = kj::newPromiseAndFulfiller<void>();
            notify = std::move(pair.fulfiller);
            signal = pair.promise.fork();
        }

        return signal.addBranch().exclusiveJoin(allPromises.addBranch()).then([this] { return next(); });
    }
};

/**
 * Create an AsyncCollect from an array of key-promise pairs.
 *
 * @param promises Array of (key, promise) pairs
 * @return An AsyncCollect that yields results in completion order
 */
template<typename K, typename V>
AsyncCollect<K, V> asyncCollect(kj::Array<std::pair<K, kj::Promise<V>>> promises)
{
    return AsyncCollect<K, V>(std::move(promises));
}

/**
 * Run an async function over each item in a range.
 *
 * Uses fail-fast semantics: if any call fails, all pending calls are
 * cancelled and the error is returned.
 *
 * @param input The range of items to process
 * @param fn A function taking an item and returning Promise<Result<void>>
 * @return Promise that resolves when all items are processed or fails on first error
 */
template<typename Input, typename Fn>
kj::Promise<Result<void>> asyncSpread(Input && input, Fn fn)
    requires requires {
        { fn(*begin(input)) } -> std::same_as<kj::Promise<Result<void>>>;
    }
{
    kj::Vector<std::pair<std::tuple<>, kj::Promise<Result<void>>>> children;
    if constexpr (requires { input.size(); }) {
        children.reserve(input.size());
    }

    for (auto & i : input) {
        children.add(std::tuple(), fn(i));
    }

    auto collect = asyncCollect(children.releaseAsArray());
    while (auto r = co_await collect.next()) {
        if (!r->second.has_value()) {
            co_return std::move(r->second);
        }
    }

    co_return result::success();
}

/**
 * Run multiple Result-returning promises concurrently.
 *
 * All promises run until completion or the first failure.
 * Similar to kj::joinPromisesFailFast but works with Result types.
 *
 * @param promises Variadic list of Promise<Result<void>>
 * @return Promise that resolves when all succeed or fails on first error
 */
template<typename... Promises>
kj::Promise<Result<void>> asyncJoin(Promises &&... promises)
    requires(std::same_as<Promises, kj::Promise<Result<void>>> && ...)
{
    auto collect = asyncCollect(kj::arr(std::pair{std::tuple{}, std::forward<Promises>(promises)}...));
    while (auto r = co_await collect.next()) {
        if (!r->second.has_value()) {
            co_return std::move(r->second);
        }
    }

    co_return result::success();
}

} // namespace nix

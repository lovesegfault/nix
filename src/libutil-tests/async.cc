#include "nix/util/async.hh"
#include "nix/util/result.hh"

#include <gtest/gtest.h>
#include <kj/async.h>

namespace nix {

/* ----------------------------------------------------------------------------
 * Result<T> Tests
 * --------------------------------------------------------------------------*/

TEST(Result, successVoid)
{
    Result<void> r = result::success();
    ASSERT_TRUE(r.has_value());
}

TEST(Result, successValue)
{
    Result<int> r = 42;
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(r.value(), 42);
}

TEST(Result, failureFromException)
{
    Result<int> r = result::failure(std::make_exception_ptr(std::runtime_error("test error")));
    ASSERT_FALSE(r.has_value());
    ASSERT_THROW(r.value(), std::runtime_error);
}

TEST(Result, currentException)
{
    std::optional<Result<int>> r;
    try {
        throw std::runtime_error("test error");
    } catch (...) {
        r = result::current_exception();
    }
    ASSERT_TRUE(r.has_value());
    ASSERT_FALSE(r->has_value());
    ASSERT_THROW(r->value(), std::runtime_error);
}

/* ----------------------------------------------------------------------------
 * AsyncContext Tests
 * --------------------------------------------------------------------------*/

TEST(AsyncContext, currentIsNullWithoutInit)
{
    // Before any AsyncIoRoot is created, current should be nullptr
    // Note: This test assumes no other test has left an AsyncContext active
    // In practice, we'd need to save/restore the context
    ASSERT_EQ(AsyncContext::current, nullptr);
}

TEST(AsyncContext, asyncIoRootSetsContext)
{
    {
        AsyncIoRoot root;
        ASSERT_NE(AsyncContext::current, nullptr);
        ASSERT_EQ(&AIO(), AsyncContext::current);
    }
    // After destruction, current should be nullptr again
    ASSERT_EQ(AsyncContext::current, nullptr);
}

/* ----------------------------------------------------------------------------
 * BlockOn Tests
 * --------------------------------------------------------------------------*/

TEST(AsyncIoRoot, blockOnImmediateValue)
{
    AsyncIoRoot root;

    // Block on a promise that immediately resolves
    auto result = root.blockOn(kj::Promise<int>(42));
    ASSERT_EQ(result, 42);
}

TEST(AsyncIoRoot, blockOnResult)
{
    AsyncIoRoot root;

    // Block on a promise that returns a Result
    auto result = root.blockOn(kj::Promise<Result<int>>(Result<int>(42)));
    ASSERT_EQ(result, 42);
}

TEST(AsyncIoRoot, blockOnVoid)
{
    AsyncIoRoot root;

    bool executed = false;
    root.blockOn(kj::Promise<void>(kj::READY_NOW).then([&]() { executed = true; }));
    ASSERT_TRUE(executed);
}

/* ----------------------------------------------------------------------------
 * Promise Chaining Tests
 * --------------------------------------------------------------------------*/

TEST(AsyncIoRoot, promiseChaining)
{
    AsyncIoRoot root;

    auto promise = kj::Promise<int>(10).then([](int x) { return x * 2; }).then([](int x) { return x + 5; });

    auto result = root.blockOn(std::move(promise));
    ASSERT_EQ(result, 25); // (10 * 2) + 5
}

TEST(AsyncIoRoot, promiseWithException)
{
    AsyncIoRoot root;

    auto promise = kj::Promise<int>(10).then([](int) -> int { throw std::runtime_error("test error"); });

    ASSERT_THROW(root.blockOn(std::move(promise)), Error);
}

} // namespace nix

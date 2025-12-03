#include "nix/util/async.hh"
#include "nix/util/async-semaphore.hh"

#include <gtest/gtest.h>

namespace nix {

/* ----------------------------------------------------------------------------
 * AsyncSemaphore Tests
 * --------------------------------------------------------------------------*/

TEST(AsyncSemaphore, initialState)
{
    AsyncSemaphore sem(5);

    ASSERT_EQ(sem.capacity(), 5u);
    ASSERT_EQ(sem.used(), 0u);
    ASSERT_EQ(sem.available(), 5u);
}

TEST(AsyncSemaphore, tryAcquireSuccess)
{
    AsyncSemaphore sem(2);

    auto token1 = sem.tryAcquire();
    ASSERT_TRUE(token1.has_value());
    ASSERT_TRUE(token1->valid());
    ASSERT_EQ(sem.used(), 1u);
    ASSERT_EQ(sem.available(), 1u);

    auto token2 = sem.tryAcquire();
    ASSERT_TRUE(token2.has_value());
    ASSERT_EQ(sem.used(), 2u);
    ASSERT_EQ(sem.available(), 0u);
}

TEST(AsyncSemaphore, tryAcquireFailsAtCapacity)
{
    AsyncSemaphore sem(1);

    auto token1 = sem.tryAcquire();
    ASSERT_TRUE(token1.has_value());

    auto token2 = sem.tryAcquire();
    ASSERT_FALSE(token2.has_value());
}

TEST(AsyncSemaphore, tokenReleasesOnDestruction)
{
    AsyncSemaphore sem(1);

    {
        auto token = sem.tryAcquire();
        ASSERT_TRUE(token.has_value());
        ASSERT_EQ(sem.available(), 0u);
    }

    // Token destroyed, semaphore should be released
    ASSERT_EQ(sem.available(), 1u);
}

TEST(AsyncSemaphore, acquireAsync)
{
    AsyncIoRoot root;
    AsyncSemaphore sem(1);

    // First acquire should succeed immediately
    auto promise1 = sem.acquire();
    auto token1 = root.blockOn(std::move(promise1));
    ASSERT_TRUE(token1.valid());
    ASSERT_EQ(sem.available(), 0u);

    // Release the token
    token1 = AsyncSemaphore::Token();
    ASSERT_EQ(sem.available(), 1u);
}

TEST(AsyncSemaphore, multipleAcquiresAndReleases)
{
    AsyncSemaphore sem(3);

    std::vector<std::optional<AsyncSemaphore::Token>> tokens;

    // Acquire all 3
    for (int i = 0; i < 3; i++) {
        tokens.push_back(sem.tryAcquire());
        ASSERT_TRUE(tokens.back().has_value());
    }

    ASSERT_EQ(sem.available(), 0u);

    // Can't acquire more
    ASSERT_FALSE(sem.tryAcquire().has_value());

    // Release one
    tokens.pop_back();
    ASSERT_EQ(sem.available(), 1u);

    // Can acquire again
    auto token = sem.tryAcquire();
    ASSERT_TRUE(token.has_value());
}

} // namespace nix

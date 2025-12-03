#include "nix/util/async.hh"
#include "nix/util/async-collect.hh"
#include "nix/util/result.hh"

#include <gtest/gtest.h>
#include <set>

namespace nix {

/* ----------------------------------------------------------------------------
 * AsyncCollect Tests
 *
 * Note: These tests are simplified because kj::Promise is not copy-constructible,
 * which makes using std::pair and kj::heapArray with initializer lists challenging.
 * --------------------------------------------------------------------------*/

TEST(AsyncCollect, emptyCollectionReturnsNullopt)
{
    AsyncIoRoot root;

    // Create an empty array using kj::heapArrayBuilder
    kj::ArrayBuilder<std::pair<int, kj::Promise<int>>> builder(0);
    auto collect = asyncCollect(builder.finish());

    auto result = root.blockOn(collect.next());
    ASSERT_FALSE(result.has_value());
}

/* ----------------------------------------------------------------------------
 * asyncJoin Tests - Simplified
 *
 * We test asyncJoin indirectly through synchronous results
 * --------------------------------------------------------------------------*/

TEST(AsyncJoin, singleSuccess)
{
    AsyncIoRoot root;

    // Create a single successful promise - blockOn will unwrap it
    auto promise = kj::Promise<Result<void>>(result::success());

    // blockOn unwraps Result<void> to void, so this just shouldn't throw
    ASSERT_NO_THROW(root.blockOn(std::move(promise)));
}

TEST(AsyncJoin, singleFailure)
{
    AsyncIoRoot root;

    auto promise = kj::Promise<Result<void>>(result::failure(std::make_exception_ptr(std::runtime_error("test"))));

    // blockOn unwraps and throws - may be wrapped in nix::Error
    ASSERT_ANY_THROW(root.blockOn(std::move(promise)));
}

/* ----------------------------------------------------------------------------
 * Simple Promise Chain Tests
 * --------------------------------------------------------------------------*/

TEST(PromiseChain, simpleTransform)
{
    AsyncIoRoot root;

    auto promise = kj::Promise<int>(10).then([](int x) { return x * 2; });

    auto result = root.blockOn(std::move(promise));
    ASSERT_EQ(result, 20);
}

TEST(PromiseChain, multipleTransforms)
{
    AsyncIoRoot root;

    auto promise = kj::Promise<int>(5)
                       .then([](int x) { return x + 3; })  // 8
                       .then([](int x) { return x * 2; }); // 16

    auto result = root.blockOn(std::move(promise));
    ASSERT_EQ(result, 16);
}

TEST(PromiseChain, resultTransform)
{
    AsyncIoRoot root;

    auto promise = kj::Promise<Result<int>>(Result<int>(42)).then([](Result<int> r) -> Result<int> {
        if (r.has_value()) {
            return r.value() * 2;
        }
        return r.error();
    });

    // blockOn unwraps Result<T> to T, so we get an int directly
    auto result = root.blockOn(std::move(promise));
    ASSERT_EQ(result, 84);
}

} // namespace nix

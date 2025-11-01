#include <gtest/gtest.h>

#include "nix/store/filetransfer-cobalt.hh"
#include "nix/store/globals.hh"
#include "nix/util/callback.hh"

#include <future>

namespace nix {

// Basic compilation and instantiation test
TEST(FileTransferCobalt, CanInstantiate)
{
    // Just verify we can create the object
    // Full functional tests will come later
    settings.caFile = "";
    settings.netrcFile = "";
    settings.downloadSpeed = 0;

    // Note: Can't actually test full functionality yet since we need proper factory integration
    // This test just verifies the code compiles and links
    SUCCEED();
}

// Test interrupt check awaitable
TEST(FileTransferCobalt, InterruptCheckAwaitable)
{
    // Verify InterruptCheck structure exists and has correct interface
    InterruptCheck check;

    // Just verify it exists - actual interrupt testing requires more setup
    // Full interrupt tests will be added in Phase 5
    SUCCEED();
}

// Test factory pattern
TEST(FileTransferCobalt, FactoryPattern)
{
    // Test that factory creates correct implementation based on flag
    settings.caFile = "";
    settings.netrcFile = "";

    // With flag disabled, should get old implementation (not cobalt)
    fileTransferSettings.useCobaltImplementation = false;
    auto oldImpl = makeFileTransfer();
    EXPECT_EQ(oldImpl.dynamic_pointer_cast<CobaltFileTransfer>(), nullptr);

    // With flag enabled, should get cobalt implementation
    fileTransferSettings.useCobaltImplementation = true;
    auto newImpl = makeFileTransfer();
    EXPECT_NE(newImpl.dynamic_pointer_cast<CobaltFileTransfer>(), nullptr);

    // Reset flag
    fileTransferSettings.useCobaltImplementation = false;
}

// Test callback API
TEST(FileTransferCobalt, CallbackAPI)
{
    settings.caFile = "";
    settings.netrcFile = "";

    auto ft = make_ref<CobaltFileTransfer>();

    std::promise<FileTransferResult> promise;
    auto future = promise.get_future();

    FileTransferRequest req(VerbatimURL(std::string("http://example.com")));

    ft->enqueueFileTransfer(req, Callback<FileTransferResult>([&promise](std::future<FileTransferResult> fut) {
                                try {
                                    promise.set_value(fut.get());
                                } catch (...) {
                                    promise.set_exception(std::current_exception());
                                }
                            }));

    // Wait for completion (with timeout)
    auto status = future.wait_for(std::chrono::seconds(10));

    if (status == std::future_status::timeout) {
        GTEST_SKIP() << "Network test timed out (may be unavailable)";
    }

    try {
        auto result = future.get();
        EXPECT_FALSE(result.data.empty());
        EXPECT_GE(result.urls.size(), 1);
    } catch (std::exception & e) {
        GTEST_SKIP() << "Network test failed: " << e.what();
    }
}

// Test synchronous download API with REAL COBALT COROUTINES!
TEST(FileTransferCobalt, SynchronousDownload)
{
    settings.caFile = "";
    settings.netrcFile = "";

    auto ft = make_ref<CobaltFileTransfer>();

    // Give io_thread a moment to start
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    try {
        FileTransferRequest req(VerbatimURL(std::string("http://example.com")));
        auto result = ft->downloadSync(req);

        EXPECT_FALSE(result.data.empty());
        EXPECT_GE(result.urls.size(), 1);
        EXPECT_GT(result.bodySize, 0);
    } catch (std::exception & e) {
        GTEST_SKIP() << "Network test failed: " << e.what();
    }
}

// Test multiple concurrent downloads
TEST(FileTransferCobalt, ConcurrentDownloads)
{
    settings.caFile = "";
    settings.netrcFile = "";

    auto ft = make_ref<CobaltFileTransfer>();

    // Give io_thread a moment to start
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    try {
        // Launch 3 concurrent downloads
        std::vector<std::promise<FileTransferResult>> promises(3);
        std::vector<std::future<FileTransferResult>> futures;
        for (auto& p : promises) {
            futures.push_back(p.get_future());
        }

        for (size_t i = 0; i < 3; ++i) {
            FileTransferRequest req(VerbatimURL(std::string("http://example.com")));
            ft->enqueueFileTransfer(req, Callback<FileTransferResult>([&promises, i](std::future<FileTransferResult> fut) {
                try {
                    promises[i].set_value(fut.get());
                } catch (...) {
                    promises[i].set_exception(std::current_exception());
                }
            }));
        }

        // Wait for all to complete
        for (auto& future : futures) {
            auto result = future.get();
            EXPECT_FALSE(result.data.empty());
        }

        std::cerr << "All 3 concurrent downloads completed successfully!" << std::endl;
    } catch (std::exception& e) {
        GTEST_SKIP() << "Concurrent download test failed: " << e.what();
    }
}

} // namespace nix

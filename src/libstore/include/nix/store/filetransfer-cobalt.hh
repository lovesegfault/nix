#pragma once
///@file

#include "nix/store/filetransfer.hh"
#include "nix/util/sync.hh"

#include <boost/asio.hpp>
#include <boost/cobalt.hpp>
#include <curl/curl.h>

#include <map>
#include <memory>
#include <thread>

namespace nix {

/**
 * Bridges libcurl's multi interface with boost::asio event loop.
 *
 * Pattern inspired by curl's asiohiper.cpp example, with improvements:
 * - Use weak_ptr to avoid use-after-free
 * - Proper socket lifecycle management
 * - Integration with cobalt coroutines
 */
class CurlAsioContext
{
public:
    boost::asio::io_context io_ctx;
    CURLM * multi = nullptr;

    CurlAsioContext();
    ~CurlAsioContext();

    void check_multi_info();

private:
    // Socket state per curl socket
    struct SocketState
    {
        curl_socket_t sockfd;
        // Use posix::stream_descriptor for generic socket
        boost::asio::posix::stream_descriptor descriptor;
        int action = 0;                  // CURL_POLL_IN/OUT/INOUT
        std::weak_ptr<SocketState> self; // Prevent use-after-free

        SocketState(boost::asio::io_context & ctx, curl_socket_t fd);
        ~SocketState();
    };

    std::map<curl_socket_t, std::shared_ptr<SocketState>> socket_map_;
    boost::asio::steady_timer timer_;

    // Curl callbacks (static C functions)
    static int socket_callback(CURL * easy, curl_socket_t s, int what, void * userp, void * socketp);
    static int timer_callback(CURLM * multi, long timeout_ms, void * userp);

    // Asio event handlers
    void monitor_socket(std::shared_ptr<SocketState> state, int action);
    void handle_socket_readable(std::weak_ptr<SocketState> weak_state, const boost::system::error_code & err);
    void handle_socket_writable(std::weak_ptr<SocketState> weak_state, const boost::system::error_code & err);
    void handle_timeout(const boost::system::error_code & err);

    friend class CobaltFileTransfer;
};

/**
 * Per-transfer state for coroutine-based downloads.
 */
struct CoroTransferState
{
    FileTransferRequest request;
    FileTransferResult result;
    Activity act;

    // Curl handles
    CURL * easy = nullptr;
    char errbuf[CURL_ERROR_SIZE];
    struct curl_slist * requestHeaders = nullptr;

    // Coroutine coordination
    std::coroutine_handle<> awaiting_coroutine;
    std::exception_ptr exception;

    // Streaming support
    boost::cobalt::channel<std::string> * data_channel = nullptr;

    // Decompression
    std::shared_ptr<FinishSink> decompressionSink;
    std::optional<StringSink> errorSink;
    LambdaSink finalSink;

    // Progress
    curl_off_t writtenToSink = 0;
    std::chrono::steady_clock::time_point startTime;

    // HTTP state
    std::string encoding;
    bool acceptRanges = false;
    unsigned int attempt = 0;

    static inline const std::set<long> successfulStatuses{200, 201, 204, 206, 304, 0};

    CoroTransferState(const FileTransferRequest & req);
    ~CoroTransferState();

    void setupCurl(CURLM * multi);
    long getHTTPStatus();

    // Curl callbacks (static)
    static size_t write_callback(void *, size_t, size_t, void *);
    static size_t header_callback(void *, size_t, size_t, void *);
    static int progress_callback(void *, curl_off_t, curl_off_t, curl_off_t, curl_off_t);
    static size_t read_callback(char *, size_t, size_t, void *) noexcept;
};

/**
 * Cobalt-based FileTransfer implementation.
 * Uses Boost.Asio event loop with libcurl multi interface.
 */
class CobaltFileTransfer : public FileTransfer
{
public:
    CobaltFileTransfer();
    ~CobaltFileTransfer() override;

    // NEW: Coroutine API (lazy task version for proper executor handling)
    boost::cobalt::task<FileTransferResult> downloadCoro(FileTransferRequest request);

    boost::cobalt::task<FileTransferResult>
    downloadCoro(FileTransferRequest request, boost::cobalt::channel<std::string> & channel);

    // Existing callback API (compatibility)
    void enqueueFileTransfer(const FileTransferRequest & request, Callback<FileTransferResult> callback) override;

    // Override synchronous download to use coroutines directly
    FileTransferResult downloadSync(const FileTransferRequest & request);

private:
    CurlAsioContext ctx_;
    boost::asio::thread_pool work_pool_;
    std::thread io_thread_;
    std::optional<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>> work_guard_;

    std::mutex active_transfers_mutex_;
    std::map<CURL *, std::unique_ptr<CoroTransferState>> active_transfers_;

    boost::cobalt::task<FileTransferResult> performTransfer(std::unique_ptr<CoroTransferState> state);
};

/**
 * Helper to check for interruption in coroutines.
 */
struct InterruptCheck
{
    bool await_ready() const;
    void await_suspend(std::coroutine_handle<> h) const;
    void await_resume() const;
};

/**
 * Download with retry logic and exponential backoff.
 */
boost::cobalt::task<FileTransferResult> downloadWithRetry(FileTransferRequest request);

} // namespace nix

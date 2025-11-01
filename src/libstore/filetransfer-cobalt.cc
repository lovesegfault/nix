#include "nix/store/filetransfer-cobalt.hh"
#include "nix/util/callback.hh"
#include "nix/store/globals.hh"
#include "nix/util/compression.hh"
#include "nix/util/finally.hh"
#include "nix/util/signals.hh"

#include <algorithm>
#include <cmath>
#include <random>

namespace nix {

// ============================================================================
// CurlAsioContext Implementation
// ============================================================================

CurlAsioContext::SocketState::SocketState(boost::asio::io_context & ctx, curl_socket_t fd)
    : sockfd(fd)
    , descriptor(ctx)
{
    // Assign the FD without taking ownership (curl owns it)
    descriptor.assign(fd);
    descriptor.non_blocking(true);
}

CurlAsioContext::SocketState::~SocketState()
{
    // Cancel any pending operations first
    if (descriptor.is_open()) {
        boost::system::error_code ec;
        descriptor.cancel(ec);
        // Release without closing (curl owns the fd)
        descriptor.release();
    }
}

CurlAsioContext::CurlAsioContext()
    : timer_(io_ctx)
{
    // Initialize curl globally (once)
    static std::once_flag globalInit;
    std::call_once(globalInit, curl_global_init, CURL_GLOBAL_ALL);

    multi = curl_multi_init();

    // Set curl callbacks
    curl_multi_setopt(multi, CURLMOPT_SOCKETFUNCTION, socket_callback);
    curl_multi_setopt(multi, CURLMOPT_SOCKETDATA, this);
    curl_multi_setopt(multi, CURLMOPT_TIMERFUNCTION, timer_callback);
    curl_multi_setopt(multi, CURLMOPT_TIMERDATA, this);

    // HTTP/2 settings
#if LIBCURL_VERSION_NUM >= 0x072b00
    curl_multi_setopt(multi, CURLMOPT_PIPELINING, CURLPIPE_MULTIPLEX);
#endif
#if LIBCURL_VERSION_NUM >= 0x071e00
    curl_multi_setopt(multi, CURLMOPT_MAX_TOTAL_CONNECTIONS, fileTransferSettings.httpConnections.get());
#endif
}

CurlAsioContext::~CurlAsioContext()
{
    if (multi) {
        curl_multi_cleanup(multi);
    }
}

int CurlAsioContext::socket_callback(CURL * easy, curl_socket_t s, int what, void * userp, void * socketp)
{
    auto * ctx = static_cast<CurlAsioContext *>(userp);

    if (what == CURL_POLL_REMOVE) {
        auto it = ctx->socket_map_.find(s);
        if (it != ctx->socket_map_.end()) {
            // Cancel pending operations
            it->second->descriptor.cancel();
            // Force release the FD to prevent "File exists" on re-add
            if (it->second->descriptor.is_open()) {
                it->second->descriptor.release();
            }
            // Erase from map (SocketState destructor runs)
            ctx->socket_map_.erase(it);
        }
        return 0;
    }

    std::shared_ptr<SocketState> state;
    auto it = ctx->socket_map_.find(s);

    if (it == ctx->socket_map_.end()) {
        // New socket - create state
        state = std::make_shared<SocketState>(ctx->io_ctx, s);
        ctx->socket_map_[s] = state;
    } else {
        // Reuse existing socket state
        state = it->second;
    }

    state->action = what;
    ctx->monitor_socket(state, what);

    return 0;
}

void CurlAsioContext::monitor_socket(std::shared_ptr<SocketState> state, int action)
{
    state->self = state; // Enable weak_ptr in callbacks

    if (action & CURL_POLL_IN) {
        state->descriptor.async_wait(
            boost::asio::posix::stream_descriptor::wait_read,
            [this, weak = state->self](const boost::system::error_code & err) { handle_socket_readable(weak, err); });
    }

    if (action & CURL_POLL_OUT) {
        state->descriptor.async_wait(
            boost::asio::posix::stream_descriptor::wait_write,
            [this, weak = state->self](const boost::system::error_code & err) { handle_socket_writable(weak, err); });
    }
}

void CurlAsioContext::handle_socket_readable(
    std::weak_ptr<SocketState> weak_state, const boost::system::error_code & err)
{
    auto state = weak_state.lock();
    if (!state || err == boost::asio::error::operation_aborted)
        return;

    int running = 0;
    curl_multi_socket_action(multi, state->sockfd, CURL_CSELECT_IN, &running);
    check_multi_info();

    // Re-arm watcher if still needed
    if (state->action & CURL_POLL_IN) {
        monitor_socket(state, state->action);
    }
}

void CurlAsioContext::handle_socket_writable(
    std::weak_ptr<SocketState> weak_state, const boost::system::error_code & err)
{
    auto state = weak_state.lock();
    if (!state || err == boost::asio::error::operation_aborted)
        return;

    int running = 0;
    curl_multi_socket_action(multi, state->sockfd, CURL_CSELECT_OUT, &running);
    check_multi_info();

    // Re-arm watcher if still needed
    if (state->action & CURL_POLL_OUT) {
        monitor_socket(state, state->action);
    }
}

int CurlAsioContext::timer_callback(CURLM * multi, long timeout_ms, void * userp)
{
    auto * ctx = static_cast<CurlAsioContext *>(userp);

    ctx->timer_.cancel();

    if (timeout_ms > 0) {
        ctx->timer_.expires_after(std::chrono::milliseconds(timeout_ms));
        ctx->timer_.async_wait([ctx](const boost::system::error_code & err) {
            if (!err) {
                ctx->handle_timeout(err);
            }
        });
    } else if (timeout_ms == 0) {
        // Immediate timeout
        ctx->handle_timeout(boost::system::error_code{});
    }

    return 0;
}

void CurlAsioContext::handle_timeout(const boost::system::error_code & err)
{
    if (err == boost::asio::error::operation_aborted)
        return;

    int running = 0;
    curl_multi_socket_action(multi, CURL_SOCKET_TIMEOUT, 0, &running);
    check_multi_info();
}

void CurlAsioContext::check_multi_info()
{
    CURLMsg * msg;
    int msgs_left;

    while ((msg = curl_multi_info_read(multi, &msgs_left))) {
        if (msg->msg == CURLMSG_DONE) {
            CURL * easy = msg->easy_handle;
            CURLcode code = msg->data.result;

            // Retrieve state
            void * private_data;
            curl_easy_getinfo(easy, CURLINFO_PRIVATE, &private_data);
            auto * state = static_cast<CoroTransferState *>(private_data);

            if (!state)
                continue;

            auto finishTime = std::chrono::steady_clock::now();
            auto httpStatus = state->getHTTPStatus();

            debug(
                "finished %s of '%s'; curl status = %d, HTTP status = %d, body = %d bytes, duration = %.2f s",
                state->request.verb(),
                state->request.uri,
                code,
                httpStatus,
                state->result.bodySize,
                std::chrono::duration_cast<std::chrono::milliseconds>(finishTime - state->startTime).count() / 1000.0f);

            // Get effective URL
            char * effectiveUriCStr = nullptr;
            curl_easy_getinfo(easy, CURLINFO_EFFECTIVE_URL, &effectiveUriCStr);
            if (effectiveUriCStr && *state->result.urls.rbegin() != effectiveUriCStr) {
                state->result.urls.push_back(effectiveUriCStr);
            }

            // Finish decompression
            if (state->decompressionSink) {
                try {
                    state->decompressionSink->finish();
                } catch (...) {
                    state->exception = std::current_exception();
                }
            }

            // Handle ETag workaround
            if (code == CURLE_WRITE_ERROR && state->result.etag == state->request.expectedETag) {
                code = CURLE_OK;
                httpStatus = 304;
            }

            // Process result
            if (!state->exception && code == CURLE_OK && CoroTransferState::successfulStatuses.count(httpStatus)) {
                // Success!
                state->result.cached = (httpStatus == 304);

                // GitHub ETag workaround
                if (httpStatus == 304 && state->result.etag == "") {
                    state->result.etag = state->request.expectedETag;
                }

                state->act.progress(state->result.bodySize, state->result.bodySize);
            } else if (!state->exception) {
                // Error occurred
                // TODO: Store error classification in state for retry logic
                [[maybe_unused]] FileTransfer::Error err = FileTransfer::Transient;

                // Classify error
                if (httpStatus == 404 || httpStatus == 410 || code == CURLE_FILE_COULDNT_READ_FILE) {
                    err = FileTransfer::NotFound;
                } else if (httpStatus == 401 || httpStatus == 403 || httpStatus == 407) {
                    err = FileTransfer::Forbidden;
                } else if (httpStatus >= 400 && httpStatus < 500 && httpStatus != 408 && httpStatus != 429) {
                    err = FileTransfer::Misc;
                } else if (httpStatus == 501 || httpStatus == 505 || httpStatus == 511) {
                    err = FileTransfer::Misc;
                } else if (code == CURLE_ABORTED_BY_CALLBACK && getInterrupted()) {
                    err = FileTransfer::Interrupted;
                } else {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wswitch-enum"
                    switch (code) {
                    case CURLE_FAILED_INIT:
                    case CURLE_URL_MALFORMAT:
                    case CURLE_NOT_BUILT_IN:
                    case CURLE_REMOTE_ACCESS_DENIED:
                    case CURLE_FILE_COULDNT_READ_FILE:
                    case CURLE_FUNCTION_NOT_FOUND:
                    case CURLE_ABORTED_BY_CALLBACK:
                    case CURLE_BAD_FUNCTION_ARGUMENT:
                    case CURLE_INTERFACE_FAILED:
                    case CURLE_UNKNOWN_OPTION:
                    case CURLE_SSL_CACERT_BADFILE:
                    case CURLE_TOO_MANY_REDIRECTS:
                    case CURLE_WRITE_ERROR:
                    case CURLE_UNSUPPORTED_PROTOCOL:
                        err = FileTransfer::Misc;
                        break;
                    default:
                        break;
                    }
#pragma GCC diagnostic pop
                }

                // Build error message
                std::optional<std::string> response;
                if (state->errorSink) {
                    response = std::move(state->errorSink->s);
                }

                // Create error - simplified for now to avoid template instantiation issues
                // TODO: Improve error messages with FileTransferError template once stable
                std::string errorMsg;
                if (code == CURLE_ABORTED_BY_CALLBACK && getInterrupted()) {
                    errorMsg = fmt("%s of '%s' was interrupted", state->request.verb(), state->request.uri);
                } else if (httpStatus != 0) {
                    errorMsg =
                        fmt("unable to %s '%s': HTTP error %d%s",
                            state->request.verb(),
                            state->request.uri,
                            httpStatus,
                            code == CURLE_OK ? "" : fmt(" (curl error: %s)", curl_easy_strerror(code)));
                } else {
                    errorMsg =
                        fmt("unable to %s '%s': %s (%d)",
                            state->request.verb(),
                            state->request.uri,
                            curl_easy_strerror(code),
                            code);
                }

                state->exception = std::make_exception_ptr(nix::Error("%s", errorMsg));
            }

            // Clean up curl
            curl_multi_remove_handle(multi, easy);

            // Resume coroutine
            if (state->awaiting_coroutine) {
                auto handle = state->awaiting_coroutine;
                state->awaiting_coroutine = nullptr;
                handle.resume();
            }
        }
    }
}

// ============================================================================
// CoroTransferState Implementation
// ============================================================================

CoroTransferState::CoroTransferState(const FileTransferRequest & req)
    : request(req)
    , act(*logger,
          lvlTalkative,
          actFileTransfer,
          fmt("%sing '%s'", request.verb(), request.uri),
          {request.uri.to_string()},
          request.parentAct)
    , finalSink([this](std::string_view data) {
        if (this->errorSink) {
            (*this->errorSink)(data);
        }

        if (this->data_channel) {
            // TODO: Implement channel sending properly
            // For now, skip channel sends in finalSink
        } else if (this->request.dataCallback) {
            auto httpStatus = this->getHTTPStatus();
            if (successfulStatuses.count(httpStatus)) {
                this->writtenToSink += data.size();
                this->request.dataCallback(data);
            }
        } else {
            this->result.data.append(data);
        }
    })
    , startTime(std::chrono::steady_clock::now())
{
    result.urls.push_back(request.uri.to_string());

    // Set up headers
    requestHeaders = curl_slist_append(requestHeaders, "Accept-Encoding: zstd, br, gzip, deflate, bzip2, xz");

    if (!request.expectedETag.empty()) {
        requestHeaders = curl_slist_append(requestHeaders, ("If-None-Match: " + request.expectedETag).c_str());
    }

    if (!request.mimeType.empty()) {
        requestHeaders = curl_slist_append(requestHeaders, ("Content-Type: " + request.mimeType).c_str());
    }

    for (auto & [key, val] : request.headers) {
        requestHeaders = curl_slist_append(requestHeaders, fmt("%s: %s", key, val).c_str());
    }
}

CoroTransferState::~CoroTransferState()
{
    if (easy) {
        curl_easy_cleanup(easy);
    }
    if (requestHeaders) {
        curl_slist_free_all(requestHeaders);
    }
}

long CoroTransferState::getHTTPStatus()
{
    long httpStatus = 0;
    long protocol = 0;
    curl_easy_getinfo(easy, CURLINFO_PROTOCOL, &protocol);
    if (protocol == CURLPROTO_HTTP || protocol == CURLPROTO_HTTPS) {
        curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &httpStatus);
    }
    return httpStatus;
}

void CoroTransferState::setupCurl(CURLM * multi)
{
    if (!easy) {
        easy = curl_easy_init();
    }

    curl_easy_reset(easy);

    // Verbose debug if requested
    if (verbosity >= lvlVomit) {
        curl_easy_setopt(easy, CURLOPT_VERBOSE, 1);
    }

    // Basic settings
    curl_easy_setopt(easy, CURLOPT_URL, request.uri.to_string().c_str());
    curl_easy_setopt(easy, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(easy, CURLOPT_MAXREDIRS, 10);
    curl_easy_setopt(easy, CURLOPT_NOSIGNAL, 1);

    // User agent
    curl_easy_setopt(
        easy,
        CURLOPT_USERAGENT,
        ("curl/" LIBCURL_VERSION " Nix/" + nixVersion
         + (fileTransferSettings.userAgentSuffix != "" ? " " + fileTransferSettings.userAgentSuffix.get() : ""))
            .c_str());

#if LIBCURL_VERSION_NUM >= 0x072b00
    curl_easy_setopt(easy, CURLOPT_PIPEWAIT, 1);
#endif

#if LIBCURL_VERSION_NUM >= 0x072f00
    if (fileTransferSettings.enableHttp2) {
        curl_easy_setopt(easy, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2TLS);
    } else {
        curl_easy_setopt(easy, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_1);
    }
#endif

    // Callbacks
    curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(easy, CURLOPT_WRITEDATA, this);
    curl_easy_setopt(easy, CURLOPT_HEADERFUNCTION, header_callback);
    curl_easy_setopt(easy, CURLOPT_HEADERDATA, this);
    curl_easy_setopt(easy, CURLOPT_XFERINFOFUNCTION, progress_callback);
    curl_easy_setopt(easy, CURLOPT_XFERINFODATA, this);
    curl_easy_setopt(easy, CURLOPT_NOPROGRESS, 0);

    curl_easy_setopt(easy, CURLOPT_HTTPHEADER, requestHeaders);

    // Download speed limit
    if (settings.downloadSpeed.get() > 0) {
        curl_easy_setopt(easy, CURLOPT_MAX_RECV_SPEED_LARGE, (curl_off_t) (settings.downloadSpeed.get() * 1024));
    }

    // HTTP method
    if (request.method == HttpMethod::HEAD) {
        curl_easy_setopt(easy, CURLOPT_NOBODY, 1);
    } else if (request.method == HttpMethod::DELETE) {
        curl_easy_setopt(easy, CURLOPT_CUSTOMREQUEST, "DELETE");
    }

    // Upload data
    if (request.data) {
        if (request.method == HttpMethod::POST) {
            curl_easy_setopt(easy, CURLOPT_POST, 1L);
            curl_easy_setopt(easy, CURLOPT_POSTFIELDSIZE_LARGE, (curl_off_t) request.data->sizeHint);
        } else if (request.method == HttpMethod::PUT) {
            curl_easy_setopt(easy, CURLOPT_UPLOAD, 1L);
            curl_easy_setopt(easy, CURLOPT_INFILESIZE_LARGE, (curl_off_t) request.data->sizeHint);
        }
        curl_easy_setopt(easy, CURLOPT_READFUNCTION, read_callback);
        curl_easy_setopt(easy, CURLOPT_READDATA, this);
    }

    // SSL/TLS settings
    if (settings.caFile != "") {
        curl_easy_setopt(easy, CURLOPT_CAINFO, settings.caFile.get().c_str());
    }

    // Timeouts
    curl_easy_setopt(easy, CURLOPT_CONNECTTIMEOUT, fileTransferSettings.connectTimeout.get());
    curl_easy_setopt(easy, CURLOPT_LOW_SPEED_LIMIT, 1L);
    curl_easy_setopt(easy, CURLOPT_LOW_SPEED_TIME, fileTransferSettings.stalledDownloadTimeout.get());

    // Netrc
    curl_easy_setopt(easy, CURLOPT_NETRC_FILE, settings.netrcFile.get().c_str());
    curl_easy_setopt(easy, CURLOPT_NETRC, CURL_NETRC_OPTIONAL);

    // Resume from offset if needed
    if (writtenToSink) {
        curl_easy_setopt(easy, CURLOPT_RESUME_FROM_LARGE, writtenToSink);
    }

    // Error buffer
    curl_easy_setopt(easy, CURLOPT_ERRORBUFFER, errbuf);
    errbuf[0] = 0;

    // Authentication
    if (request.usernameAuth) {
        curl_easy_setopt(easy, CURLOPT_USERNAME, request.usernameAuth->username.c_str());
        if (request.usernameAuth->password) {
            curl_easy_setopt(easy, CURLOPT_PASSWORD, request.usernameAuth->password->c_str());
        }
    }

    // Store this for later retrieval
    curl_easy_setopt(easy, CURLOPT_PRIVATE, this);
}

size_t CoroTransferState::write_callback(void * contents, size_t size, size_t nmemb, void * userp)
{
    auto * state = static_cast<CoroTransferState *>(userp);
    size_t realSize = size * nmemb;

    try {
        state->result.bodySize += realSize;

        if (!state->decompressionSink) {
            state->decompressionSink = makeDecompressionSink(state->encoding, state->finalSink);

            if (!successfulStatuses.count(state->getHTTPStatus())) {
                state->errorSink = StringSink{};
            }
        }

        (*state->decompressionSink)({(char *) contents, realSize});

        return realSize;
    } catch (...) {
        state->exception = std::current_exception();
        return 0;
    }
}

size_t CoroTransferState::header_callback(void * contents, size_t size, size_t nmemb, void * userp)
{
    auto * state = static_cast<CoroTransferState *>(userp);
    size_t realSize = size * nmemb;
    std::string line((char *) contents, realSize);

    // Simplified header processing for now
    auto i = line.find(':');
    if (i != std::string::npos) {
        std::string name = toLower(trim(line.substr(0, i)));

        if (name == "content-encoding") {
            state->encoding = trim(line.substr(i + 1));
        } else if (name == "etag") {
            state->result.etag = trim(line.substr(i + 1));
        }
    }

    return realSize;
}

int CoroTransferState::progress_callback(
    void * userp, curl_off_t dltotal, curl_off_t dlnow, curl_off_t ultotal, curl_off_t ulnow)
{
    auto * state = static_cast<CoroTransferState *>(userp);
    auto isUpload = bool(state->request.data);

    try {
        state->act.progress(isUpload ? ulnow : dlnow, isUpload ? ultotal : dltotal);
    } catch (nix::Interrupted &) {
        assert(getInterrupted());
    }

    return getInterrupted();
}

size_t CoroTransferState::read_callback(char * buffer, size_t size, size_t nitems, void * userp) noexcept
try {
    auto * state = static_cast<CoroTransferState *>(userp);
    auto data = state->request.data;
    return data->source->read(buffer, nitems * size);
} catch (EndOfFile &) {
    return 0;
} catch (...) {
    return CURL_READFUNC_ABORT;
}

// ============================================================================
// CobaltFileTransfer Implementation
// ============================================================================

CobaltFileTransfer::CobaltFileTransfer()
    : work_pool_(std::thread::hardware_concurrency())
    , work_guard_(boost::asio::make_work_guard(ctx_.io_ctx))
{
    // Context is initialized in its constructor

    // Start I/O thread
    io_thread_ = std::thread([this]() { ctx_.io_ctx.run(); });
}

CobaltFileTransfer::~CobaltFileTransfer()
{
    // Reset work guard to allow io_context to finish
    work_guard_.reset();

    // Stop io_context (will finish current operations then exit)
    ctx_.io_ctx.stop();

    // Wait for threads
    if (io_thread_.joinable()) {
        io_thread_.join();
    }
    work_pool_.join();
}

boost::cobalt::task<FileTransferResult> CobaltFileTransfer::downloadCoro(FileTransferRequest request)
{
    // Lazy task - won't start until co_awaited, executor will be set by spawner
    auto state = std::make_unique<CoroTransferState>(request);
    co_return co_await performTransfer(std::move(state));
}

boost::cobalt::task<FileTransferResult>
CobaltFileTransfer::downloadCoro(FileTransferRequest request, boost::cobalt::channel<std::string> & channel)
{
    auto state = std::make_unique<CoroTransferState>(request);
    state->data_channel = &channel;
    co_return co_await performTransfer(std::move(state));
}

boost::cobalt::task<FileTransferResult> CobaltFileTransfer::performTransfer(std::unique_ptr<CoroTransferState> state)
{
    // Set up curl easy handle
    state->setupCurl(ctx_.multi);

    // Create awaitable
    struct TransferAwaitable
    {
        std::unique_ptr<CoroTransferState> state;
        CobaltFileTransfer * ft;
        CoroTransferState * state_ptr = nullptr; // Raw pointer for await_resume

        bool await_ready() const noexcept
        {
            return false;
        }

        void await_suspend(std::coroutine_handle<> h)
        {
            state->awaiting_coroutine = h;
            state_ptr = state.get(); // Save pointer before move

            // Store for later lookup (thread-safe)
            CURL * easy = state->easy;
            {
                std::lock_guard<std::mutex> lock(ft->active_transfers_mutex_);
                ft->active_transfers_[easy] = std::move(state); // state is now null
            }

            // CRITICAL: curl_multi is NOT thread-safe!
            // Must call curl_multi_add_handle from the io_context thread
            boost::asio::post(ft->ctx_.io_ctx, [easy, multi = ft->ctx_.multi]() {
                // Add to curl_multi (triggers socket callbacks)
                curl_multi_add_handle(multi, easy);

                // Kick curl to start processing immediately
                int running = 0;
                curl_multi_socket_action(multi, CURL_SOCKET_TIMEOUT, 0, &running);
            });
        }

        FileTransferResult await_resume()
        {
            // Use the saved pointer (state was moved in await_suspend)
            // Need to remove from active_transfers to reclaim ownership
            std::unique_ptr<CoroTransferState> reclaimed_state;
            {
                std::lock_guard<std::mutex> lock(ft->active_transfers_mutex_);
                auto it = ft->active_transfers_.find(state_ptr->easy);
                if (it != ft->active_transfers_.end()) {
                    reclaimed_state = std::move(it->second);
                    ft->active_transfers_.erase(it);
                }
            }

            if (!reclaimed_state) {
                throw nix::Error("Transfer state was already removed");
            }

            if (reclaimed_state->exception) {
                std::rethrow_exception(reclaimed_state->exception);
            }
            return std::move(reclaimed_state->result);
        }
    };

    co_return co_await TransferAwaitable{std::move(state), this};
}

FileTransferResult CobaltFileTransfer::downloadSync(const FileTransferRequest & request)
{
    // Use cobalt::spawn with our io_context executor
    auto future = boost::cobalt::spawn(ctx_.io_ctx.get_executor(), downloadCoro(request), boost::asio::use_future);

    return future.get();
}

void CobaltFileTransfer::enqueueFileTransfer(const FileTransferRequest & request, Callback<FileTransferResult> callback)
{
    // Keep callback alive with shared_ptr
    auto cb = std::make_shared<Callback<FileTransferResult>>(std::move(callback));

    // Spawn on executor with callback completion token
    boost::cobalt::spawn(
        ctx_.io_ctx.get_executor(), downloadCoro(request), [cb](std::exception_ptr ex, FileTransferResult result) {
            if (ex) {
                cb->rethrow(ex);
            } else {
                (*cb)(std::move(result));
            }
        });
}

// ============================================================================
// InterruptCheck Implementation
// ============================================================================

bool InterruptCheck::await_ready() const
{
    return getInterrupted();
}

void InterruptCheck::await_suspend(std::coroutine_handle<> h) const
{
    if (getInterrupted()) {
        h.resume();
    }
}

void InterruptCheck::await_resume() const
{
    checkInterrupt(); // Throws if interrupted
}

// ============================================================================
// Retry Logic
// ============================================================================

// Simplified retry for now - will be enhanced with proper cobalt integration later
boost::cobalt::task<FileTransferResult> downloadWithRetry(FileTransferRequest request)
{
    // For now, just call downloadCoro once (no retry yet)
    // TODO: Implement proper retry logic with exponential backoff
    auto ft = make_ref<CobaltFileTransfer>();
    co_return co_await ft->downloadCoro(request);
}

} // namespace nix

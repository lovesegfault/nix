#include "nix/store/filetransfer-async.hh"
#include "nix/util/async-collect.hh"
#include "nix/util/callback.hh"

#include <kj/async.h>
#include <memory>

namespace nix::async {

/**
 * Bridge between Callback-based API and kj::Promise.
 *
 * This creates a promise/fulfiller pair and wraps it in a Callback
 * that can be passed to the FileTransfer API.
 */
template<typename T>
struct PromiseCallback
{
    // Use shared_ptr to allow copying for std::function
    struct FulfillerHolder
    {
        kj::Own<kj::PromiseFulfiller<Result<T>>> fulfiller;

        explicit FulfillerHolder(kj::Own<kj::PromiseFulfiller<Result<T>>> f)
            : fulfiller(std::move(f))
        {
        }
    };

    kj::Promise<Result<T>> promise{nullptr};
    std::shared_ptr<FulfillerHolder> holder;

    PromiseCallback()
    {
        auto paf = kj::newPromiseAndFulfiller<Result<T>>();
        promise = std::move(paf.promise);
        holder = std::make_shared<FulfillerHolder>(std::move(paf.fulfiller));
    }

    kj::Promise<Result<T>> getPromise()
    {
        return std::move(promise);
    }

    Callback<T> getCallback()
    {
        // Create a callback that fulfills the promise
        // shared_ptr allows the lambda to be copy-constructible
        return Callback<T>([holder = this->holder](std::future<T> future) mutable {
            try {
                holder->fulfiller->fulfill(future.get());
            } catch (...) {
                holder->fulfiller->fulfill(result::current_exception());
            }
        });
    }
};

kj::Promise<Result<FileTransferResult>> AsyncFileTransfer::download(FileTransferRequest request)
try {
    request.method = HttpMethod::Get;

    PromiseCallback<FileTransferResult> pc;
    transfer->enqueueFileTransfer(request, pc.getCallback());

    co_return TRY_AWAIT(pc.getPromise());
} catch (...) {
    co_return result::current_exception();
}

kj::Promise<Result<FileTransferResult>> AsyncFileTransfer::upload(FileTransferRequest request)
try {
    if (request.method != HttpMethod::Put && request.method != HttpMethod::Post) {
        request.method = HttpMethod::Put;
    }

    PromiseCallback<FileTransferResult> pc;
    transfer->enqueueFileTransfer(request, pc.getCallback());

    co_return TRY_AWAIT(pc.getPromise());
} catch (...) {
    co_return result::current_exception();
}

kj::Promise<Result<FileTransferResult>> AsyncFileTransfer::deleteResource(FileTransferRequest request)
try {
    request.method = HttpMethod::Delete;

    PromiseCallback<FileTransferResult> pc;
    transfer->enqueueFileTransfer(request, pc.getCallback());

    co_return TRY_AWAIT(pc.getPromise());
} catch (...) {
    co_return result::current_exception();
}

kj::Promise<Result<FileTransferResult>> AsyncFileTransfer::head(FileTransferRequest request)
try {
    request.method = HttpMethod::Head;

    PromiseCallback<FileTransferResult> pc;
    transfer->enqueueFileTransfer(request, pc.getCallback());

    co_return TRY_AWAIT(pc.getPromise());
} catch (...) {
    co_return result::current_exception();
}

kj::Promise<Result<FileTransferResult>> AsyncFileTransfer::downloadToSink(FileTransferRequest request, Sink & sink)
try {
    request.method = HttpMethod::Get;

    // Set up a data callback that writes to the sink
    request.dataCallback = [&sink](std::string_view data) -> PauseTransfer {
        sink(data);
        return PauseTransfer::No;
    };

    PromiseCallback<FileTransferResult> pc;
    transfer->enqueueFileTransfer(request, pc.getCallback());

    co_return TRY_AWAIT(pc.getPromise());
} catch (...) {
    co_return result::current_exception();
}

kj::Promise<Result<FileTransferResult>> downloadAsync(FileTransferRequest request)
{
    auto aft = AsyncFileTransfer::create();
    return aft.download(std::move(request));
}

kj::Promise<Result<FileTransferResult>> uploadAsync(FileTransferRequest request)
{
    auto aft = AsyncFileTransfer::create();
    return aft.upload(std::move(request));
}

kj::Promise<Result<std::map<std::string, FileTransferResult>>>
downloadMultipleAsync(const std::vector<std::string> & urls)
try {
    auto aft = AsyncFileTransfer::create();

    // Create promises for all downloads
    kj::Vector<std::pair<std::string, kj::Promise<Result<FileTransferResult>>>> promises;
    promises.reserve(urls.size());

    for (const auto & url : urls) {
        FileTransferRequest request(url);
        promises.add(std::pair{url, aft.download(std::move(request))});
    }

    // Collect results
    auto collect = asyncCollect(promises.releaseAsArray());
    std::map<std::string, FileTransferResult> results;

    while (auto item = co_await collect.next()) {
        auto & [url, resultOrError] = *item;
        if (!resultOrError.has_value()) {
            // Propagate the first error
            co_return resultOrError.error();
        }
        results[url] = std::move(resultOrError.value());
    }

    co_return results;
} catch (...) {
    co_return result::current_exception();
}

} // namespace nix::async

#pragma once
/**
 * @file
 *
 * @brief Async extensions for FileTransfer.
 *
 * This provides kj::Promise-based wrappers around the FileTransfer API,
 * allowing file transfers to be integrated with the kj event loop.
 */

#include "nix/store/filetransfer.hh"
#include "nix/util/async.hh"
#include "nix/util/async-io.hh"
#include "nix/util/async-signal.hh"
#include "nix/util/result.hh"

#include <kj/async.h>

namespace nix::async {

/**
 * Async wrapper for file transfer operations.
 *
 * This bridges the callback-based FileTransfer API with kj::Promise,
 * allowing transfers to be composed with other async operations.
 */
class AsyncFileTransfer
{
    ref<FileTransfer> transfer;

public:
    explicit AsyncFileTransfer(ref<FileTransfer> transfer)
        : transfer(transfer)
    {
    }

    /**
     * Create an AsyncFileTransfer using the shared FileTransfer instance.
     */
    static AsyncFileTransfer create()
    {
        return AsyncFileTransfer(getFileTransfer());
    }

    /**
     * Download a file asynchronously.
     *
     * @param request The download request
     * @return A promise that resolves to the transfer result
     */
    kj::Promise<Result<FileTransferResult>> download(FileTransferRequest request);

    /**
     * Upload a file asynchronously.
     *
     * @param request The upload request
     * @return A promise that resolves to the transfer result
     */
    kj::Promise<Result<FileTransferResult>> upload(FileTransferRequest request);

    /**
     * Delete a resource asynchronously.
     *
     * @param request The delete request
     * @return A promise that resolves to the transfer result
     */
    kj::Promise<Result<FileTransferResult>> deleteResource(FileTransferRequest request);

    /**
     * Perform a HEAD request asynchronously.
     *
     * @param request The request (method will be set to HEAD)
     * @return A promise that resolves to the transfer result
     */
    kj::Promise<Result<FileTransferResult>> head(FileTransferRequest request);

    /**
     * Download a file to a sink asynchronously.
     *
     * The sink will be called with chunks of data as they arrive.
     * This is useful for streaming large files.
     *
     * @param request The download request
     * @param sink The sink to write data to
     * @return A promise that resolves when the download is complete
     */
    kj::Promise<Result<FileTransferResult>> downloadToSink(FileTransferRequest request, Sink & sink);

    /**
     * Get the underlying FileTransfer instance.
     */
    ref<FileTransfer> getTransfer()
    {
        return transfer;
    }
};

/**
 * Perform a simple async download.
 *
 * This is a convenience function that creates a temporary AsyncFileTransfer.
 */
kj::Promise<Result<FileTransferResult>> downloadAsync(FileTransferRequest request);

/**
 * Perform a simple async upload.
 */
kj::Promise<Result<FileTransferResult>> uploadAsync(FileTransferRequest request);

/**
 * Download multiple URLs concurrently.
 *
 * Uses AsyncCollect to manage the concurrent downloads with fail-fast
 * semantics.
 *
 * @param urls The URLs to download
 * @return A promise that resolves to a map of URL to result
 */
kj::Promise<Result<std::map<std::string, FileTransferResult>>>
downloadMultipleAsync(const std::vector<std::string> & urls);

} // namespace nix::async

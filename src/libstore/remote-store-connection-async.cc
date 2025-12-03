#include "nix/store/remote-store-connection-async.hh"
#include "nix/store/worker-protocol.hh"
#include "nix/util/serialise.hh"

namespace nix::async {

kj::Promise<Result<RemoteError>> processStderrAsync(RemoteStore::Connection & conn, AsyncFdIoStream & stream)
try {
    while (true) {
        // Read the message type
        uint64_t msgType;
        auto n = TRY_AWAIT(stream.readRange(&msgType, sizeof(msgType), sizeof(msgType)));
        if (!n) {
            throw Error("unexpected EOF from daemon");
        }

        switch (msgType) {
        case STDERR_LAST:
            // Daemon is done
            co_return RemoteError{nullptr};

        case STDERR_ERROR: {
            // Read error message
            // First read the error type (for backwards compat)
            uint64_t errorType;
            TRY_AWAIT(stream.readRange(&errorType, sizeof(errorType), sizeof(errorType)));

            // Read the error message length and content
            uint64_t len;
            TRY_AWAIT(stream.readRange(&len, sizeof(len), sizeof(len)));

            std::string errorMsg(len, '\0');
            TRY_AWAIT(stream.readRange(errorMsg.data(), len, len));

            // Read optional exit status
            uint64_t exitStatus = 1;
            if (errorType != 0) {
                TRY_AWAIT(stream.readRange(&exitStatus, sizeof(exitStatus), sizeof(exitStatus)));
            }

            try {
                throw Error("%s", errorMsg);
            } catch (...) {
                co_return RemoteError{std::current_exception()};
            }
        }

        case STDERR_NEXT: {
            // Log message from daemon
            uint64_t len;
            TRY_AWAIT(stream.readRange(&len, sizeof(len), sizeof(len)));

            std::string msg(len, '\0');
            TRY_AWAIT(stream.readRange(msg.data(), len, len));

            // Just log it
            printError("%s", msg);
            break;
        }

        case STDERR_START_ACTIVITY: {
            // Activity start - skip for now
            // TODO: Properly handle activity messages
            uint64_t actId, lvl, type;
            TRY_AWAIT(stream.readRange(&actId, sizeof(actId), sizeof(actId)));
            TRY_AWAIT(stream.readRange(&lvl, sizeof(lvl), sizeof(lvl)));
            TRY_AWAIT(stream.readRange(&type, sizeof(type), sizeof(type)));

            // Read the activity text
            uint64_t len;
            TRY_AWAIT(stream.readRange(&len, sizeof(len), sizeof(len)));
            std::string text(len, '\0');
            TRY_AWAIT(stream.readRange(text.data(), len, len));

            // Read fields (we skip them)
            uint64_t numFields;
            TRY_AWAIT(stream.readRange(&numFields, sizeof(numFields), sizeof(numFields)));
            for (uint64_t i = 0; i < numFields; i++) {
                uint64_t fieldType;
                TRY_AWAIT(stream.readRange(&fieldType, sizeof(fieldType), sizeof(fieldType)));
                if (fieldType == 0) {
                    // Integer
                    uint64_t val;
                    TRY_AWAIT(stream.readRange(&val, sizeof(val), sizeof(val)));
                } else {
                    // String
                    uint64_t len2;
                    TRY_AWAIT(stream.readRange(&len2, sizeof(len2), sizeof(len2)));
                    std::string s(len2, '\0');
                    TRY_AWAIT(stream.readRange(s.data(), len2, len2));
                }
            }

            // Read parent activity id
            uint64_t parent;
            TRY_AWAIT(stream.readRange(&parent, sizeof(parent), sizeof(parent)));
            break;
        }

        case STDERR_STOP_ACTIVITY: {
            // Activity stop - skip for now
            uint64_t actId;
            TRY_AWAIT(stream.readRange(&actId, sizeof(actId), sizeof(actId)));
            break;
        }

        case STDERR_RESULT: {
            // Activity result - skip for now
            uint64_t actId, type, numFields;
            TRY_AWAIT(stream.readRange(&actId, sizeof(actId), sizeof(actId)));
            TRY_AWAIT(stream.readRange(&type, sizeof(type), sizeof(type)));
            TRY_AWAIT(stream.readRange(&numFields, sizeof(numFields), sizeof(numFields)));

            for (uint64_t i = 0; i < numFields; i++) {
                uint64_t fieldType;
                TRY_AWAIT(stream.readRange(&fieldType, sizeof(fieldType), sizeof(fieldType)));
                if (fieldType == 0) {
                    uint64_t val;
                    TRY_AWAIT(stream.readRange(&val, sizeof(val), sizeof(val)));
                } else {
                    uint64_t len2;
                    TRY_AWAIT(stream.readRange(&len2, sizeof(len2), sizeof(len2)));
                    std::string s(len2, '\0');
                    TRY_AWAIT(stream.readRange(s.data(), len2, len2));
                }
            }
            break;
        }

        default:
            throw Error("unknown message type %d from daemon", msgType);
        }
    }
} catch (...) {
    co_return result::current_exception();
}

kj::Promise<Result<void>> AsyncConnectionHandle::processStderr(AsyncFdIoStream & stream)
try {
    auto err = TRY_AWAIT(processStderrAsync(*handle, stream));
    if (err.e) {
        std::rethrow_exception(err.e);
    }
    co_return result::success();
} catch (...) {
    co_return result::current_exception();
}

kj::Promise<Result<void>> AsyncConnectionHandle::withFramedStream(
    AsyncFdIoStream & stream, std::function<kj::Promise<Result<void>>(AsyncOutputStream & out)> fun)
try {
    AsyncFramedOutputStream framedOut{stream};

    TRY_AWAIT(fun(framedOut));
    TRY_AWAIT(framedOut.finish());
    TRY_AWAIT(processStderr(stream));

    co_return result::success();
} catch (...) {
    co_return result::current_exception();
}

} // namespace nix::async

namespace nix::async_ops {

kj::Promise<Result<bool>> isValidPathAsync(async::AsyncConnectionHandle & conn, const StorePath & path)
try {
    // Get store and connection references
    const auto & store = conn.getStore();
    auto & handle = *conn;

    // Flush and send the command
    handle.to.flush();
    handle.to << WorkerProto::Op::IsValidPath;
    WorkerProto::write(store, static_cast<WorkerProto::WriteConn>(handle), path);
    handle.to.flush();

    // Process stderr (synchronously for now)
    auto ex = handle.processStderrReturn();
    if (ex) {
        std::rethrow_exception(ex);
    }

    // Read the result
    co_return static_cast<bool>(readInt(handle.from));
} catch (...) {
    co_return result::current_exception();
}

kj::Promise<Result<StorePathSet>> queryAllValidPathsAsync(async::AsyncConnectionHandle & conn)
try {
    const auto & store = conn.getStore();
    auto & handle = *conn;

    // Flush and send the command
    handle.to.flush();
    handle.to << WorkerProto::Op::QueryAllValidPaths;
    handle.to.flush();

    // Process stderr
    auto ex = handle.processStderrReturn();
    if (ex) {
        std::rethrow_exception(ex);
    }

    // Read the result
    co_return WorkerProto::Serialise<StorePathSet>::read(store, static_cast<WorkerProto::ReadConn>(handle));
} catch (...) {
    co_return result::current_exception();
}

} // namespace nix::async_ops

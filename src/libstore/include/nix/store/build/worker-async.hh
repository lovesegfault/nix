#pragma once
/**
 * @file
 *
 * @brief Async extensions for the build Worker.
 *
 * This provides async-aware versions of worker functionality that can
 * be used with the kj event loop. It bridges the existing Goal::Co
 * coroutine system with kj::Promise for incremental asyncification.
 */

#include "nix/store/build/worker.hh"
#include "nix/store/build/goal.hh"
#include "nix/store/globals.hh"
#include "nix/util/async.hh"
#include "nix/util/async-semaphore.hh"
#include "nix/util/result.hh"

#include <kj/async.h>
#include <kj/async-io.h>
#include <kj/async-unix.h>

namespace nix {

/**
 * Result of a goal's work, for use with async APIs.
 */
struct GoalWorkResult
{
    Goal::ExitCode exitCode;
    std::optional<Error> error;
    BuildResult buildResult;

    static GoalWorkResult fromGoal(const Goal & goal)
    {
        return GoalWorkResult{
            .exitCode = goal.exitCode,
            .error = goal.ex,
            .buildResult = goal.buildResult,
        };
    }
};

namespace async {

/**
 * Async extensions for the Worker class.
 *
 * This provides async-aware functionality on top of the existing Worker
 * without modifying its core structure. The extensions allow using
 * kj::Promise for coordination while the goal system continues to use
 * Goal::Co internally.
 */
class WorkerAsync
{
    Worker & worker;

    /**
     * Semaphore for limiting local build jobs.
     */
    AsyncSemaphore buildSlots;

    /**
     * Semaphore for limiting substitution jobs.
     */
    AsyncSemaphore substitutionSlots;

public:
    WorkerAsync(Worker & worker)
        : worker(worker)
        , buildSlots(std::max(1U, settings.maxBuildJobs.get()))
        , substitutionSlots(std::max(1U, settings.maxSubstitutionJobs.get()))
    {
    }

    Worker & getWorker()
    {
        return worker;
    }

    /**
     * Acquire a build slot asynchronously.
     *
     * Returns a token that releases the slot when destroyed.
     */
    kj::Promise<AsyncSemaphore::Token> acquireBuildSlot()
    {
        return buildSlots.acquire();
    }

    /**
     * Acquire a substitution slot asynchronously.
     */
    kj::Promise<AsyncSemaphore::Token> acquireSubstitutionSlot()
    {
        return substitutionSlots.acquire();
    }

    /**
     * Try to acquire a slot without waiting.
     */
    std::optional<AsyncSemaphore::Token> tryAcquireBuildSlot()
    {
        return buildSlots.tryAcquire();
    }

    std::optional<AsyncSemaphore::Token> tryAcquireSubstitutionSlot()
    {
        return substitutionSlots.tryAcquire();
    }

    /**
     * Get the number of available build slots.
     */
    unsigned availableBuildSlots() const
    {
        return buildSlots.available();
    }

    unsigned availableSubstitutionSlots() const
    {
        return substitutionSlots.available();
    }

    /**
     * Create a promise that resolves when a goal completes.
     *
     * This bridges the Goal::Co system with kj::Promise by creating
     * a promise that polls the goal's completion status.
     *
     * @param goal The goal to wait for
     * @return A promise that resolves to the goal's result
     */
    kj::Promise<Result<GoalWorkResult>> waitForGoal(GoalPtr goal);

    /**
     * Run the worker loop asynchronously.
     *
     * This is an async version of Worker::run() that uses kj for
     * I/O multiplexing instead of poll().
     *
     * @param topGoals The top-level goals to run
     * @return A promise that resolves when all goals are complete
     */
    kj::Promise<Result<void>> runAsync(const Goals & topGoals);
};

/**
 * Create a promise that resolves after a delay.
 *
 * Useful for implementing poll intervals and timeouts.
 */
inline kj::Promise<void> delay(kj::Duration duration)
{
    return AIO().provider.getTimer().afterDelay(duration);
}

/**
 * Monitor a file descriptor for readability asynchronously.
 *
 * @param fd The file descriptor to monitor
 * @return A promise that resolves when the fd is readable
 */
kj::Promise<void> waitForReadable(int fd);

/**
 * Monitor a set of file descriptors for I/O.
 *
 * This is an async replacement for poll() that integrates with the
 * kj event loop.
 */
class AsyncFdMonitor
{
    kj::UnixEventPort::FdObserver observer;
    int fd;

public:
    AsyncFdMonitor(int fd_)
        : observer(AIO().unixEventPort, fd_, kj::UnixEventPort::FdObserver::OBSERVE_READ)
        , fd(fd_)
    {
    }

    /**
     * Wait for the fd to become readable.
     */
    kj::Promise<void> whenReadable()
    {
        return observer.whenBecomesReadable();
    }

    int getFd() const
    {
        return fd;
    }
};

} // namespace async

} // namespace nix

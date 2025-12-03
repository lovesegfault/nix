#include "nix/store/build/worker-async.hh"
#include "nix/store/build/substitution-goal.hh"
#include "nix/store/build/derivation-goal.hh"
#include "nix/store/globals.hh"
#include "nix/util/signals.hh"

#include <kj/async-unix.h>

namespace nix::async {

kj::Promise<void> waitForReadable(int fd)
{
    // Create an observer for this fd
    kj::UnixEventPort::FdObserver observer(AIO().unixEventPort, fd, kj::UnixEventPort::FdObserver::OBSERVE_READ);
    return observer.whenBecomesReadable();
}

kj::Promise<Result<GoalWorkResult>> WorkerAsync::waitForGoal(GoalPtr goal)
try {
    // Poll the goal's status until it's done
    while (goal->exitCode == Goal::ecBusy) {
        // Let the goal do some work
        goal->work();

        // Check if it's done now
        if (goal->exitCode != Goal::ecBusy) {
            break;
        }

        // Wait a bit before polling again
        // In a full async implementation, we'd wait for specific events
        co_await delay(kj::MILLISECONDS * 10);

        // Check for interrupts
        checkInterrupt();
    }

    co_return GoalWorkResult::fromGoal(*goal);
} catch (...) {
    co_return result::current_exception();
}

kj::Promise<Result<void>> WorkerAsync::runAsync(const Goals & topGoals)
try {
    // Add top goals to the worker
    for (auto & goal : topGoals) {
        worker.wakeUp(goal);
    }

    // Main loop
    while (true) {
        checkInterrupt();

        // Process all awake goals
        bool didWork = false;
        Goals awakeGoals;

        // Collect awake goals (we can't iterate and modify at the same time)
        // Note: This is a simplified version - the real implementation would
        // access worker's internal awake set
        for (auto & goal : topGoals) {
            if (goal->exitCode == Goal::ecBusy) {
                awakeGoals.insert(goal);
            }
        }

        for (auto & goal : awakeGoals) {
            checkInterrupt();
            goal->work();
            didWork = true;

            // Check if all top goals are done
            bool allDone = true;
            for (auto & g : topGoals) {
                if (g->exitCode == Goal::ecBusy) {
                    allDone = false;
                    break;
                }
            }
            if (allDone) {
                co_return result::success();
            }
        }

        // If no work was done, yield to let other async tasks run
        if (!didWork) {
            co_await delay(kj::MILLISECONDS * 10);
        }

        // Check if all goals are complete
        bool allDone = true;
        for (auto & goal : topGoals) {
            if (goal->exitCode == Goal::ecBusy) {
                allDone = false;
                break;
            }
        }

        if (allDone) {
            break;
        }
    }

    co_return result::success();
} catch (...) {
    co_return result::current_exception();
}

} // namespace nix::async

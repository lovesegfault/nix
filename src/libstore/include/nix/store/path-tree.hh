#pragma once
/// @file

#include "nix/util/source-accessor.hh"
#include "nix/store/path.hh"
#include "nix/store/store-api.hh"

namespace nix {

/**
 * Generate a graph string showing the dependency path from `start` to `to`.
 *
 * @param start The starting store path (e.g., the package)
 * @param to The target store path (e.g., the dependency)
 * @param graphData Map of each store path to the paths it references
 * @param store The store to use for path operations
 * @param all Show all paths in the graph, not just the shortest path
 * @param precise Show which files contain the references
 * @param accessor Optional custom accessor for accessing file contents (e.g., for chroot)
 * @return A formatted string showing the dependency tree
 */
std::string genGraphString(
    const StorePath & start,
    const StorePath & to,
    const std::map<StorePath, StorePathSet> & graphData,
    Store & store,
    bool all,
    bool precise,
    std::optional<ref<SourceAccessor>> accessor = std::nullopt);

} // namespace nix

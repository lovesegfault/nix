#include "nix/store/path-tree.hh"
#include "nix/store/path-references.hh"
#include "nix/util/ansicolor.hh"
#include "nix/util/error.hh"
#include "nix/util/strings.hh"
#include <queue>

#define ANSI_ALREADY_VISITED "\e[38;5;244m"

namespace nix {

static std::string hilite(const std::string & s, size_t pos, size_t len, const std::string & colour = ANSI_RED)
{
    return std::string(s, 0, pos) + colour + std::string(s, pos, len) + ANSI_NORMAL + std::string(s, pos + len);
}

static std::string filterPrintable(const std::string & s)
{
    std::string res;
    for (char c : s)
        res += isprint(c) ? c : '.';
    return res;
}

struct Node
{
    StorePath path;
    StorePathSet dependencies;
    StorePathSet dependents;
    std::optional<size_t> dist = std::nullopt;
    Node * prev = nullptr;
    bool queued = false;
    bool visited = false;
};

struct BailOut
{};

static std::map<StorePath, Node> mkGraph(
    const StorePath & packagePath,
    const StorePath & dependencyPath,
    const std::map<StorePath, StorePathSet> & graph,
    Store & store,
    bool all,
    bool precise)
{
    std::map<StorePath, Node> graphData;
    for (auto & [path, dependencies] : graph) {
        graphData.emplace(
            path,
            Node{
                .path = path,
                .dependencies = dependencies,
                .dist = path == dependencyPath ? std::optional(0) : std::nullopt,
            });
    }

    // Transpose the graph to build dependents (reverse edges)
    for (auto & node : graphData) {
        for (auto & ref : node.second.dependencies) {
            auto it = graphData.find(ref);
            if (it != graphData.end()) {
                it->second.dependents.insert(node.first);
            }
        }
    }

    /* Run Dijkstra's shortest path algorithm to get the distance
       of every path in the closure to 'dependency'. */
    std::priority_queue<Node *> queue;

    queue.push(&graphData.at(dependencyPath));
    auto const inf = std::numeric_limits<size_t>::max();

    while (!queue.empty()) {
        auto & node = *queue.top();
        queue.pop();

        for (auto & rref : node.dependents) {
            auto & node2 = graphData.at(rref);
            auto dist = node.dist.transform([](auto n) { return n + 1; });
            if (dist.value_or(inf) < node2.dist.value_or(inf)) {
                node2.dist = dist;
                node2.prev = &node;
                if (!node2.queued) {
                    node2.queued = true;
                    queue.push(&node2);
                }
            }
        }
    }

    return graphData;
}

static void printNode(
    Node & node,
    const std::string & firstPad,
    const std::string & tailPad,
    bool all,
    bool precise,
    Store & store,
    const StorePath & packagePath,
    const StorePath & dependencyPath,
    std::map<StorePath, Node> & graph,
    Strings & output,
    std::optional<ref<SourceAccessor>> accessor)
{
    auto pathS = store.printStorePath(node.path);

    assert(node.dist.has_value());
    if (precise) {
        output.push_back(
            fmt("%s%s%s%s" ANSI_NORMAL,
                firstPad,
                node.path == dependencyPath ? ANSI_NORMAL
                : node.visited              ? ANSI_ALREADY_VISITED
                                            : "",
                firstPad != "" ? "→ " : "",
                pathS));
    }

    if (node.path == dependencyPath && !all && packagePath != dependencyPath) {
        throw BailOut();
    }

    if (node.visited) {
        return;
    }

    if (precise) {
        node.visited = true;
    }

    /* Sort the references by distance to `dependency` to
       ensure that the shortest path is printed first. */
    std::multimap<size_t, Node *> refs;
    StorePathSet refPaths;

    for (auto & ref : node.dependencies) {
        if (ref == node.path && packagePath != dependencyPath) {
            continue;
        }
        auto it = graph.find(ref);
        if (it == graph.end()) {
            continue;
        }
        auto & node2 = it->second;
        if (auto dist = node2.dist) {
            refs.emplace(*node2.dist, &node2);
            refPaths.insert(node2.path);
        }
    }

    /* For each reference, find the files and symlinks that
       contain the reference. */
    std::map<std::string, Strings> hits;

    auto dependencyPathHash = dependencyPath.hashPart();

    auto getColour = [&](const std::string & hash) { return hash == dependencyPathHash ? ANSI_GREEN : ANSI_BLUE; };

    if (auto acc = accessor; precise && acc) {
        // Use scanForReferencesDeep to find files containing references
        scanForReferencesDeep(**acc, CanonPath::root, refPaths, [&](FileRefScanResult result) {
            auto p2 = result.filePath.isRoot() ? result.filePath.abs() : result.filePath.rel();
            auto st = (*acc)->lstat(result.filePath);

            if (st.type == SourceAccessor::Type::tRegular) {
                auto contents = (*acc)->readFile(result.filePath);

                // For each reference found in this file, extract context
                for (auto & foundRef : result.foundRefs) {
                    std::string hash(foundRef.hashPart());
                    auto pos = contents.find(hash);
                    if (pos != std::string::npos) {
                        size_t margin = 32;
                        auto pos2 = pos >= margin ? pos - margin : 0;
                        hits[hash].emplace_back(
                            fmt("%s: …%s…",
                                p2,
                                hilite(
                                    filterPrintable(std::string(contents, pos2, pos - pos2 + hash.size() + margin)),
                                    pos - pos2,
                                    StorePath::HashLen,
                                    getColour(hash))));
                    }
                }
            } else if (st.type == SourceAccessor::Type::tSymlink) {
                auto target = (*acc)->readLink(result.filePath);

                // For each reference found in this symlink, show it
                for (auto & foundRef : result.foundRefs) {
                    std::string hash(foundRef.hashPart());
                    auto pos = target.find(hash);
                    if (pos != std::string::npos)
                        hits[hash].emplace_back(
                            fmt("%s -> %s", p2, hilite(target, pos, StorePath::HashLen, getColour(hash))));
                }
            }
        });
    }

    for (auto & ref : refs) {
        std::string hash(ref.second->path.hashPart());

        bool last = all ? ref == *refs.rbegin() : true;

        for (auto & hit : hits[hash]) {
            bool first = hit == *hits[hash].begin();
            output.push_back(
                fmt("%s%s%s", tailPad, (first ? (last ? treeLast : treeConn) : (last ? treeNull : treeLine)), hit));
            if (!all) {
                break;
            }
        }

        if (!precise) {
            auto pathS = store.printStorePath(ref.second->path);
            output.push_back(
                fmt("%s%s%s%s" ANSI_NORMAL,
                    firstPad,
                    ref.second->path == dependencyPath ? ANSI_BOLD
                    : ref.second->visited              ? ANSI_ALREADY_VISITED
                                                       : "",
                    last ? treeLast : treeConn,
                    pathS));
            node.visited = true;
        }

        printNode(
            *ref.second,
            tailPad + (last ? treeNull : treeLine),
            tailPad + (last ? treeNull : treeLine),
            all,
            precise,
            store,
            packagePath,
            dependencyPath,
            graph,
            output,
            accessor);
    }
}

std::string genGraphString(
    const StorePath & start,
    const StorePath & to,
    const std::map<StorePath, StorePathSet> & graphData,
    Store & store,
    bool all,
    bool precise,
    std::optional<ref<SourceAccessor>> maybeAccessor)
{
    auto graph = mkGraph(start, to, graphData, store, all, precise);

    Strings output;
    if (!precise) {
        output.push_back(fmt("%s", store.printStorePath(graph.at(start).path)));
    }

    try {
        printNode(graph.at(start), "", "", all, precise, store, start, to, graph, output, maybeAccessor);
    } catch (BailOut &) {
    }

    return concatStringsSep("\n", output);
}

} // namespace nix

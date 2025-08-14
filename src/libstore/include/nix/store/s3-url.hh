#pragma once
///@file

#include "nix/util/url.hh"
#include "nix/util/util.hh"

#include <optional>
#include <string>
#include <variant>

namespace nix {

/**
 * Parsed S3 URL.
 */
struct ParsedS3URL
{
    std::string bucket;
    std::string key;
    std::optional<std::string> profile;
    std::optional<std::string> region;
    std::optional<std::string> scheme;
    /**
     * The endpoint can be either missing, be an absolute URI (with a scheme like `http:`)
     * or an authority (so an IP address or a registered name).
     */
    std::variant<std::monostate, ParsedURL, ParsedURL::Authority> endpoint;

    std::optional<std::string> getEncodedEndpoint() const
    {
        return std::visit(
            overloaded{
                [](std::monostate) -> std::optional<std::string> { return std::nullopt; },
                [](const auto & authorityOrUrl) -> std::optional<std::string> { return authorityOrUrl.to_string(); },
            },
            endpoint);
    }

    static ParsedS3URL parse(std::string_view uri);
    auto operator<=>(const ParsedS3URL & other) const = default;
};

} // namespace nix
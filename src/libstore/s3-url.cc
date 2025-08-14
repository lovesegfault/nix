#include "nix/store/s3-url.hh"
#include "nix/util/split.hh"

#include <string_view>

using namespace std::string_view_literals;

namespace nix {

ParsedS3URL ParsedS3URL::parse(std::string_view uri)
try {
    auto parsed = parseURL(uri);

    if (parsed.scheme != "s3"sv)
        throw BadURL("URI scheme '%s' is not 's3'", parsed.scheme);

    /* Yeah, S3 URLs in Nix have the bucket name as authority. Luckily registered name type
       authority has the same restrictions (mostly) as S3 bucket names.
       TODO: Validate against:
       https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html#general-purpose-bucket-names
       */
    if (!parsed.authority || parsed.authority->host.empty()
        || parsed.authority->hostType != ParsedURL::Authority::HostType::Name)
        throw BadURL("URI has a missing or invalid bucket name");

    std::string_view key = parsed.path;
    /* Make the key a relative path. */
    splitPrefix(key, "/");

    /* TODO: Validate the key against:
       https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html#object-key-guidelines
       */

    ParsedS3URL res{
        .bucket = parsed.authority->host,
        .key = std::string(key),
    };

    for (const auto & [name, value] : parsed.query) {
        if (name == "profile")
            res.profile = value;
        else if (name == "region")
            res.region = value;
        else if (name == "scheme")
            res.scheme = value;
        else if (name == "endpoint") {
            if (value.find("://") != std::string::npos) {
                res.endpoint = parseURL(value);
            } else {
                res.endpoint = ParsedURL::Authority::parse(value);
            }
        }
        /* FIXME what to do about other query params? */
    }

    return res;
} catch (BadURL & e) {
    throw BadURL("while parsing S3 URL '%s': %s", uri, e.what());
}

} // namespace nix
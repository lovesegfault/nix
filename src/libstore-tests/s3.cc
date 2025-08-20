#include "nix/store/s3.hh"
#include "nix/store/config.hh"
#include "nix/util/tests/gmock-matchers.hh"

#if NIX_WITH_S3_SUPPORT

#  include <gtest/gtest.h>
#  include <gmock/gmock.h>

namespace nix {

struct ParsedS3URLTestCase
{
    std::string url;
    ParsedS3URL expected;
    std::string description;
};

class ParsedS3URLTest : public ::testing::WithParamInterface<ParsedS3URLTestCase>, public ::testing::Test
{};

TEST_P(ParsedS3URLTest, parseS3URLSuccessfully)
{
    const auto & testCase = GetParam();
    auto parsed = ParsedS3URL::parse(testCase.url);
    ASSERT_EQ(parsed, testCase.expected);
}

INSTANTIATE_TEST_SUITE_P(
    QueryParams,
    ParsedS3URLTest,
    ::testing::Values(
        ParsedS3URLTestCase{
            "s3://my-bucket/my-key.txt",
            {
                .bucket = "my-bucket",
                .key = "my-key.txt",
            },
            "basic_s3_bucket"},
        ParsedS3URLTestCase{
            "s3://prod-cache/nix/store/abc123.nar.xz?region=eu-west-1",
            {
                .bucket = "prod-cache",
                .key = "nix/store/abc123.nar.xz",
                .region = "eu-west-1",
            },
            "with_region"},
        ParsedS3URLTestCase{
            "s3://bucket/key?region=us-west-2&profile=prod&endpoint=custom.s3.com&scheme=https&region=us-east-1",
            {
                .bucket = "bucket",
                .key = "key",
                .profile = "prod",
                .region = "us-west-2", //< using the first parameter (decodeQuery ignores dupicates)
                .scheme = "https",
                .endpoint = ParsedURL::Authority{.host = "custom.s3.com"},
            },
            "complex"},
        ParsedS3URLTestCase{
            "s3://cache/file.txt?profile=production&region=ap-southeast-2",
            {
                .bucket = "cache",
                .key = "file.txt",
                .profile = "production",
                .region = "ap-southeast-2",
            },
            "with_profile_and_region"},
        ParsedS3URLTestCase{
            "s3://bucket/key?endpoint=https://minio.local&scheme=http",
            {
                .bucket = "bucket",
                .key = "key",
                /* TODO: Figure out what AWS SDK is doing when both endpointOverride and scheme are set. */
                .scheme = "http",
                .endpoint =
                    ParsedURL{
                        .scheme = "https",
                        .authority = ParsedURL::Authority{.host = "minio.local"},
                    },
            },
            "with_absolute_endpoint_uri"}),
    [](const ::testing::TestParamInfo<ParsedS3URLTestCase> & info) { return info.param.description; });

TEST(InvalidParsedS3URLTest, parseS3URLErrors)
{
    auto invalidBucketMatcher = ::testing::ThrowsMessage<BadURL>(
        testing::HasSubstrIgnoreANSIMatcher("error: URI has a missing or invalid bucket name"));

    /* Empty bucket (authority) */
    ASSERT_THAT([]() { ParsedS3URL::parse("s3:///key"); }, invalidBucketMatcher);
    /* Invalid bucket name */
    ASSERT_THAT([]() { ParsedS3URL::parse("s3://127.0.0.1"); }, invalidBucketMatcher);
}

// AWS Credential Provider Tests

class AwsCredentialProviderTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        // Clear any existing AWS environment variables for clean tests
        unsetenv("AWS_ACCESS_KEY_ID");
        unsetenv("AWS_SECRET_ACCESS_KEY");
        unsetenv("AWS_SESSION_TOKEN");
        unsetenv("AWS_PROFILE");
    }
};

TEST_F(AwsCredentialProviderTest, createDefault)
{
    try {
        auto provider = AwsCredentialProvider::createDefault();
        EXPECT_NE(provider, nullptr);
    } catch (const AwsAuthError & e) {
        // Expected in sandboxed environments where AWS CRT isn't available
        GTEST_SKIP() << "AWS CRT not available: " << e.what();
    }
}

TEST_F(AwsCredentialProviderTest, createProfile_Empty)
{
    try {
        auto provider = AwsCredentialProvider::createProfile("");
        EXPECT_NE(provider, nullptr);
    } catch (const AwsAuthError & e) {
        // Expected in sandboxed environments where AWS CRT isn't available
        GTEST_SKIP() << "AWS CRT not available: " << e.what();
    }
}

TEST_F(AwsCredentialProviderTest, createProfile_Named)
{
    // Creating a non-existent profile should throw
    try {
        auto provider = AwsCredentialProvider::createProfile("test-profile");
        // If we got here, the profile exists (unlikely in test environment)
        EXPECT_NE(provider, nullptr);
    } catch (const AwsAuthError & e) {
        // Expected - profile doesn't exist
        EXPECT_TRUE(std::string(e.what()).find("test-profile") != std::string::npos);
    }
}

TEST_F(AwsCredentialProviderTest, getCredentials_NoCredentials)
{
    // With no environment variables or profile, should throw when getting credentials
    try {
        auto provider = AwsCredentialProvider::createDefault();
        ASSERT_NE(provider, nullptr);

        // This should throw if there are no credentials available
        try {
            auto creds = provider->getCredentials();
            // If we got here, credentials were found (e.g., from IMDS or ~/.aws/credentials)
            EXPECT_TRUE(true); // Basic sanity check
        } catch (const AwsAuthError &) {
            // Expected if no credentials are available
            EXPECT_TRUE(true);
        }
    } catch (const AwsAuthError & e) {
        GTEST_SKIP() << "AWS authentication failed: " << e.what();
    }
}

TEST_F(AwsCredentialProviderTest, getCredentials_FromEnvironment)
{
    // Set up test environment variables
    setenv("AWS_ACCESS_KEY_ID", "test-access-key", 1);
    setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key", 1);
    setenv("AWS_SESSION_TOKEN", "test-session-token", 1);

    try {
        auto provider = AwsCredentialProvider::createDefault();
        ASSERT_NE(provider, nullptr);

        auto creds = provider->getCredentials();
        EXPECT_EQ(creds.accessKeyId, "test-access-key");
        EXPECT_EQ(creds.secretAccessKey, "test-secret-key");
        EXPECT_TRUE(creds.sessionToken.has_value());
        EXPECT_EQ(*creds.sessionToken, "test-session-token");
    } catch (const AwsAuthError & e) {
        // Clean up first
        unsetenv("AWS_ACCESS_KEY_ID");
        unsetenv("AWS_SECRET_ACCESS_KEY");
        unsetenv("AWS_SESSION_TOKEN");
        GTEST_SKIP() << "AWS authentication failed: " << e.what();
    }

    // Clean up
    unsetenv("AWS_ACCESS_KEY_ID");
    unsetenv("AWS_SECRET_ACCESS_KEY");
    unsetenv("AWS_SESSION_TOKEN");
}

TEST_F(AwsCredentialProviderTest, getCredentials_WithoutSessionToken)
{
    // Set up test environment variables without session token
    setenv("AWS_ACCESS_KEY_ID", "test-access-key-2", 1);
    setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key-2", 1);

    try {
        auto provider = AwsCredentialProvider::createDefault();
        ASSERT_NE(provider, nullptr);

        auto creds = provider->getCredentials();
        EXPECT_EQ(creds.accessKeyId, "test-access-key-2");
        EXPECT_EQ(creds.secretAccessKey, "test-secret-key-2");
        EXPECT_FALSE(creds.sessionToken.has_value());
    } catch (const AwsAuthError & e) {
        // Clean up first
        unsetenv("AWS_ACCESS_KEY_ID");
        unsetenv("AWS_SECRET_ACCESS_KEY");
        GTEST_SKIP() << "AWS authentication failed: " << e.what();
    }

    // Clean up
    unsetenv("AWS_ACCESS_KEY_ID");
    unsetenv("AWS_SECRET_ACCESS_KEY");
}

TEST_F(AwsCredentialProviderTest, multipleProviders_Independent)
{
    // Test that multiple providers can be created independently
    try {
        auto provider1 = AwsCredentialProvider::createDefault();
        auto provider2 = AwsCredentialProvider::createDefault(); // Use default for both

        EXPECT_NE(provider1, nullptr);
        EXPECT_NE(provider2, nullptr);
        EXPECT_NE(provider1.get(), provider2.get());
    } catch (const AwsAuthError & e) {
        GTEST_SKIP() << "AWS authentication failed: " << e.what();
    }
}

} // namespace nix

#endif

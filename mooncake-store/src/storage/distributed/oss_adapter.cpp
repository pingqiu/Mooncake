#include "storage/distributed/oss_adapter.h"

#include <boost/algorithm/string.hpp>

#include <utility>

#include "config/oss_adapter_config.h"
#include "storage/distributed/object_storage_signing.h"

namespace mooncake {
namespace {

using object_storage_signing::CanonicalQuery;
using object_storage_signing::Hex;
using object_storage_signing::HmacSha256;
using object_storage_signing::Sha256Hex;
using object_storage_signing::SigningTimestamp;
using object_storage_signing::UriEncode;

}  // namespace

tl::expected<void, ErrorCode> OssObjectStorageAdapter::Init() {
    auto config = OssAdapterConfig::FromEnvironment();
    if (!config.has_value()) {
        return tl::make_unexpected(config.error());
    }
    endpoint_ = std::move(config->endpoint);
    bucket_ = std::move(config->bucket);
    region_ = std::move(config->region);
    access_key_id_ = std::move(config->access_key_id);
    access_key_secret_ = std::move(config->access_key_secret);
    security_token_ = std::move(config->security_token);
    path_style_ = config->path_style;
    anonymous_ = config->anonymous;
    max_connections_ = config->max_connections;
    receive_buffer_size_ = config->receive_buffer_size;
    upload_buffer_size_ = config->upload_buffer_size;
    FinishInit();
    return {};
}

std::string OssObjectStorageAdapter::BuildAuthorization(
    const std::string& method, const std::string& physical_key,
    const std::map<std::string, std::string>& query,
    const std::string& timestamp,
    const std::map<std::string, std::string>& oss_headers) const {
    const std::string date = timestamp.substr(0, 8);
    const std::string canonical_uri =
        UriEncode("/" + bucket_ + "/" + physical_key, true);
    std::string canonical_headers;
    for (const auto& [name, value] : oss_headers) {
        canonical_headers +=
            name + ":" + boost::algorithm::trim_copy(value) + "\n";
    }
    // OSS V4 signs Content-Type, Content-MD5, and x-oss-* headers by
    // default. AdditionalHeaders is therefore empty because this request
    // does not sign any optional, non-default headers.
    const std::string canonical_request =
        method + "\n" + canonical_uri + "\n" + CanonicalQuery(query) + "\n" +
        canonical_headers + "\n\nUNSIGNED-PAYLOAD";
    const std::string scope = date + "/" + region_ + "/oss/aliyun_v4_request";
    const std::string string_to_sign = "OSS4-HMAC-SHA256\n" + timestamp + "\n" +
                                       scope + "\n" +
                                       Sha256Hex(canonical_request);

    const std::string secret = "aliyun_v4" + access_key_secret_;
    auto date_key = HmacSha256(secret.data(), secret.size(), date);
    auto region_key = HmacSha256(date_key.data(), date_key.size(), region_);
    auto service_key = HmacSha256(region_key.data(), region_key.size(), "oss");
    auto signing_key =
        HmacSha256(service_key.data(), service_key.size(), "aliyun_v4_request");
    auto signature =
        HmacSha256(signing_key.data(), signing_key.size(), string_to_sign);
    return "OSS4-HMAC-SHA256 Credential=" + access_key_id_ + "/" + scope +
           ",Signature=" + Hex(signature.data(), signature.size());
}

std::vector<std::string> OssObjectStorageAdapter::BuildSignedHeaders(
    const std::string& method, const std::string& physical_key,
    const std::map<std::string, std::string>& query,
    const std::string& range) const {
    const std::string timestamp = SigningTimestamp().first;
    std::map<std::string, std::string> oss_headers{
        {"x-oss-content-sha256", "UNSIGNED-PAYLOAD"},
        {"x-oss-date", timestamp},
    };
    if (!security_token_.empty())
        oss_headers["x-oss-security-token"] = security_token_;
    if (!range.empty()) oss_headers["x-oss-range-behavior"] = "standard";
    std::vector<std::string> headers;
    for (const auto& [name, value] : oss_headers) {
        headers.push_back(name + ": " + value);
    }
    if (!anonymous_)
        headers.push_back("Authorization: " +
                          BuildAuthorization(method, physical_key, query,
                                             timestamp, oss_headers));
    return headers;
}

}  // namespace mooncake

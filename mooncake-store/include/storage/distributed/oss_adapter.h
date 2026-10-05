#pragma once

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "storage/distributed/rest_object_storage_adapter.h"

namespace mooncake {

/**
 * Alibaba Cloud OSS implementation of ObjectStorageAdapter.
 *
 * Configuration is read by Init() from MOONCAKE_OSS_* environment variables.
 * Requests are signed with OSS V4; the request engine is shared with the S3
 * adapter through RestObjectStorageAdapter.
 */
class OssObjectStorageAdapter final : public RestObjectStorageAdapter {
   public:
    explicit OssObjectStorageAdapter(std::string key_prefix)
        : RestObjectStorageAdapter(std::move(key_prefix)) {}
    ~OssObjectStorageAdapter() override = default;

    tl::expected<void, ErrorCode> Init() override;
    const char* GetName() const override { return "oss"; }

   protected:
    std::vector<std::string> BuildSignedHeaders(
        const std::string& method, const std::string& physical_key,
        const std::map<std::string, std::string>& query,
        const std::string& range) const override;
    bool EqualsForEmptyQueryValue() const override { return false; }
    bool ContinuationTokenIsUrlEncoded() const override { return true; }
    const char* LogName() const override { return "OSS"; }

   private:
    std::string BuildAuthorization(
        const std::string& method, const std::string& physical_key,
        const std::map<std::string, std::string>& query,
        const std::string& timestamp,
        const std::map<std::string, std::string>& oss_headers) const;
};

}  // namespace mooncake

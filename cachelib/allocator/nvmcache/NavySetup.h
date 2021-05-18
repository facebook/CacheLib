#pragma once
#include <folly/dynamic.h>

#include "cachelib/allocator/nvmcache/NavyConfig.h"
#include "cachelib/navy/AbstractCache.h"
namespace facebook {
namespace cachelib {
// return a navy cache which is created by CacheProto whose data is from
// NavyConfig.
std::unique_ptr<facebook::cachelib::navy::AbstractCache> createNavyCache(
    const navy::NavyConfig& config,
    facebook::cachelib::navy::DestructorCallback cb,
    bool truncate,
    std::shared_ptr<navy::DeviceEncryptor> encryptor);

// public only for testing
std::unique_ptr<cachelib::navy::Device> createDevice(
    const navy::NavyConfig& config,
    std::shared_ptr<navy::DeviceEncryptor> encryptor);
} // namespace cachelib
} // namespace facebook

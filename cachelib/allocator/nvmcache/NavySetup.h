#pragma once
#include <folly/dynamic.h>

#include "cachelib/allocator/nvmcache/NavyConfig.h"
#include "cachelib/navy/AbstractCache.h"
namespace facebook {
namespace cachelib {
std::unique_ptr<facebook::cachelib::navy::AbstractCache> createNavyCache(
    const folly::dynamic& options,
    facebook::cachelib::navy::DestructorCallback cb,
    bool truncate,
    std::shared_ptr<navy::DeviceEncryptor> encryptor);

void populateDefaultNavyOptions(folly::dynamic& options);

// returns the size threshold for small engine vs large engine. If small
// engine is disabled, returns 0 to indicate all objects will be in large
// engine.
uint64_t getSmallItemThreshold(const folly::dynamic& options);

// public only for testing
std::unique_ptr<cachelib::navy::Device> createDevice(
    const folly::dynamic& options,
    std::shared_ptr<navy::DeviceEncryptor> encryptor);

std::unique_ptr<cachelib::navy::Device> createDevice(
    const navy::NavyConfig& config,
    std::shared_ptr<navy::DeviceEncryptor> encryptor);
} // namespace cachelib
} // namespace facebook

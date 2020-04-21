#pragma once
#include "cachelib/navy/AbstractCache.h"

#include <folly/dynamic.h>
namespace facebook {
namespace cachelib {

std::unique_ptr<facebook::cachelib::navy::AbstractCache> createNavyCache(
    const folly::dynamic& options,
    facebook::cachelib::navy::DestructorCallback cb,
    bool truncate,
    std::shared_ptr<navy::DeviceEncryptor> encryptor);

void populateDefaultNavyOptions(folly::dynamic& options);

uint64_t getNvmCacheSize(const folly::dynamic& options);

// returns the size threshold for small engine vs large engine. If small
// engine is disabled, returns 0 to indicate all objects will be in large
// engine.
uint64_t getSmallItemThreshold(const folly::dynamic& options);

} // namespace cachelib
} // namespace facebook

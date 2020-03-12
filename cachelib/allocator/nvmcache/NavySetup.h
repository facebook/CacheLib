#pragma once
#include "cachelib/navy/AbstractCache.h"

#include <folly/dynamic.h>
namespace facebook {
namespace cachelib {

std::unique_ptr<facebook::cachelib::navy::AbstractCache> createNavyCache(
    const folly::dynamic& options,
    facebook::cachelib::navy::DestructorCallback cb,
    bool truncate);

void populateDefaultNavyOptions(folly::dynamic& options);

uint64_t getNvmCacheSize(const folly::dynamic& options);

} // namespace cachelib
} // namespace facebook

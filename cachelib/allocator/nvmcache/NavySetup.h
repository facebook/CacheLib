#pragma once
#include <folly/dynamic.h>

#include "cachelib/allocator/nvmcache/NavyConfig.h"
#include "cachelib/navy/AbstractCache.h"
namespace facebook {
namespace cachelib {
// returns true if navy config is single file
bool usesSimpleFile(const folly::dynamic& options);

// returns true if navy config is raid multile files
bool usesRaidFiles(const folly::dynamic& options);

// return a navy cache which is created by CacheProto whose data is from
// dipperOptions (to be deprecated).
std::unique_ptr<facebook::cachelib::navy::AbstractCache> createNavyCache(
    const folly::dynamic& options,
    facebook::cachelib::navy::DestructorCallback cb,
    bool truncate,
    std::shared_ptr<navy::DeviceEncryptor> encryptor);
// return a navy cache which is created by CacheProto whose data is from
// NavyConfig.
std::unique_ptr<facebook::cachelib::navy::AbstractCache> createNavyCache(
    const navy::NavyConfig& config,
    facebook::cachelib::navy::DestructorCallback cb,
    bool truncate,
    std::shared_ptr<navy::DeviceEncryptor> encryptor);

void populateDefaultNavyOptions(folly::dynamic& options);

// returns the size threshold for small engine vs large engine. If small
// engine is disabled, returns 0 to indicate all objects will be in large
// engine.
uint64_t getSmallItemThreshold(const folly::dynamic& options,
                               const navy::NavyConfig& config);

// public only for testing
std::unique_ptr<cachelib::navy::Device> createDevice(
    const folly::dynamic& options,
    std::shared_ptr<navy::DeviceEncryptor> encryptor);

std::unique_ptr<cachelib::navy::Device> createDevice(
    const navy::NavyConfig& config,
    std::shared_ptr<navy::DeviceEncryptor> encryptor);

// validate file/raid paths config options, throws std::invalid_argument if
// anything is invalid
void validatePathConfig(const folly::dynamic& options);

// returns navy file path
// use only when usesSimpleFile() returns true
std::string getNavyFilePath(const folly::dynamic& options);

// returns ordered navy raid paths
// use only when usesRaidFiles() returns true
std::vector<std::string> getNavyRaidPaths(const folly::dynamic& options);

// returns navy file size
uint64_t getNavyFileSize(const folly::dynamic& options);

} // namespace cachelib
} // namespace facebook

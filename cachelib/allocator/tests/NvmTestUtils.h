#pragma once

#include <folly/dynamic.h>

#include "cachelib/allocator/nvmcache/NavyConfig.h"

namespace facebook {
namespace cachelib {
namespace tests {
namespace utils {
using NavyConfig = navy::NavyConfig;
inline NavyConfig getNvmTestConfig(const std::string& cacheDir) {
  NavyConfig config{};
  config.setSimpleFile(cacheDir + "/navy", 100 * 1024ULL * 1024ULL);
  config.setBlockCacheRegionSize(4 * 1024 * 1024);
  config.setDeviceMetadataSize(4 * 1024 * 1024);
  config.setBlockCacheLru(true);
  config.setBlockSize(1024);
  config.setNavyReqOrderingShards(10);
  config.setBigHash(50, 1024, 8, 100);
  return config;
}

} // namespace utils
} // namespace tests
} // namespace cachelib
} // namespace facebook

#pragma once

#include <folly/dynamic.h>

#include "cachelib/allocator/nvmcache/NavyConfig.h"

namespace facebook {
namespace cachelib {
namespace tests {
namespace utils {

inline folly::dynamic getNvmTestOptions(const std::string& cacheDir) {
  folly::dynamic options = folly::dynamic::object;
  options["dipper_navy_recovery_path"] = cacheDir;
  options["dipper_navy_file_size"] = 100 * 1024ULL * 1024ULL; /* megabytes */
  options["dipper_navy_file_name"] = cacheDir + "/navy";
  options["dipper_navy_region_size"] = 4 * 1024 * 1024;   /* 4 MB */
  options["dipper_navy_metadata_size"] = 4 * 1024 * 1024; /* 4 MB */
  options["dipper_navy_lru"] = true;
  options["dipper_navy_block_size"] = 1024;
  options["dipper_navy_req_order_shards_power"] = 10;
  options["dipper_navy_bighash_size_pct"] = 50;
  options["dipper_navy_bighash_bucket_size"] = 1024;
  options["dipper_navy_small_item_max_size"] = 100;
  return options;
}

using NavyConfig = navy::NavyConfig;
inline NavyConfig getNvmTestConfig(const std::string& cacheDir) {
  NavyConfig config{};
  config.setFileSize(100 * 1024ULL * 1024ULL);
  config.setFileName(cacheDir + "/navy");
  config.setBlockCacheRegionSize(4 * 1024 * 1024);
  config.setDeviceMetadataSize(4 * 1024 * 1024);
  config.setBlockCacheLru(true);
  config.setBlockSize(1024);
  config.setNavyReqOrderingShards(10);
  config.setBigHashSizePct(50);
  config.setBigHashBucketSize(1024);
  config.setBigHashSmallItemMaxSize(100);
  config.disable(); // add this line in order to test dipper options
                    // functionality
  return config;
}

} // namespace utils
} // namespace tests
} // namespace cachelib
} // namespace facebook

#pragma once

#include <folly/dynamic.h>

namespace facebook {
namespace cachelib {
namespace tests {
namespace utils {

inline folly::dynamic getNvmTestConfig(const std::string& cacheDir) {
  folly::dynamic config = folly::dynamic::object;
  config["dipper_navy_recovery_path"] = cacheDir;
  config["dipper_navy_file_size"] = 100 * 1024ULL * 1024ULL; /* megabytes */
  config["dipper_navy_file_name"] = cacheDir + "/navy";
  config["dipper_navy_region_size"] = 4 * 1024 * 1024;   /* 4 MB */
  config["dipper_navy_metadata_size"] = 4 * 1024 * 1024; /* 4 MB */
  config["dipper_navy_lru"] = true;
  config["dipper_navy_block_size"] = 1024;
  config["dipper_navy_req_order_shards_power"] = 10;
  config["dipper_navy_bighash_size_pct"] = 50;
  config["dipper_navy_bighash_bucket_size"] = 1024;
  config["dipper_navy_small_item_max_size"] = 100;
  return config;
}

} // namespace utils
} // namespace tests
} // namespace cachelib
} // namespace facebook

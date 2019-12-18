#pragma once

#include <cstdint>
#include <string>

namespace facebook {
namespace cachelib {
// If you're bumping kCacheRamFormatVersion or kCacheNvmFormatVersion,
// you *MUST* bump this as well.
//
// If the change is not an incompatible one, but you want to keep track of it,
// then you only need to bump this version.
// I.e. you're rolling out a new feature that is cache compatible with previous
// Cachelib instances.
constexpr uint64_t kCachelibVersion = 11;

// Updating this version will cause RAM cache to be dropped for all
// cachelib users!!! Proceed with care!! You must coordinate with
// cachelib users either directly or via Cache Library group.
//
// If you're bumping this version, you *MUST* bump kCachelibVersion
// as well.
constexpr uint64_t kCacheRamFormatVersion = 2;

// Updating this version will cause NVM cache to be dropped for all
// cachelib users!!! Proceed with care!! You must coordinate with
// cachelib users either directly or via Cache Library group.
//
// If you're bumping this version, you *MUST* bump kCachelibVersion
// as well.
constexpr uint64_t kCacheNvmFormatVersion = 2;

inline const std::string& getCacheVersionString() {
  // allocator: X, ram: Y, nvm: Z
  static std::string kVersionStr =
      "{ \"cachelib\" : " + std::to_string(kCachelibVersion) +
      ", \"ram\" : " + std::to_string(kCacheRamFormatVersion) +
      ", \"nvm\" : " + std::to_string(kCacheNvmFormatVersion) + "}";
  return kVersionStr;
}
} // namespace cachelib
} // namespace facebook

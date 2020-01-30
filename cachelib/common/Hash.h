#pragma once

#include <folly/hash/Hash.h>

#include "cachelib/common/MurmurHash.h"

namespace facebook {
namespace cachelib {

// Hasher object for the hash table.
struct Hash {
  virtual ~Hash() = default;
  virtual uint32_t operator()(const void* buf, size_t n) const noexcept = 0;
  virtual int getMagicId() const noexcept = 0;
};
using Hasher = std::shared_ptr<Hash>;

struct FNVHash final : public Hash {
  uint32_t operator()(const void* buf, size_t n) const noexcept override {
    return folly::hash::fnv32_buf(buf, n, folly::hash::FNV_32_HASH_START);
  }
  int getMagicId() const noexcept override { return 1; }
};

struct MurmurHash2 final : public Hash {
  uint32_t operator()(const void* buf, size_t n) const noexcept override {
    return facebook::cachelib::murmurHash2(buf, static_cast<int>(n),
                                           kMurmur2Seed);
  }
  int getMagicId() const noexcept override { return 2; }

  constexpr static uint64_t kMurmur2Seed = 4193360111ul;
};
} // namespace cachelib
} // namespace facebook

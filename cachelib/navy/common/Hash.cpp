#include "Hash.h"

#include <folly/hash/Checksum.h>
#include <folly/hash/Hash.h>

namespace facebook {
namespace cachelib {
namespace navy {
uint64_t hashBuffer(BufferView key, uint64_t seed) {
  return folly::hash::SpookyHashV2::Hash64(key.data(), key.size(), seed);
}

uint64_t hashInt(uint64_t key) { return folly::hash::twang_mix64(key); }

uint64_t combineHashes(uint64_t h1, uint64_t h2) {
  return folly::hash::hash_128_to_64(h1, h2);
}

uint32_t checksum(BufferView data, uint32_t startingChecksum) {
  return folly::crc32(data.data(), data.size(), startingChecksum);
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

#include "cachelib/navy/common/Hash.h"

#include <folly/hash/Checksum.h>
#include <folly/hash/Hash.h>

namespace facebook {
namespace cachelib {
namespace navy {
uint64_t hashBuffer(BufferView key, uint64_t seed) {
  return folly::hash::SpookyHashV2::Hash64(key.data(), key.size(), seed);
}

uint32_t checksum(BufferView data, uint32_t startingChecksum) {
  return folly::crc32(data.data(), data.size(), startingChecksum);
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

#pragma once

#include <atomic>
#include <cstdint>
#include <map>
#include <vector>

#include <folly/logging/xlog.h>

#include "cachelib/common/AtomicCounter.h"

namespace facebook {
namespace cachelib {
namespace navy {
class KangarooSizeDistribution {
 public:
  // Create a size distribution that spans [@min, @max] at a granularity of
  // @factor. (equal spacing in buckets)
  KangarooSizeDistribution(uint64_t min, uint64_t max, uint64_t factor);

  // Recover from a previously saved snapshot
  explicit KangarooSizeDistribution(std::map<int64_t, int64_t> snapshot);

  void addSize(uint64_t size);
  void removeSize(uint64_t size);

  // Return {size -> number of items} mapping
  // Return signed value so it's easy to use this with thrift structures
  std::map<int64_t, int64_t> getSnapshot() const;

  void reset();

 private:
  std::map<uint64_t, AtomicCounter> dist_;
  uint64_t maxValue_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

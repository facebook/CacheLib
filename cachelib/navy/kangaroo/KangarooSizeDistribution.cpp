#include "cachelib/navy/kangaroo/KangarooSizeDistribution.h"

#include <algorithm>
#include <cassert>

namespace facebook {
namespace cachelib {
namespace navy {
namespace {
std::vector<uint64_t> generateSizes(uint64_t min, uint64_t max, uint64_t factor) {
  XDCHECK_GE(factor, 1);
  std::vector<uint64_t> sizes;
  uint64_t current = min;
  while (current < max) {
    sizes.push_back(current);
    current += factor;
  }
  sizes.push_back(max);
  return sizes;
}
} // namespace

KangarooSizeDistribution::KangarooSizeDistribution(uint64_t min, uint64_t max, uint64_t factor) {
  maxValue_ = max;
  auto sizes = generateSizes(min, max, factor);
  for (auto size : sizes) {
    dist_.emplace(size, AtomicCounter{});
  }
}

KangarooSizeDistribution::KangarooSizeDistribution(std::map<int64_t, int64_t> snapshot) {
  for (const auto& kv : snapshot) {
    dist_.emplace(static_cast<uint64_t>(kv.first),
                  AtomicCounter{static_cast<uint64_t>(kv.second)});
  }
}

void KangarooSizeDistribution::addSize(uint64_t size) {
  // TODO: It's possible user warm-rolled cache from a version without
  // KangarooSizeDistribution support. We will remove this once we bring an ice-roll.
  if (dist_.empty()) {
    return;
  }
  
  if (size > maxValue_) {
    XLOG(INFO, "overrun max in kangaroo size distribution at ", size);
    size = maxValue_;
  }

  auto res =
      std::lower_bound(dist_.begin(), dist_.end(), size, [](auto itr1, auto s) {
        return itr1.first < s;
      });
  XDCHECK_NE(res, dist_.end());
  res->second.add(size);
}

void KangarooSizeDistribution::removeSize(uint64_t size) {
  // TODO: It's possible user warm-rolled cache from a version without
  // KangarooSizeDistribution support. We will remove this once we bring an ice-roll.
  if (dist_.empty()) {
    return;
  }

  auto res =
      std::lower_bound(dist_.begin(), dist_.end(), size, [](auto itr1, auto s) {
        return itr1.first < s;
      });
  XDCHECK_NE(res, dist_.end());
  res->second.sub(size);
}

std::map<int64_t, int64_t> KangarooSizeDistribution::getSnapshot() const {
  std::map<int64_t, int64_t> snapshot;
  for (auto& kv : dist_) {
    snapshot.emplace(static_cast<int64_t>(kv.first),
                     static_cast<int64_t>(kv.second.get()));
  }
  return snapshot;
}

void KangarooSizeDistribution::reset() {
  for (auto& d : dist_) {
    d.second.set(0);
  }
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

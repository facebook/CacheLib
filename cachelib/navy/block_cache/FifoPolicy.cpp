#include "cachelib/navy/block_cache/FifoPolicy.h"

namespace facebook {
namespace cachelib {
namespace navy {
FifoPolicy::FifoPolicy() { XLOG(INFO, "FIFO policy"); }

void FifoPolicy::track(const Region& region) {
  std::lock_guard<std::mutex> lock{mutex_};
  queue_.push_back(region.id());
}

RegionId FifoPolicy::evict() {
  std::lock_guard<std::mutex> lock{mutex_};
  if (queue_.empty()) {
    return RegionId{};
  }
  auto rid = queue_.front();
  queue_.pop_front();
  return rid;
}

void FifoPolicy::reset() {
  std::lock_guard<std::mutex> lock{mutex_};
  queue_.clear();
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

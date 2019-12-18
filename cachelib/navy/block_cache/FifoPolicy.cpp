#include "FifoPolicy.h"

namespace facebook {
namespace cachelib {
namespace navy {
FifoPolicy::FifoPolicy() { XLOG(INFO, "FIFO policy"); }

void FifoPolicy::track(RegionId rid) {
  std::lock_guard<std::mutex> lock{mutex_};
  queue_.push_back(rid);
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

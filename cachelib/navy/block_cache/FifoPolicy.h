#pragma once

#include <deque>
#include <mutex>

#include "cachelib/navy/block_cache/EvictionPolicy.h"

namespace facebook {
namespace cachelib {
namespace navy {
// Simple FIFO policy
class FifoPolicy final : public EvictionPolicy {
 public:
  FifoPolicy();
  FifoPolicy(const FifoPolicy&) = delete;
  FifoPolicy& operator=(const FifoPolicy&) = delete;
  ~FifoPolicy() override = default;

  void touch(RegionId /* rid */) override {}
  void track(RegionId rid) override;
  RegionId evict() override;
  void reset() override;

  size_t memorySize() const override {
    std::lock_guard<std::mutex> lock{mutex_};
    return sizeof(*this) + sizeof(RegionId) * queue_.size();
  }

  void getCounters(const CounterVisitor&) const override {}

 private:
  std::deque<RegionId> queue_;
  mutable std::mutex mutex_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

#include "cachelib/allocator/Cache.h"

#include <mutex>

#include "cachelib/allocator/RebalanceStrategy.h"

namespace facebook {
namespace cachelib {

namespace detail {
const std::string kShmInfoName = "shm_info";
const std::string kShmCacheName = "shm_cache";
const std::string kShmHashTableName = "shm_hash_table";
const std::string kShmChainedItemHashTableName = "shm_chained_alloc_hash_table";
} // namespace detail

void CacheBase::setRebalanceStrategy(
    PoolId pid, std::shared_ptr<RebalanceStrategy> strategy) {
  std::unique_lock<std::mutex> l(lock_);
  poolRebalanceStrategies_[pid] = std::move(strategy);
}

std::shared_ptr<RebalanceStrategy> CacheBase::getRebalanceStrategy(
    PoolId pid) const {
  std::unique_lock<std::mutex> l(lock_);
  auto it = poolRebalanceStrategies_.find(pid);
  if (it != poolRebalanceStrategies_.end() && it->second) {
    return it->second;
  }
  return nullptr;
}

void CacheBase::setResizeStrategy(PoolId pid,
                                  std::shared_ptr<RebalanceStrategy> strategy) {
  std::unique_lock<std::mutex> l(lock_);
  poolResizeStrategies_[pid] = std::move(strategy);
}

std::shared_ptr<RebalanceStrategy> CacheBase::getResizeStrategy(
    PoolId pid) const {
  std::unique_lock<std::mutex> l(lock_);
  auto it = poolResizeStrategies_.find(pid);
  if (it != poolResizeStrategies_.end() && it->second) {
    return it->second;
  }
  return nullptr;
}

void CacheBase::setPoolOptimizeStrategy(
    std::shared_ptr<PoolOptimizeStrategy> strategy) {
  std::unique_lock<std::mutex> l(lock_);
  poolOptimizeStrategy_ = std::move(strategy);
}

std::shared_ptr<PoolOptimizeStrategy> CacheBase::getPoolOptimizeStrategy()
    const {
  std::unique_lock<std::mutex> l(lock_);
  return poolOptimizeStrategy_;
}
} // namespace cachelib
} // namespace facebook

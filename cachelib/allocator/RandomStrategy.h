#pragma once

#include "cachelib/allocator/RebalanceStrategy.h"

#include <folly/Random.h>

namespace facebook {
namespace cachelib {

// simple implementation for testing slab release that picks random victims
// and receivers. This randomness can be controlled to make the choices
// deterministic as well.
class RandomStrategy : public RebalanceStrategy {
 public:
  struct Config : public BaseConfig {
    Config() = default;
    explicit Config(unsigned int m) : minSlabs(m) {}
    unsigned int minSlabs{1};
  };

  RandomStrategy() = default;
  explicit RandomStrategy(Config c) : RebalanceStrategy(Random), config_{c} {}

  RebalanceContext pickVictimAndReceiverImpl(const CacheBase& cache,
                                             PoolId pid) final {
    const auto stats = cache.getPoolStats(pid);
    auto victimIds =
        filterByNumEvictableSlabs(stats, stats.getClassIds(), config_.minSlabs);
    const auto victim = pickRandom(victimIds);
    auto receiverIds = stats.getClassIds();
    receiverIds.erase(victim);
    const auto receiver = pickRandom(receiverIds);
    return RebalanceContext{victim, receiver};
  }

  void updateConfig(const BaseConfig& baseConfig) override {
    config_ = static_cast<const Config&>(baseConfig);
  }

 private:
  ClassId pickRandom(const std::set<ClassId>& classIds) {
    auto r = folly::Random::rand32(0, classIds.size());
    for (auto c : classIds) {
      if (r-- == 0) {
        return c;
      }
    }
    return Slab::kInvalidClassId;
  }

  Config config_{};
};
} // namespace cachelib
} // namespace facebook

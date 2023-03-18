/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <folly/Random.h>

#include "cachelib/allocator/RebalanceStrategy.h"

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

  RebalanceContext pickVictimAndReceiverImpl(const CacheBase&,
                                             PoolId,
                                             const PoolStats& stats) final {
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

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

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <unordered_map>
#include <vector>

namespace facebook {
namespace cachelib {

// Data structure that ranks the entities according to marginal hits.
template <typename EntityId>
struct MarginalHitsState {
  // IDs for entities that participate in size optimization
  std::vector<EntityId> entities;

  // smoothed rankings for entities
  std::unordered_map<EntityId, double> smoothedRanks;

  // sort rankings for current round and update smoothed rankings
  void updateRankings(const std::unordered_map<EntityId, double>& scores,
                      double movingAverageParam) {
    return updateRankingsImpl(
        entities, scores, movingAverageParam, smoothedRanks);
  }

  // pick victim and receiver pools according to smoothed rankings
  // (validVictim and validReceiver are used to indicate whether an entity
  // can be picked as victim or receiver this round)
  std::pair<EntityId, EntityId> pickVictimAndReceiverFromRankings(
      const std::unordered_map<EntityId, bool>& validVictim,
      const std::unordered_map<EntityId, bool>& validReceiver,
      EntityId kInvalidEntityId) const {
    return pickVictimAndReceiverFromRankingsImpl(
        smoothedRanks, validVictim, validReceiver, kInvalidEntityId);
  }

 private:
  // internal helper functions

  static void sortEntitiesByScores(
      const std::unordered_map<EntityId, double>& scores,
      std::vector<EntityId>& entities);

  static double& updateMovingAverage(const double& newValue,
                                     const double& movingAverageParam,
                                     double& avg);

  static void updateRankingsImpl(
      std::vector<EntityId> entities,
      const std::unordered_map<EntityId, double>& scores,
      const double& movingAverageParam,
      std::unordered_map<EntityId, double>& smoothedRanks);

  static std::pair<EntityId, EntityId> pickVictimAndReceiverFromRankingsImpl(
      const std::unordered_map<EntityId, double>& smoothedRanks,
      const std::unordered_map<EntityId, bool>& validVictim,
      const std::unordered_map<EntityId, bool>& validReceiver,
      EntityId kInvalidEntityId);
};

} // namespace cachelib
} // namespace facebook

#include "cachelib/allocator/MarginalHitsState-inl.h"

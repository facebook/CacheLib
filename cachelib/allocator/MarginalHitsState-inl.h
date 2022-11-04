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

namespace facebook {
namespace cachelib {
template <typename EntityId>
void MarginalHitsState<EntityId>::sortEntitiesByScores(
    const std::unordered_map<EntityId, double>& scores,
    std::vector<EntityId>& entities) {
  auto cmp = [&](auto x, auto y) { return scores.at(x) < scores.at(y); };
  std::sort(entities.begin(), entities.end(), cmp);
}

template <typename EntityId>
double& MarginalHitsState<EntityId>::updateMovingAverage(
    const double& newValue, const double& movingAverageParam, double& avg) {
  avg = avg * movingAverageParam + newValue * (1 - movingAverageParam);
  return avg;
}

template <typename EntityId>
void MarginalHitsState<EntityId>::updateRankingsImpl(
    std::vector<EntityId> entities,
    const std::unordered_map<EntityId, double>& scores,
    const double& movingAverageParam,
    std::unordered_map<EntityId, double>& smoothedRanks) {
  sortEntitiesByScores(scores, entities);
  for (uint32_t i = 0; i < entities.size(); i++) {
    updateMovingAverage(i, movingAverageParam, smoothedRanks[entities[i]]);
  }
}

template <typename EntityId>
std::pair<EntityId, EntityId>
MarginalHitsState<EntityId>::pickVictimAndReceiverFromRankingsImpl(
    const std::unordered_map<EntityId, double>& smoothedRanks,
    const std::unordered_map<EntityId, bool>& validVictim,
    const std::unordered_map<EntityId, bool>& validReceiver,
    EntityId kInvalidEntityId) {
  EntityId victimId = kInvalidEntityId;
  EntityId receiverId = kInvalidEntityId;
  double minRank = smoothedRanks.size();
  double maxRank = -1;
  for (auto it : smoothedRanks) {
    if (validReceiver.at(it.first) && it.second > maxRank) {
      maxRank = it.second;
      receiverId = it.first;
    }
    if (validVictim.at(it.first) && it.second < minRank) {
      minRank = it.second;
      victimId = it.first;
    }
  }
  return std::make_pair(victimId, receiverId);
}
} // namespace cachelib
} // namespace facebook

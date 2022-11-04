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

#include <folly/Random.h>

#include "cachelib/allocator/MarginalHitsState.h"
#include "cachelib/allocator/tests/TestBase.h"

namespace facebook {
namespace cachelib {

namespace tests {
class MarginalHitsStateTest : public testing::Test {};

TEST_F(MarginalHitsStateTest, PickAccordingToScores) {
  MarginalHitsState<int> state;
  const std::vector<int> entities = {0, 1, 2};
  const auto smoothness = 0.3;
  std::unordered_map<int, double> scores;
  std::unordered_map<int, bool> validVictim;
  std::unordered_map<int, bool> validReceiver;

  // initialization
  state.entities = entities;

  // scores: entity 2 > entity 0 > entity 1, but entity 2 cannot be receiver
  {
    scores[0] = 0.2;
    scores[1] = 0.1;
    scores[2] = 0.3;
    state.updateRankings(scores, smoothness);
    EXPECT_DOUBLE_EQ(0.7, state.smoothedRanks[0]);
    EXPECT_DOUBLE_EQ(0.0, state.smoothedRanks[1]);
    EXPECT_DOUBLE_EQ(1.4, state.smoothedRanks[2]);

    validVictim[0] = true;
    validVictim[1] = true;
    validVictim[2] = true;
    validReceiver[0] = true;
    validReceiver[1] = true;
    validReceiver[2] = false;
    auto selection =
        state.pickVictimAndReceiverFromRankings(validVictim, validReceiver, -1);
    EXPECT_EQ(1, selection.first);  // victim
    EXPECT_EQ(0, selection.second); // receiver
  }

  // scores: entity 1 > entity 2 > entity 0, but entity 0 cannot be victim
  {
    scores[0] = 0.1;
    scores[1] = 0.3;
    scores[2] = 0.2;
    state.updateRankings(scores, smoothness);
    EXPECT_DOUBLE_EQ(0.21, state.smoothedRanks[0]);
    EXPECT_DOUBLE_EQ(1.4, state.smoothedRanks[1]);
    EXPECT_DOUBLE_EQ(1.12, state.smoothedRanks[2]);

    validVictim[0] = false;
    validVictim[1] = true;
    validVictim[2] = true;
    validReceiver[0] = true;
    validReceiver[1] = true;
    validReceiver[2] = true;
    auto selection =
        state.pickVictimAndReceiverFromRankings(validVictim, validReceiver, -1);
    EXPECT_EQ(2, selection.first);  // victim
    EXPECT_EQ(1, selection.second); // receiver
  }

  // scores: entity 1 > entity 2 > entity 0, but no entity can be a victim
  {
    scores[0] = 0.1;
    scores[1] = 0.3;
    scores[2] = 0.2;
    state.updateRankings(scores, smoothness);
    EXPECT_DOUBLE_EQ(0.063, state.smoothedRanks[0]);
    EXPECT_DOUBLE_EQ(1.82, state.smoothedRanks[1]);
    EXPECT_DOUBLE_EQ(1.036, state.smoothedRanks[2]);

    validVictim[0] = false;
    validVictim[1] = false;
    validVictim[2] = false;
    validReceiver[0] = true;
    validReceiver[1] = true;
    validReceiver[2] = true;
    auto selection =
        state.pickVictimAndReceiverFromRankings(validVictim, validReceiver, -1);
    EXPECT_EQ(-1, selection.first); // victim
    EXPECT_EQ(1, selection.second); // receiver
  }

  // scores: entity 1 > entity 2 > entity 0, but no entity can be a receiver
  {
    scores[0] = 0.1;
    scores[1] = 0.3;
    scores[2] = 0.2;
    state.updateRankings(scores, smoothness);
    EXPECT_DOUBLE_EQ(0.0189, state.smoothedRanks[0]);
    EXPECT_DOUBLE_EQ(1.946, state.smoothedRanks[1]);
    EXPECT_DOUBLE_EQ(1.0108, state.smoothedRanks[2]);

    validVictim[0] = true;
    validVictim[1] = true;
    validVictim[2] = true;
    validReceiver[0] = false;
    validReceiver[1] = false;
    validReceiver[2] = false;
    auto selection =
        state.pickVictimAndReceiverFromRankings(validVictim, validReceiver, -1);
    EXPECT_EQ(0, selection.first);   // victim
    EXPECT_EQ(-1, selection.second); // receiver
  }
}

} // namespace tests
} // namespace cachelib
} // namespace facebook

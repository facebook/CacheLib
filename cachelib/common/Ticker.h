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
#include "cachelib/common/Time.h"
namespace facebook {
namespace cachelib {

// Inteface class for CacheLib to retrieve the current tick.
// A tick is a unit of time in operation. This interface allows
// user to override the concept of time in CacheLib and provide
// their own time management mechanism.
class Ticker {
 public:
  virtual uint32_t getCurrentTick() = 0;
  virtual ~Ticker() = default;
};

namespace detail {
// Ticker based on the clock.
// This is the default ticker.
class ClockBasedTicker : public Ticker {
 public:
  // Return the current second as the tick.
  uint32_t getCurrentTick() override { return util::getCurrentTimeSec(); }
};
} // namespace detail
} // namespace cachelib
} // namespace facebook

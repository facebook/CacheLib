// Copyright 2004-present Facebook. All Rights Reserved.

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

#pragma once

#include <atomic>
#include <memory>

namespace facebook {
namespace cachelib {
namespace cachebench {

// check whether the load test should stop. e.g. user interrupt the cachebench.
bool shouldTestStop();

// Called when stop request from user is captured. instead of stop the load
// test immediately, the method sets the state "stopped_" to true. Actual
// stop logic is in somewhere else.
void stopTest();

} // namespace cachebench
} // namespace cachelib
} // namespace facebook

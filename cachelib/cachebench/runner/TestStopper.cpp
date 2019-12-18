#include "cachelib/cachebench/runner/TestStopper.h"

#include <memory>

namespace facebook {
namespace cachelib {
namespace cachebench {

static std::atomic<bool> stopped(false);

bool shouldTestStop() { return stopped.load(std::memory_order_acquire); }

void stopTest() { stopped.store(true, std::memory_order_release); }

} // namespace cachebench
} // namespace cachelib
} // namespace facebook

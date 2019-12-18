#pragma once

#include <folly/logging/xlog.h>

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/allocator/MemoryMonitor.h"
#include "tupperware/agent/api/TaskMetadata.h"

namespace facebook {
namespace cachelib {
namespace twutil {

using facebook::tupperware::api::TaskMetadata;

// Configure a CacheAllocator::Config with defaults for a tupperware job.
// This will start advising away when RSS is within memUpperGB of the task's
// memory ResourceLimit and resume allocation once there is memLowerGB of
// free memory. The memory monitor will run every monitorInterval seconds to
// find slabs to advise away.
template <class AllocatorConfig>
bool enableMemoryMonitor(
    AllocatorConfig& config,
    unsigned int memUpperGB = 1,
    unsigned int memLowerGB = 4,
    std::chrono::milliseconds monitorInterval = std::chrono::seconds(5)) {
  const auto taskMaxMemBytes = TaskMetadata::getTaskMaxMemoryBytes();
  if (taskMaxMemBytes.hasError()) {
    XLOG(ERR) << "Failed fetching task metadata";
    return false;
  }

  auto taskMemGB = taskMaxMemBytes.value() / pow(1024, 3);
  config.memMonitorMode = cachelib::MemoryMonitor::ResidentMemory;
  config.memMonitorInterval = monitorInterval;
  config.memLowerLimit = taskMemGB - memLowerGB;
  config.memUpperLimit = taskMemGB - memUpperGB;
  CHECK_LT(config.memLowerLimit, config.memUpperLimit);
  XLOG(INFO) << "Initialized cachelib memory monitor to maintain "
             << config.memUpperLimit << "gb RSS and grow at "
             << config.memLowerLimit << "gb; task has max size of " << taskMemGB
             << "gb";
  return true;
}

} // namespace twutil
} // namespace cachelib
} // namespace facebook

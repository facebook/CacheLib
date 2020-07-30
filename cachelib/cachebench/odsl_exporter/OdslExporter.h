#pragma once

#include <memory>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#include "common/stats/ServiceData.h"
#include "monitoring/odsl/ODSCounters.h"
#include "monitoring/odsl/ODSGauges.h"
#include "monitoring/odsl/configuration/ODSDefaultConfig.h"
#pragma GCC diagnostic pop

namespace facebook {
namespace cachelib {
namespace cachebench {
class OdslExporter {
 public:
  OdslExporter();

 private:
  void exportData();

  std::shared_ptr<monitoring::ODSDefaultConfig> defaultConfig_;
  std::unordered_map<std::string, monitoring::ODSGauge> counters_;
  folly::FunctionScheduler dataExportScheduler_;
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook

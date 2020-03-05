#include "cachelib/cachebench/odsl_exporter/OdslExporter.h"

#include <folly/logging/xlog.h>

namespace facebook {
namespace cachelib {
namespace cachebench {
using namespace monitoring;
OdslExporter::OdslExporter()
    : defaultConfig_{
          std::make_shared<ODSDefaultConfig>(OdsCategoryId::ODS_CACHELIB)} {
  dataExportScheduler_.addFunction([this]() { exportData(); },
                                   std::chrono::seconds{60},
                                   "push_based_counters");
  dataExportScheduler_.setThreadName("CacheBenchDataExportScheduler");
  dataExportScheduler_.start();
}

void OdslExporter::exportData() {
  auto values = fbData->getCounters();

  for (const auto& pair : values) {
    if (pair.first.find("cachelib") == std::string::npos) {
      continue;
    }

    auto res = counters_.find(pair.first);
    if (res == counters_.end()) {
      auto itr = counters_.emplace(
          pair.first,
          ODSGauge{defaultConfig_,
                   std::string{pair.first} /* copy the string*/});
      itr.first->second = pair.second;
      continue;
    }
    res->second = pair.second;
  }
}
} // namespace cachebench
} // namespace cachelib
} // namespace facebook

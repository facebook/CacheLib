#include <folly/init/Init.h>
#include <folly/io/async/EventBase.h>
#include <folly/logging/LoggerDB.h>
#include <gflags/gflags.h>

// Comment out the define for FB_ENV when we build for external environments
#define CACHEBENCH_FB_ENV
#ifdef CACHEBENCH_FB_ENV
#include "cachelib/cachebench/fb303/FB303ThriftServer.h"
#include "cachelib/cachebench/odsl_exporter/OdslExporter.h"
#include "common/init/Init.h"
#endif

#include "cachelib/cachebench/runner/Runner.h"
#include "cachelib/common/Utils.h"

DEFINE_string(json_test_config,
              "",
              "path to test config. If empty, use default setting");
DEFINE_uint64(
    progress,
    60,
    "if set, prints progress every X seconds as configured, 0 to disable");
DEFINE_string(progress_stats_file,
              "",
              "Print detailed stats at each progress interval to this file");
DEFINE_int32(timeout_seconds,
             0,
             "Maximum allowed seconds for running test. 0 means no timeout");

#ifdef CACHEBENCH_FB_ENV
DEFINE_bool(export_to_ods, true, "Upload cachelib stats to ODS");
DEFINE_int32(fb303_port,
             0,
             "Port for cachebench fb303 service. If 0, do not export to fb303. "
             "If valid, this will disable ODSL export.");
#endif

std::unique_ptr<facebook::cachelib::cachebench::Runner> gRunner;

void sigint_handler(int sig_num) {
  switch (sig_num) {
  case SIGINT:
  case SIGTERM:
    if (gRunner) {
      gRunner->abort();
    }
    break;
  }
}

int main(int argc, char** argv) {
  using namespace facebook::cachelib::cachebench;

#ifdef CACHEBENCH_FB_ENV
  facebook::initFacebook(&argc, &argv);
  std::unique_ptr<OdslExporter> odslExporter_;
  std::unique_ptr<FB303ThriftService> fb303_;
  if (FLAGS_fb303_port == 0 && FLAGS_export_to_ods) {
    odslExporter_ = std::make_unique<OdslExporter>();
  } else if (FLAGS_fb303_port > 0) {
    fb303_ = std::make_unique<FB303ThriftService>(FLAGS_fb303_port);
  }
#else
  folly::init(&argc, &argv, true);
#endif

  gRunner = std::make_unique<Runner>(
      FLAGS_json_test_config, FLAGS_progress_stats_file, FLAGS_progress);

  // Handle signals properly
  struct sigaction act;
  memset(&act, 0, sizeof(struct sigaction));
  act.sa_handler = &sigint_handler;
  act.sa_flags = SA_RESETHAND;
  if (sigaction(SIGINT, &act, nullptr) == -1) {
    std::cout << "Failed to register a SIGINT handler" << std::endl;
    return 1;
  }

  // make sure the test end before timeout
  folly::EventBase eb;
  if (FLAGS_timeout_seconds > 0) {
    eb.runAfterDelay([]() { gRunner->abort(); }, FLAGS_timeout_seconds * 1000);
  }

  if (!gRunner->run()) {
    return 1;
  }
  return 0;
}

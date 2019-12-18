#include <folly/logging/LoggerDB.h>
#include <gflags/gflags.h>

#include "cachelib/cachebench/fb303/FB303ThriftServer.h"
#include "cachelib/cachebench/runner/Runner.h"
#include "cachelib/cachebench/runner/TestStopper.h"
#include "cachelib/common/Utils.h"
#include "cachelib/logger/ScubaLogger.h"
#include "common/init/Init.h"

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
DEFINE_int32(fb303_port, 12345, "Port for cachebench fb303 service.");

void sigint_handler(int sig_num) {
  switch (sig_num) {
  case SIGINT:
  case SIGTERM:
    facebook::cachelib::cachebench::stopTest();
    break;
  }
}

namespace folly {
const char* getBaseLoggingConfig() {
  facebook::cachelib::registerCachelibScubaLogger();
  return CACHELIB_FOLLY_LOGGING_CONFIG;
}
} // namespace folly

int main(int argc, char** argv) {
  using namespace facebook::cachelib::cachebench;

  // Handle signals properly
  struct sigaction act;
  memset(&act, 0, sizeof(struct sigaction));
  act.sa_handler = &sigint_handler;
  act.sa_flags = SA_RESETHAND;
  if (sigaction(SIGINT, &act, nullptr) == -1) {
    std::cout << "Failed to register a SIGINT handler" << std::endl;
    return 1;
  }

  facebook::initFacebook(&argc, &argv);

  std::unique_ptr<FB303ThriftService> fb303_;
  if (FLAGS_fb303_port) {
    fb303_ = std::make_unique<FB303ThriftService>(FLAGS_fb303_port);
  }

  Runner runner{FLAGS_json_test_config, FLAGS_progress_stats_file,
                FLAGS_progress};
  if (!runner.run()) {
    return 1;
  }
}

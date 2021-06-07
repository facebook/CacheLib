#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "cachelib/cachebench/runner/Runner.h"

using namespace facebook::cachelib;
using namespace std;

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

DEFINE_string(benchmark_name, "", "benchmark name");

BENCHMARK_COUNTERS(testCacheBench, counters) {
  std::cerr << "json_test_config: " << FLAGS_json_test_config << std::endl;

  facebook::cachelib::cachebench::CacheBenchConfig config(
      "cachelib/cachebench/test_configs/simple_test.json");
  auto cacheConfigCustomizer =
      [](facebook::cachelib::cachebench::CacheConfig c) { return c; };
  auto admPolicy =
      std::make_unique<facebook::cachelib::cachebench::StressorAdmPolicy>();

  facebook::cachelib::cachebench::Runner runner(
      config, FLAGS_progress_stats_file, FLAGS_progress, cacheConfigCustomizer,
      std::move(admPolicy));
  runner.run(counters);
}

int main(int argc, char** argv) {
  folly::init(&argc, &argv);

  // add folly benchmark programmingly, if this works we only need one binary
  // for cachebench and pass in the benchmark name and config for different
  // testings
  folly::addBenchmark(
      __FILE__, FLAGS_benchmark_name, [=](folly::UserCounters& counters) {
        facebook::cachelib::cachebench::CacheBenchConfig config(
            FLAGS_json_test_config);
        auto cacheConfigCustomizer =
            [](facebook::cachelib::cachebench::CacheConfig c) { return c; };
        auto admPolicy = std::make_unique<
            facebook::cachelib::cachebench::StressorAdmPolicy>();

        facebook::cachelib::cachebench::Runner runner(
            config, FLAGS_progress_stats_file, FLAGS_progress,
            cacheConfigCustomizer, std::move(admPolicy));
        runner.run(counters);
        return 1;
      });

  folly::runBenchmarks();
  return 0;
}

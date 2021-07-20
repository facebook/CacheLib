#include <folly/Benchmark.h>
#include <folly/experimental/io/FsUtil.h>
#include <folly/init/Init.h>

#include "cachelib/cachebench/runner/Runner.h"

using namespace facebook::cachelib::cachebench;

DEFINE_string(json_test_config,
              "",
              "path to test config, the path is relatvie to "
              "cachelib/cachebench/test_configs");
DEFINE_string(benchmark_name, "", "benchmark name");

/**
 * This is a special version of CacheBench for Servicelab benchmark ONLY.
 * For manual run, use 'cachelib/cachebench:cachebench'
 *
 * Servicelab starts the binary with following args:
 * cachebench_servicelab --json_test_config simple_test.json --benchmark_name
 BenchmarkName -bm_regex BenchmarkName$ --json --bm_json_verbose
 /tmp/tmpqs72t7ar
 *
 * The 'test_configs' directory will be in the location of this binary in the
 * fbpkg, then the input json_test_config is the relative to 'test_configs'.
 *
 * "folly::UserCounters" is used for customizable benchmark metrics.
 */

bool checkArgsValidity(const std::string& test_config) {
  if (FLAGS_benchmark_name.empty()) {
    std::cerr << "Empty benchmark name!" << std::endl;
    return false;
  }

  if (test_config.empty() ||
      !facebook::cachelib::util::pathExists(test_config)) {
    std::cerr << "Invalid config file: " << test_config
              << ". pass a valid --json_test_config for cachebench."
              << std::endl;
    return false;
  }

  return true;
}

int main(int argc, char** argv) {
  folly::init(&argc, &argv);

  auto exe = folly::fs::executable_path().parent_path();
  auto test_config = exe.string() + "/" + FLAGS_json_test_config;

  if (!checkArgsValidity(test_config)) {
    return 1;
  }

  // add folly benchmark programmingly, so that we can pass in the benchmark
  // name and config for different testings
  folly::addBenchmark(
      __FILE__, FLAGS_benchmark_name, [=](folly::UserCounters& counters) {
        CacheBenchConfig config(test_config);
        Runner runner(config);
        runner.run(counters);
        // "1" means iteration number, we always run once for cachebench
        return 1;
      });

  folly::runBenchmarks();
  return 0;
}

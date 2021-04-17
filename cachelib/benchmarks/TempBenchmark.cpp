#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include <chrono> // std::chrono::seconds
#include <thread> // std::this_thread::sleep_for

// this is a temporary benchmark test used to integrate with servicelab
// this is to be deleted once we finish the integration.
// buck run @mode/opt cachelib/benchmarks:temp

BENCHMARK(TempTest) { std::this_thread::sleep_for(std::chrono::seconds(5)); }

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}

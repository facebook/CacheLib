#include <thread>

#include <folly/Benchmark.h>
#include <folly/ThreadLocal.h>
#include <folly/init/Init.h>

DEFINE_uint64(num_threads, 32, "Number of threads to be run concurrently");
DEFINE_uint64(num_ops, 1e7, "Number of operations to be run per thread");

namespace {

std::atomic<uint64_t> val;
class DummyTag;
folly::ThreadLocal<uint64_t, DummyTag> tlVal;
} // namespace

BENCHMARK(AtomicStats) {
  std::vector<std::thread> threads;
  for (uint64_t i = 0; i < FLAGS_num_threads; i++) {
    // bind captures the variable by copy.
    threads.push_back(std::thread([&]() {
      for (uint64_t j = 0; j < FLAGS_num_ops; j++) {
        val.fetch_add(1, std::memory_order_relaxed);
      }
    }));
  }

  for (auto& thread : threads) {
    if (thread.joinable()) {
      thread.join();
    }
  }
}

BENCHMARK_RELATIVE(ThreadLocalStats) {
  std::vector<std::thread> threads;
  for (uint64_t i = 0; i < FLAGS_num_threads; i++) {
    // bind captures the variable by copy.
    threads.push_back(std::thread([&]() {
      for (uint64_t j = 0; j < FLAGS_num_ops; j++) {
        ++(*tlVal);
      }
    }));
  }

  for (auto& thread : threads) {
    if (thread.joinable()) {
      thread.join();
    }
  }
}

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}

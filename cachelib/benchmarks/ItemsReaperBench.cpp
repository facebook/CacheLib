#include <iomanip>

#include <folly/Benchmark.h>
#include <folly/Random.h>
#include <folly/init/Init.h>

#include "cachelib/allocator/CacheAllocator.h"

using namespace facebook::cachelib;

DEFINE_uint64(num_keys, 1 << 20, "Number of keys to populate the cache with");
DEFINE_uint64(hash_power,
              21,
              "Hash power controlling the number of buckets in hash table");
DEFINE_uint64(lock_power,
              10,
              "Lock power controlling number of hash table locks");
DEFINE_uint64(size_bytes,
              1024 * 10 * Slab::kSize,
              "Size of the cache in bytes");
DEFINE_uint32(reaper_interval_s,
              1,
              "time to sleep between each reaping period");
DEFINE_uint32(reaper_iterations,
              10000,
              "number of iterations per reaper period");
DEFINE_uint32(reaper_sleep_time_ms, 10, "reaper throttler sleep time");
DEFINE_uint32(reaper_sleep_interval_ms, 5, "reaper throttler sleep interval");
DEFINE_uint32(print_interval_ms,
              100,
              "interval at which to print reaper speed");
DEFINE_uint32(benchmark_duration_s, 10, "how long to run the benchmark");

namespace {

constexpr unsigned int kMaxKeySize = 255;
constexpr unsigned int kMinKeySize = 50;
constexpr unsigned int kMaxAllocSize = Slab::kSize - 500;
constexpr unsigned int kMinAllocSize = 100;
constexpr unsigned int kAllocSizeMean = 4096;
constexpr unsigned int kAllocSizeDev = 10 * 4096;

static uint32_t getRandomAllocSize(double val) {
  if (val < kMinAllocSize) {
    return kMinAllocSize;
  } else if (val > kMaxAllocSize) {
    return kMaxAllocSize;
  } else {
    return std::round(val);
  }
}

std::string generateKey(std::mt19937& gen) {
  std::uniform_int_distribution<uint32_t> keySize(kMinKeySize, kMaxKeySize);
  std::uniform_int_distribution<uint8_t> charDis(0, 25);
  const auto keyLen = keySize(gen);
  std::string s;
  for (unsigned int j = 0; j < keyLen; j++) {
    s += ('a' + charDis(gen));
  }
  return s;
}

} // namespace

int main(int argc, char** argv) {
  folly::init(&argc, &argv);

  std::cout << "initializing cache" << std::endl;
  LruAllocator::AccessConfig accessConfig(FLAGS_hash_power, FLAGS_lock_power);
  LruAllocator::Config lruConfig;
  lruConfig.setCacheSize(FLAGS_size_bytes);
  lruConfig.setAccessConfig(accessConfig);
  lruConfig.enableItemReaperInBackground(
      std::chrono::milliseconds{FLAGS_reaper_iterations},
      FLAGS_reaper_iterations,
      util::Throttler::Config{FLAGS_reaper_sleep_time_ms,
                              FLAGS_reaper_sleep_interval_ms});
  assert(lruConfig.itemsReaperEnabled());
  LruAllocator cache(lruConfig);
  const auto poolId =
      cache.addPool("default", cache.getCacheMemoryStats().cacheSize);

  std::cout << "allocating items" << std::endl;
  std::mt19937 gen(folly::Random::rand32());
  std::normal_distribution<> sizeDis(kAllocSizeMean, kAllocSizeDev);
  for (uint64_t i = 0; i < FLAGS_num_keys; ++i) {
    util::allocateAccessible(cache, poolId, generateKey(gen),
                             getRandomAllocSize(sizeDis(gen)),
                             /*ttlSecs=*/36000);
  }

  std::cout << "benchmarking" << std::endl;
  const auto start = std::chrono::steady_clock::now();
  auto lastPrint = start;
  std::chrono::steady_clock::time_point now;
  const auto initialVisited = cache.getReaperStats().numVisitedItems;
  uint64_t lastVisited = initialVisited;
  do {
    std::this_thread::sleep_for(
        std::chrono::milliseconds(FLAGS_print_interval_ms));
    const auto curVisited = cache.getReaperStats().numVisitedItems;
    now = std::chrono::steady_clock::now();
    const auto timeSpent =
        std::chrono::duration_cast<std::chrono::nanoseconds>(now - lastPrint);
    const auto visited = curVisited - lastVisited;
    std::cout << "visited " << visited << " in " << timeSpent.count() << " ns. "
              << std::fixed << std::setprecision(2)
              << visited / (timeSpent.count() * 0.000000001) << " visits/sec"
              << std::endl;
    lastVisited = curVisited;
    lastPrint = now;
  } while (now - start < std::chrono::seconds(FLAGS_benchmark_duration_s));

  const auto timeSpent =
      std::chrono::duration_cast<std::chrono::nanoseconds>(now - start);
  std::cout << "overall visit speed: " << std::fixed << std::setprecision(2)
            << (cache.getReaperStats().numVisitedItems - initialVisited) /
                   (timeSpent.count() * 0.000000001)
            << std::endl;

  return 0;
}

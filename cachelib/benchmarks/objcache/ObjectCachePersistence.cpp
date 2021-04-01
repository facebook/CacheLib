#include <folly/Benchmark.h>
#include <folly/Random.h>
#include <folly/container/EvictingCacheMap.h>
#include <folly/container/F14Map.h>

#include <map>
#include <scoped_allocator>
#include <string>
#include <unordered_map>
#include <vector>

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/benchmarks/objcache/Common.h"
#include "cachelib/benchmarks/objcache/gen-cpp2/ObjectCache_types.h"
#include "cachelib/navy/serialization/RecordIO.h"
#include "cachelib/experimental/objcache/Allocator.h"
#include "cachelib/experimental/objcache/ObjectCache.h"
#include "folly/BenchmarkUtil.h"

namespace facebook {
namespace cachelib {
namespace benchmark {
using namespace facebook::cachelib::objcache;

// const std::string kBootDriveFile =
//     "/data/users/beyondsora/persistence_testing_file";
// constexpr uint64_t kCacheSize = 1 * 1024 * 1024 * 1024ul; // 1GB
const std::string kBootDriveFile = "/root/persistence_testing_file";
constexpr uint64_t kCacheSize = 20 * 1024 * 1024 * 1024ul; // 20GB
const std::string kString = "hello I am a long lonnnnnnnnnnnnng string";

std::unique_ptr<LruObjectCache> getCache() {
  LruAllocator::Config cacheAllocatorConfig;
  cacheAllocatorConfig.setCacheSize(kCacheSize);
  cacheAllocatorConfig.setAccessConfig(LruAllocator::AccessConfig{20, 10});
  cacheAllocatorConfig.configureChainedItems(LruAllocator::AccessConfig{20, 10},
                                             10);
  cacheAllocatorConfig.setDefaultAllocSizes(1.08, 1024 * 1024, 64, false);
  LruObjectCache::Config config;
  config.setCacheAllocatorConfig(cacheAllocatorConfig);
  config.enablePersistence(
      [](folly::StringPiece /* key */, void* unalignedMem) {
        auto* obj =
            getType<StdObjectWithAlloc,
                    MonotonicBufferResource<CacheDescriptor<LruAllocator>>>(
                unalignedMem);
        return Serializer::serializeToIOBuf(*obj);
      },
      [](PoolId poolId,
         folly::StringPiece key,
         folly::StringPiece payload,
         LruObjectCache& cache) {
        Deserializer deserializer{
            reinterpret_cast<const uint8_t*>(payload.begin()),
            reinterpret_cast<const uint8_t*>(payload.end())};

        // TODO: First deserialize onto heap. Can we deserialize directly into a
        // custom allocator?
        auto tmp = deserializer.deserialize<StdObjectWithAlloc>();

        auto obj = cache.create<StdObjectWithAlloc>(poolId, key, tmp);
        if (!obj) {
          return LruObjectCache::ItemHandle{};
        }
        return std::move(obj).releaseItemHandle();
      });

  auto cache = std::make_unique<LruObjectCache>(config);
  cache->getCacheAlloc().addPool(
      "default", cache->getCacheAlloc().getCacheMemoryStats().cacheSize);
  return cache;
}

template <typename T>
void updateObject(T& obj) {
  for (int j = 0; j < 100; j++) {
    obj->m1_ref().value().insert(std::make_pair(j, j));
  }
  for (int j = 0; j < 4; j++) {
    obj->m2_ref().value().insert(std::make_pair(j, kString));
  }
  for (int j = 0; j < 4; j++) {
    // Small vector. Assume max 8 entries.
    for (int k = 0; k < 8; k++) {
      obj->m3_ref().value()[static_cast<int16_t>(j)].push_back(k);
    }
  }
  for (int j = 0; j < 2; j++) {
    obj->m4_ref().value()[static_cast<int16_t>(j)];
    for (int k = 0; k < 2; k++) {
      obj->m4_ref().value()[static_cast<int16_t>(j)].insert(
          std::make_pair(k, kString));
    }
  }
}

void fillupCache(LruObjectCache& cache) {
  std::vector<LruObjectCache::ObjectHandle<StdObjectWithAlloc>> handles;
  uint64_t i = 0;
  while (true) {
    try {
      auto hdl = cache.create<StdObjectWithAlloc>(0 /* pool id */,
                                                  folly::sformat("key_{}", i));
      updateObject(hdl);
      cache.insertOrReplace(hdl);
      handles.push_back(std::move(hdl));
      i++;
    } catch (const exception::ObjectCacheAllocationError& ex) {
      break;
    }
  }
}

void persistToMemory() {
  std::unique_ptr<LruObjectCache> cache;
  BENCHMARK_SUSPEND {
    cache = getCache();
    fillupCache(*cache);
  }

  folly::IOBufQueue queue;
  auto rw = createMemoryRecordWriter(queue);
  cache->persist(*rw);
}

void persistToTmpFile() {
  std::unique_ptr<LruObjectCache> cache;
  BENCHMARK_SUSPEND {
    cache = getCache();
    fillupCache(*cache);
  }

  auto tmpFile = folly::File::temporary();
  auto rw = navy::createFileRecordWriter(tmpFile.fd());
  cache->persist(*rw);
}

void persistToBootDrive() {
  std::unique_ptr<LruObjectCache> cache;
  folly::File file;
  BENCHMARK_SUSPEND {
    cache = getCache();
    fillupCache(*cache);

    file = folly::File(kBootDriveFile.c_str(), O_RDWR | O_CREAT | O_DIRECT);
    if (::fallocate(file.fd(), 0, 0, kCacheSize) < 0) {
      throw std::system_error(
          errno,
          std::system_category(),
          folly::sformat("failed fallocate with size {}", kCacheSize));
    }
  }
  auto rw = navy::createFileRecordWriter(file.fd());
  cache->persist(*rw);
}

void recoverFromMemory() {
  std::unique_ptr<LruObjectCache> cache;
  folly::IOBufQueue queue;
  BENCHMARK_SUSPEND {
    cache = getCache();
    fillupCache(*cache);
    auto rw = createMemoryRecordWriter(queue);
    cache->persist(*rw);

    // create a new cache
    cache = getCache();
  }

  auto rr = createMemoryRecordReader(queue);
  cache->recover(*rr);
}

void recoverFromTmpFile() {
  std::unique_ptr<LruObjectCache> cache;
  folly::IOBufQueue queue;
  folly::File tmpFile;
  BENCHMARK_SUSPEND {
    cache = getCache();
    fillupCache(*cache);
    tmpFile = folly::File::temporary();
    auto rw = navy::createFileRecordWriter(tmpFile.fd());
    cache->persist(*rw);

    // create a new cache
    cache = getCache();
  }

  auto rr = navy::createFileRecordReader(tmpFile.fd());
  cache->recover(*rr);
}

void recoverFromBootDrive() {
  std::unique_ptr<LruObjectCache> cache;
  folly::File file;
  BENCHMARK_SUSPEND {
    cache = getCache();
    fillupCache(*cache);

    file = folly::File(kBootDriveFile.c_str(), O_RDWR | O_CREAT | O_DIRECT);
    if (::fallocate(file.fd(), 0, 0, kCacheSize) < 0) {
      throw std::system_error(
          errno,
          std::system_category(),
          folly::sformat("failed fallocate with size {}", kCacheSize));
    }
    auto rw = navy::createFileRecordWriter(file.fd());
    cache->persist(*rw);

    // create a new cache
    cache = getCache();
  }

  auto rr = navy::createFileRecordReader(file.fd());
  cache->recover(*rr);
}
} // namespace benchmark
} // namespace cachelib
} // namespace facebook

using namespace facebook::cachelib::benchmark;

BENCHMARK(NOOP) {}
BENCHMARK_DRAW_LINE();

BENCHMARK(persist_to_mem) { persistToMemory(); }
BENCHMARK_RELATIVE(persist_to_tmp_file) { persistToTmpFile(); }
BENCHMARK_RELATIVE(persist_to_boot_drive) { persistToBootDrive(); }
BENCHMARK(recover_from_mem) { recoverFromMemory(); }
BENCHMARK_RELATIVE(recover_from_tmp_file) { recoverFromTmpFile(); }
BENCHMARK_RELATIVE(recover_from_boot_drive) { recoverFromBootDrive(); }

int main() { folly::runBenchmarks(); }

/*
Architecture:        x86_64
CPU op-mode(s):      32-bit, 64-bit
Byte Order:          Little Endian
CPU(s):              36
On-line CPU(s) list: 0-35
Thread(s) per core:  2
Core(s) per socket:  18
Socket(s):           1
NUMA node(s):        1
Vendor ID:           GenuineIntel
CPU family:          6
Model:               85
Model name:          Intel(R) Xeon(R) D-2191A CPU @ 1.60GHz
Stepping:            4
CPU MHz:             2382.042
CPU max MHz:         1601.0000
CPU min MHz:         800.0000
BogoMIPS:            3200.00
Virtualization:      VT-x
L1d cache:           32K
L1i cache:           32K
L2 cache:            1024K
L3 cache:            25344K
NUMA node0 CPU(s):   0-35

============================================================================
ObjectCachePersistence.cpprelative                         time/iter iters/s
============================================================================
NOOP                                                         0.00fs  Infinity
----------------------------------------------------------------------------
persist_to_mem                                               26.52s   37.70m
persist_to_tmp_file                              102.44%     25.89s   38.62m
persist_to_boot_drive                              1.12%   39.43min  422.67u
recover_from_mem                                             47.09s   21.24m
recover_from_tmp_file                             99.34%     47.40s   21.10m
recover_from_boot_drive                           38.61%    2.03min    8.20m
============================================================================
*/

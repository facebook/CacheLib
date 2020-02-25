#pragma once

#include "cachelib/cachebench/cache/Cache.h"
#include "cachelib/cachebench/util/Config.h"

namespace facebook {
namespace cachelib {
namespace cachebench {
struct ThroughputStats {
  std::string name;
  uint64_t set{0};
  uint64_t setFailure{0};
  uint64_t get{0};
  uint64_t getMiss{0};
  uint64_t del{0};
  uint64_t delNotFound{0};
  uint64_t addChained{0};
  uint64_t addChainedFailure{0};
  // current number of ops executed. Read periodically to track progress
  uint64_t ops{0};

  ThroughputStats& operator+=(const ThroughputStats& other);

  void render(uint64_t elapsedTimeNs, std::ostream& out) const;
};

class GeneratorBase;

class Stressor {
 public:
  static std::unique_ptr<Stressor> makeStressor(CacheConfig cacheConfig,
                                                StressorConfig stressorConfig);

  virtual ~Stressor() {}

  virtual Stats getCacheStats() const = 0;
  virtual ThroughputStats aggregateThroughputStats() const = 0;
  virtual std::chrono::time_point<std::chrono::system_clock> startTime()
      const = 0;
  virtual uint64_t getTestDurationNs() const = 0;

  virtual void start() = 0;
  virtual void finish() = 0;
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook

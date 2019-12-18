#pragma once

#include "cachelib/allocator/CacheStats.h"

namespace facebook {
namespace cachelib {

/**
 * A virtual interface for compact cache
 */
class ICompactCache {
 public:
  ICompactCache() {}
  virtual ~ICompactCache() {}

  // return the name of the compact cache.
  virtual std::string getName() const = 0;

  virtual PoolId getPoolId() const = 0;

  // get the size of the ccache's allocator (in bytes). Returns 0 if it is
  // disabled.
  virtual size_t getSize() const = 0;

  // get the config size of the compact cache
  virtual size_t getConfiguredSize() const = 0;

  // get the stats about the compact cache
  virtual CCacheStats getStats() const = 0;

  // resize the compact cache according to configured size
  virtual void resize() = 0;
};
} // namespace cachelib
} // namespace facebook

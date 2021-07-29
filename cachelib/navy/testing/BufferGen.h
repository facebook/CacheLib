#pragma once

#include <random>

#include "cachelib/navy/common/Buffer.h"

namespace facebook {
namespace cachelib {
namespace navy {
// Helper class that generates an arbitrarily long buffer
class BufferGen {
 public:
  // @param seed  seed value
  explicit BufferGen(uint32_t seed = 1) : rg_(seed) {}

  // @param size    size of the buffer to generate
  Buffer gen(uint32_t size);

  // Randomly generate a buffer between [sizeMin, sizeMax)
  // @param sizeMin   mininum size of the new buffer
  // @param sizeMax   maximum size of the new buffer
  Buffer gen(uint32_t sizeMin, uint32_t sizeMax);

 private:
  static const char kAlphabet[65];
  std::minstd_rand rg_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

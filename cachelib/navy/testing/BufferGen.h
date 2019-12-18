#pragma once

#include <random>

#include "cachelib/navy/common/Buffer.h"

namespace facebook {
namespace cachelib {
namespace navy {
class BufferGen {
 public:
  explicit BufferGen(uint32_t seed = 1) : rg_(seed) {}
  Buffer gen(uint32_t size);
  Buffer gen(uint32_t sizeMin, uint32_t sizeMax);

 private:
  static const char kAlphabet[65];
  std::minstd_rand rg_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

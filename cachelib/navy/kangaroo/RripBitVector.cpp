#include <cassert>
#include <iostream>

#include "cachelib/navy/common/Utils.h"
#include "cachelib/navy/kangaroo/RripBitVector.h"

namespace facebook {
namespace cachelib {
namespace navy {

namespace {
uint16_t bitMask(uint32_t bitIdx) {return 1u << bitIdx;}

bool bitSet(uint32_t& bits, uint32_t bitIdx) {bits |= bitMask(bitIdx);}
bool bitGet(uint32_t& bits, uint32_t bitIdx) {return bits & bitMask(bitIdx);}
} // namespace

RripBitVector::RripBitVector(uint32_t numVectors)
    : numVectors_{numVectors},
      bits_{std::make_unique<uint32_t[]>(numVectors_)} {
      
  // Don't have to worry about @bits_ memory:
  // make_unique initialized memory with 0
  return;
  }

void RripBitVector::set(uint32_t bucketIdx, uint32_t keyIdx) {
  XDCHECK_LT(bucketIdx, numVectors_);
  bitSet(bits_[bucketIdx], keyIdx);
}

bool RripBitVector::get(uint32_t bucketIdx, uint32_t keyIdx) {
  XDCHECK_LT(bucketIdx, numVectors_);
  if (keyIdx >= vectorSize_ * 8) {
    return 0;
  }
  return bitGet(bits_[bucketIdx], keyIdx);
}

void RripBitVector::clear(uint32_t bucketIdx) {
  XDCHECK_LT(bucketIdx, numVectors_);
  bits_[bucketIdx] = 0u;
}

} // namespace navy
} // namespace cachelib
} // namespace facebook

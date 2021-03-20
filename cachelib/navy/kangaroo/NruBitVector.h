#pragma once 

#include <cstdint>
#include <memory>

namespace facebook {
namespace cachelib {
namespace navy {
// Kangaroo uses bit vector to keep track of whether objects in a bucket
// have been hit. They are by default emptied every time the bucket is
// rewritten (ie on inserts or removes)
//
// Thread safe if user guards operations to a bucket.
class NruBitVector {
 public:
  // Creates @numVectors bitVectors, each a word (32 bits) large.
  //
  // Throws std::exception if invalid arguments.
  NruBitVector(uint32_t numVectors);

  // Not copyable
  NruBitVector(const NruBitVector&) = delete;
  NruBitVector& operator=(const NruBitVector&) = delete;
  NruBitVector(NruBitVector&&) = default;
  NruBitVector& operator=(NruBitVector&&) = default;

  // clear all bit vectors
  void reset();

  // For all operations below:
  // @bucketIdx   Index of bit vector to make op on
  // @keyIdx      Index of key within bucket
  //
  // Doesn't check bounds, like vector. Only asserts.
  void set(uint32_t bucketIdx, uint32_t keyIdx);
  bool get(uint32_t bucketIdx, uint32_t keyIdx);

  // Zeroes bit vectors
  void clear(uint32_t bucketIdx);

  uint32_t numVectors() const { return numVectors_; }

  size_t getByteSize() const { return numVectors_ * vectorSize_; }

 private:
  const uint32_t numVectors_{};
  const uint32_t vectorSize_ = 4;
  std::unique_ptr<uint32_t[]> bits_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

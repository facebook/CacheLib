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
class RripBitVector {
 public:
  // Creates @numVectors bitVectors, each can track 16 bits (ie 16 objects).
  //
  // Throws std::exception if invalid arguments.
  RripBitVector(uint32_t numVectors);

  // Not copyable
  RripBitVector(const RripBitVector&) = delete;
  RripBitVector& operator=(const RripBitVector&) = delete;
  RripBitVector(RripBitVector&&) = default;
  RripBitVector& operator=(RripBitVector&&) = default;

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
  const uint32_t vectorSize_ = 2;
  std::unique_ptr<uint32_t[]> bits_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

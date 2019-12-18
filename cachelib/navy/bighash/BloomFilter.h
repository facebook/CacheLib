#pragma once

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <vector>

#include <folly/io/RecordIO.h>
#include "cachelib/navy/serialization/RecordIO.h"

namespace facebook {
namespace cachelib {
namespace navy {
// BigHash uses bloom filters (BF) to reduce number of IO. We want to maintain
// BF for every bucket, because we want to rebuild it on every remove to keep
// its filtering properties. By default, the bloom filter is initialized to
// indicate that BigHash is empty and couldExist would return false.
//
// Think of it as an array of BF. User does BF operations referencing BF with
// an index. It solves problem of lots of small BFs: allocated one-by-one BFs
// have large overhead.
//
// Thread safe if user guards operations to a bucket.
class BloomFilter {
 public:
  // Creates @numFilters BFs. Each small BF uses @numHashes hash functions, maps
  // hash value into a table of @hashTableBitSize bits (must be power of 2).
  // Each small BF takes rounded up to byte @numHashes * @hashTableBitSize bits.
  //
  // Experiments showed that if we have 16 bytes for BF with 25 entries, then
  // optimal number of hash functions is 4 and false positive rate below 10%.
  // See details:
  // https://fb.facebook.com/groups/522950611436641/permalink/579237922474576/
  //
  // Throws std::exception if invalid arguments.
  BloomFilter(uint32_t numFilters, uint32_t numHashes, size_t hashTableBitSize);

  // Not copyable, bacause assumed to have huge memory footprint
  BloomFilter(const BloomFilter&) = delete;
  BloomFilter& operator=(const BloomFilter&) = delete;
  BloomFilter(BloomFilter&&) = default;
  BloomFilter& operator=(BloomFilter&&) = default;

  void persist(RecordWriter& rw);
  void recover(RecordReader& rw);

  // reset the whole bloom filter to default state where the init bits are set
  // and filter bits are set to return false
  void reset();

  // For all BF operations below:
  // @idx   Index of BF to make op on
  // @key   Integer key to set/test. In fact, hash of byte string.
  //
  // Doesn't check bounds, like vector. Only asserts. @set and @couldExist have
  // not effect if bucket is not initialized (@set actually sets bit, but until
  // bucket marked "initialized" @couldExist returns only true).
  void set(uint32_t idx, uint64_t key);
  bool couldExist(uint32_t idx, uint64_t key) const;

  // Zeroes BF and clears "initialized" bit.
  void clear(uint32_t idx);

  // Sets "initialized" bit. Only initialized filters tested.
  void setInitBit(uint32_t idx);
  bool getInitBit(uint32_t idx);

  uint32_t numFilters() const { return numFilters_; }

  size_t getByteSize() const { return numFilters_ * filterByteSize_; }

 private:
  uint8_t* getFilterBytes(uint32_t idx) const {
    return bits_.get() + idx * filterByteSize_;
  }

  const uint32_t numFilters_{};
  const size_t hashTableBitSize_{};
  const size_t filterByteSize_{};
  std::vector<uint64_t> seeds_;
  std::unique_ptr<uint8_t[]> bits_;
  // Bucket bloom filter initialized flags
  std::unique_ptr<uint8_t[]> init_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

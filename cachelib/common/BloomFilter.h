#pragma once

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <vector>

#include "cachelib/common/Serialization.h"
#include "cachelib/common/gen-cpp2/BloomFilter_types.h"

namespace facebook {
namespace cachelib {
// BigHash uses bloom filters (BF) to reduce number of IO. We want to maintain
// BF for every bucket, because we want to rebuild it on every remove to keep
// its filtering properties. By default, the bloom filter is initialized to
// indicate that BigHash is empty and couldExist would return false.
//
// Think of it as an array of BF. User does BF operations referencing BF with
// an index. It solves problem of lots of small BFs: allocated one-by-one BFs
// have large overhead. By default, the bloom filter is initialized to
// indicate that it is empty and couldExist would return false.
//
// Thread safe if user guards operations to an idx.
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

  template <typename SerializationProto>
  void persist(RecordWriter& rw);

  template <typename SerializationProto>
  void recover(RecordReader& rw);

  // reset the whole bloom filter to default state where the init bits are set
  // and filter bits are set to return false
  void reset();

  // For all BF operations below:
  // @idx   Index of BF to make op on
  // @key   Integer key to set/test. In fact, hash of byte string.
  //
  // Doesn't check bounds, like vector. Only asserts.
  void set(uint32_t idx, uint64_t key);
  bool couldExist(uint32_t idx, uint64_t key) const;

  // Zeroes BF for idx to indicate all elements exist.
  void clear(uint32_t idx);


  // number of unique filters
  uint32_t numFilters() const { return numFilters_; }

  size_t getByteSize() const { return numFilters_ * filterByteSize_; }

 private:
  uint8_t* getFilterBytes(uint32_t idx) const {
    return bits_.get() + idx * filterByteSize_;
  }

  void serializeBits(RecordWriter& rw, size_t fragmentSize);
  void deserializeBits(RecordReader& rr);

  static constexpr uint32_t kPersistFragmentSize = 1024 * 1024;

  const uint32_t numFilters_{};
  const size_t hashTableBitSize_{};
  const size_t filterByteSize_{};
  std::vector<uint64_t> seeds_;
  std::unique_ptr<uint8_t[]> bits_;
};

template <typename SerializationProto>
void BloomFilter::persist(RecordWriter& rw) {
  serialization::BloomFilterPersistentData bd;
  bd.numFilters = numFilters_;
  bd.hashTableBitSize = hashTableBitSize_;
  bd.filterByteSize = filterByteSize_;
  bd.fragmentSize = kPersistFragmentSize;
  bd.seeds.resize(seeds_.size());
  for (uint32_t i = 0; i < seeds_.size(); i++) {
    bd.seeds[i] = seeds_[i];
  }
  facebook::cachelib::serializeProto<serialization::BloomFilterPersistentData,
                                     SerializationProto>(bd, rw);
  serializeBits(rw, bd.fragmentSize);
}

template <typename SerializationProto>
void BloomFilter::recover(RecordReader& rr) {
  const auto bd = facebook::cachelib::deserializeProto<
      serialization::BloomFilterPersistentData,
      SerializationProto>(rr);
  if (numFilters_ != static_cast<uint32_t>(bd.numFilters) ||
      hashTableBitSize_ != static_cast<uint64_t>(bd.hashTableBitSize) ||
      filterByteSize_ != static_cast<uint64_t>(bd.filterByteSize) ||
      static_cast<uint32_t>(bd.fragmentSize) != kPersistFragmentSize) {
    throw std::invalid_argument(
        "Could not recover BloomFilter. Invalid BloomFilter.");
  }

  for (uint32_t i = 0; i < bd.seeds.size(); i++) {
    seeds_[i] = bd.seeds[i];
  }
  deserializeBits(rr);
}

} // namespace cachelib
} // namespace facebook

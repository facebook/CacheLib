/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <vector>

#include "cachelib/common/Serialization.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#include "cachelib/common/gen-cpp2/BloomFilter_types.h"
#pragma GCC diagnostic pop

namespace facebook {
namespace cachelib {
// Think of it as an array of BF. User does BF operations referencing BF with
// an index. It solves problem of lots of small BFs: allocated one-by-one BFs
// have large overhead. By default, the bloom filter is initialized to
// indicate that it is empty and couldExist would return false.
//
// Thread safe if user guards operations to an idx.
class BloomFilter {
 public:
  // empty bloom filter that always returns true
  BloomFilter() = default;

  // Creates @numFilters BFs. Each small BF uses @numHashes hash functions, maps
  // hash value into a table of @hashTableBitSize bits (must be power of 2).
  // Each small BF takes rounded up to byte @numHashes * @hashTableBitSize bits.
  //
  //
  // Throws std::exception if invalid arguments.
  BloomFilter(uint32_t numFilters, uint32_t numHashes, size_t hashTableBitSize);

  // calculates the optimal numHashes and hashTableBitSize for the fbProb and
  // creates one.
  static BloomFilter makeBloomFilter(uint32_t numFilters,
                                     size_t elementCount,
                                     double fpProb);

  // Not copyable, bacause assumed to have huge memory footprint
  BloomFilter(const BloomFilter&) = delete;
  BloomFilter& operator=(const BloomFilter&) = delete;

  // move sets the seeds to be empty and hence ensures that apis return the
  // appropriate result of an uninitialized bloom filter
  BloomFilter(BloomFilter&& other) noexcept
      : numFilters_(other.numFilters_),
        hashTableBitSize_(other.hashTableBitSize_),
        filterByteSize_(other.filterByteSize_),
        seeds_(std::exchange(other.seeds_, {})),
        bits_(std::exchange(other.bits_, nullptr)) {}

  BloomFilter& operator=(BloomFilter&& other) {
    if (this != &other) {
      this->~BloomFilter();
      new (this) BloomFilter(std::move(other));
    }
    return *this;
  }

  // For all BF operations below:
  // @idx   Index of BF to make op on
  // @key   Integer key to set/test. In fact, hash of byte string.
  //
  // Doesn't check bounds, like vector. Only asserts.
  void set(uint32_t idx, uint64_t key);
  bool couldExist(uint32_t idx, uint64_t key) const;

  // Zeroes BF for idx to indicate all elements exist.
  void clear(uint32_t idx);

  // reset the whole bloom filter to default state where the init bits are set
  // and filter bits are set to return false
  void reset();

  // number of unique filters
  uint32_t numFilters() const { return numFilters_; }

  // number of hash functions per filter
  uint32_t numHashes() const { return static_cast<uint32_t>(seeds_.size()); }

  // number of bits per filter
  size_t numBitsPerFilter() const { return filterByteSize_ * 8ULL; }

  // overall byte footprint of the array of filters.
  size_t getByteSize() const { return numFilters_ * filterByteSize_; }

  // serialize and deserialize the bloom filter into a suitable buffer and
  // serialization format. the serialization format is used for storing the
  // configuration parameters. the buffer is filled with the serialized
  // confiuguration followed by the actual bits.
  template <typename SerializationProto>
  void persist(RecordWriter& rw);

  template <typename SerializationProto>
  void recover(RecordReader& rw);

 private:
  uint8_t* getFilterBytes(uint32_t idx) const {
    XDCHECK(bits_);
    return bits_.get() + idx * filterByteSize_;
  }

  void serializeBits(RecordWriter& rw, uint64_t fragmentSize);
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
  *bd.numFilters() = numFilters_;
  *bd.hashTableBitSize() = hashTableBitSize_;
  *bd.filterByteSize() = filterByteSize_;
  *bd.fragmentSize() = kPersistFragmentSize;
  bd.seeds()->resize(seeds_.size());
  for (uint32_t i = 0; i < seeds_.size(); i++) {
    bd.seeds()[i] = seeds_[i];
  }
  facebook::cachelib::serializeProto<serialization::BloomFilterPersistentData,
                                     SerializationProto>(bd, rw);
  serializeBits(rw, *bd.fragmentSize());
}

template <typename SerializationProto>
void BloomFilter::recover(RecordReader& rr) {
  const auto bd = facebook::cachelib::deserializeProto<
      serialization::BloomFilterPersistentData,
      SerializationProto>(rr);
  if (numFilters_ != static_cast<uint32_t>(*bd.numFilters()) ||
      hashTableBitSize_ != static_cast<uint64_t>(*bd.hashTableBitSize()) ||
      filterByteSize_ != static_cast<uint64_t>(*bd.filterByteSize()) ||
      static_cast<uint32_t>(*bd.fragmentSize()) != kPersistFragmentSize) {
    throw std::invalid_argument(
        "Could not recover BloomFilter. Invalid BloomFilter.");
  }

  for (uint32_t i = 0; i < bd.seeds()->size(); i++) {
    seeds_[i] = bd.seeds()[i];
  }
  deserializeBits(rr);
}

} // namespace cachelib
} // namespace facebook

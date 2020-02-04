
#include <cassert>
#include <cstring>

#include "cachelib/navy/bighash/BloomFilter.h"
#include "cachelib/navy/common/Hash.h"
#include "cachelib/navy/common/Utils.h"
#include "cachelib/navy/serialization/Serialization.h"

namespace facebook {
namespace cachelib {
namespace navy {
namespace {
size_t byteIndex(size_t bitIdx) { return bitIdx >> 3u; }

uint8_t bitMask(size_t bitIdx) { return 1u << (bitIdx & 7u); }

// @bitSet, @gitClear and @bitGet are helper functions to test and set bit.
// @bitIndex is an arbitrary large bit index to test/set. @ptr points to the
// first byte of large bitfield.
void bitSet(uint8_t* ptr, size_t bitIdx) {
  ptr[byteIndex(bitIdx)] |= bitMask(bitIdx);
}

void bitClear(uint8_t* ptr, size_t bitIdx) {
  ptr[byteIndex(bitIdx)] &= ~bitMask(bitIdx);
}

bool bitGet(const uint8_t* ptr, size_t bitIdx) {
  return ptr[byteIndex(bitIdx)] & bitMask(bitIdx);
}

size_t bitsToBytes(size_t bits) { return powTwoAlign(bits, 8) >> 3u; }
} // namespace

constexpr uint32_t kBloomFilterPersistFragmentSize = 1024 * 1024;

BloomFilter::BloomFilter(uint32_t numFilters,
                         uint32_t numHashes,
                         size_t hashTableBitSize)
    : numFilters_{numFilters},
      hashTableBitSize_{hashTableBitSize},
      filterByteSize_{bitsToBytes(numHashes * hashTableBitSize)},
      seeds_(numHashes),
      bits_{std::make_unique<uint8_t[]>(getByteSize())},
      init_{std::make_unique<uint8_t[]>(bitsToBytes(numFilters))} {
  if (numFilters == 0 || numHashes == 0 || hashTableBitSize == 0 ||
      !folly::isPowTwo(hashTableBitSize)) {
    throw std::invalid_argument("invalid bloom filter params");
  }
  // Don't have to worry about @bits_ and @init_ memory:
  // make_unique initialized memory with 0, because it news explicitly init
  // array: new T[N]().

  for (size_t i = 0; i < seeds_.size(); i++) {
    seeds_[i] = hashInt(i);
  }
  // By default all filters are initialized
  std::memset(init_.get(), 0xff, bitsToBytes(numFilters_));
}

void BloomFilter::set(uint32_t idx, uint64_t key) {
  XDCHECK_LT(idx, numFilters_);
  auto* filterPtr = getFilterBytes(idx);
  size_t firstBit = 0;
  for (auto seed : seeds_) {
    auto bucket = combineHashes(key, seed) & (hashTableBitSize_ - 1);
    bitSet(filterPtr, firstBit + bucket);
    firstBit += hashTableBitSize_;
  }
}

bool BloomFilter::couldExist(uint32_t idx, uint64_t key) const {
  XDCHECK_LT(idx, numFilters_);
  if (!bitGet(init_.get(), idx)) {
    // Go to device if BF is not initialized. This may be recovery.
    return true;
  }
  auto* filterPtr = getFilterBytes(idx);
  size_t firstBit = 0;
  for (auto seed : seeds_) {
    auto bucket = combineHashes(key, seed) & (hashTableBitSize_ - 1);
    if (!bitGet(filterPtr, firstBit + bucket)) {
      return false;
    }
    firstBit += hashTableBitSize_;
  }
  return true;
}

void BloomFilter::clear(uint32_t idx) {
  XDCHECK_LT(idx, numFilters_);
  std::memset(getFilterBytes(idx), 0, filterByteSize_);
  bitClear(init_.get(), idx);
}

void BloomFilter::setInitBit(uint32_t idx) {
  XDCHECK_LT(idx, numFilters_);
  bitSet(init_.get(), idx);
}

bool BloomFilter::getInitBit(uint32_t idx) {
  XDCHECK_LT(idx, numFilters_);
  return bitGet(init_.get(), idx);
}

void BloomFilter::recover(RecordReader& rr) {
  auto bd = deserializeProto<serialization::BloomFilterPersistentData>(rr);
  if (numFilters_ != static_cast<uint32_t>(bd.numFilters) ||
      hashTableBitSize_ != static_cast<uint64_t>(bd.hashTableBitSize) ||
      filterByteSize_ != static_cast<uint64_t>(bd.filterByteSize) ||
      static_cast<uint32_t>(bd.fragmentSize) !=
          kBloomFilterPersistFragmentSize) {
    throw std::invalid_argument(
        "Could not recover BloomFilter. Invalid BloomFilter.");
  }

  for (uint32_t i = 0; i < bd.seeds.size(); i++) {
    seeds_[i] = bd.seeds[i];
  }
  auto bitsSize = getByteSize();
  uint64_t off = 0;
  while (off < bitsSize) {
    auto bitsBuf = rr.readRecord();
    if (!bitsBuf) {
      throw std::invalid_argument(
          folly::sformat("Failed to recover bits_ off: {}", off));
    }
    memcpy(bits_.get() + off, bitsBuf->data(), bitsBuf->length());
    off += bitsBuf->length();
  }

  auto initSize = bitsToBytes(numFilters_);
  off = 0;
  while (off < initSize) {
    auto initBuf = rr.readRecord();
    if (!initBuf) {
      throw std::invalid_argument(
          folly::sformat("Failed to recover init_ off: {}", off));
    }
    memcpy(init_.get() + off, initBuf->data(), initBuf->length());
    off += initBuf->length();
  }
  XLOG(INFO, "Recovered bloom filter");
}

void BloomFilter::persist(RecordWriter& rw) {
  serialization::BloomFilterPersistentData bd;
  bd.numFilters = numFilters_;
  bd.hashTableBitSize = hashTableBitSize_;
  bd.filterByteSize = filterByteSize_;
  bd.fragmentSize = kBloomFilterPersistFragmentSize;
  bd.seeds.resize(seeds_.size());
  for (uint32_t i = 0; i < seeds_.size(); i++) {
    bd.seeds[i] = seeds_[i];
  }
  serializeProto(bd, rw);
  uint64_t fragmentSize = bd.fragmentSize;
  uint64_t bitsSize = getByteSize();
  uint64_t off = 0;
  while (off < bitsSize) {
    Buffer buffer{kBloomFilterPersistFragmentSize, kBlockSize};
    auto nBytes = std::min(bitsSize - off, fragmentSize);
    memcpy(buffer.data(), bits_.get() + off, nBytes);
    auto wbuf = folly::IOBuf::takeOwnership(buffer.release(), nBytes);
    rw.writeRecord(std::move(wbuf));
    off += nBytes;
  }

  auto initSize = bitsToBytes(numFilters_);
  off = 0;
  while (off < initSize) {
    auto nBytes = std::min(initSize - off, fragmentSize);
    Buffer buffer{kBloomFilterPersistFragmentSize, kBlockSize};
    memcpy(buffer.data(), init_.get() + off, nBytes);
    auto wbuf = folly::IOBuf::takeOwnership(buffer.release(), nBytes);
    rw.writeRecord(std::move(wbuf));
    off += nBytes;
  }
  XLOG(INFO, "bloom filter persist done");
}

void BloomFilter::reset() {
  // make the bits indicate that no keys are set
  std::memset(bits_.get(), 0, getByteSize());

  // set all buckets to init state
  std::memset(init_.get(), 0xff, bitsToBytes(numFilters_));
}

} // namespace navy
} // namespace cachelib
} // namespace facebook

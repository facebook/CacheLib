#include <cassert>
#include <cstring>

#include "cachelib/common/BloomFilter.h"
#include "cachelib/common/Hash.h"

namespace facebook {
namespace cachelib {
namespace {
size_t byteIndex(size_t bitIdx) { return bitIdx >> 3u; }

uint8_t bitMask(size_t bitIdx) { return 1u << (bitIdx & 7u); }

// @bitSet, @bitGet are helper functions to test and set bit.
// @bitIndex is an arbitrary large bit index to test/set. @ptr points to the
// first byte of large bitfield.
void bitSet(uint8_t* ptr, size_t bitIdx) {
  ptr[byteIndex(bitIdx)] |= bitMask(bitIdx);
}

bool bitGet(const uint8_t* ptr, size_t bitIdx) {
  return ptr[byteIndex(bitIdx)] & bitMask(bitIdx);
}

size_t bitsToBytes(size_t bits) {
  // align to closest 8 byte
  return ((bits + 7ULL) & ~(7ULL)) >> 3u;
}
} // namespace

constexpr uint32_t BloomFilter::kPersistFragmentSize;

BloomFilter::BloomFilter(uint32_t numFilters,
                         uint32_t numHashes,
                         size_t hashTableBitSize)
    : numFilters_{numFilters},
      hashTableBitSize_{hashTableBitSize},
      filterByteSize_{bitsToBytes(numHashes * hashTableBitSize)},
      seeds_(numHashes),
      bits_{std::make_unique<uint8_t[]>(getByteSize())} {
  if (numFilters == 0 || numHashes == 0 || hashTableBitSize == 0) {
    throw std::invalid_argument("invalid bloom filter params");
  }
  // Don't have to worry about @bits_ memory:
  // make_unique initialized memory with 0, because it news explicitly init
  // array: new T[N]().
  for (size_t i = 0; i < seeds_.size(); i++) {
    seeds_[i] = facebook::cachelib::hashInt(i);
  }
}

void BloomFilter::set(uint32_t idx, uint64_t key) {
  XDCHECK_LT(idx, numFilters_);
  auto* filterPtr = getFilterBytes(idx);
  size_t firstBit = 0;
  for (auto seed : seeds_) {
    auto bitNum =
        facebook::cachelib::combineHashes(key, seed) % hashTableBitSize_;
    bitSet(filterPtr, firstBit + bitNum);
    firstBit += hashTableBitSize_;
  }
}

bool BloomFilter::couldExist(uint32_t idx, uint64_t key) const {
  XDCHECK_LT(idx, numFilters_);
  auto* filterPtr = getFilterBytes(idx);
  size_t firstBit = 0;
  for (auto seed : seeds_) {
    auto bitNum =
        facebook::cachelib::combineHashes(key, seed) % hashTableBitSize_;
    if (!bitGet(filterPtr, firstBit + bitNum)) {
      return false;
    }
    firstBit += hashTableBitSize_;
  }
  return true;
}

void BloomFilter::clear(uint32_t idx) {
  XDCHECK_LT(idx, numFilters_);
  std::memset(getFilterBytes(idx), 0, filterByteSize_);
}

void BloomFilter::reset() {
  // make the bits indicate that no keys are set
  std::memset(bits_.get(), 0, getByteSize());
}

void BloomFilter::serializeBits(RecordWriter& rw, size_t fragmentSize) {
  uint64_t bitsSize = getByteSize();
  uint64_t off = 0;
  while (off < bitsSize) {
    auto nBytes = std::min(bitsSize - off, fragmentSize);
    auto wbuf = folly::IOBuf::copyBuffer(bits_.get() + off, nBytes);
    rw.writeRecord(std::move(wbuf));
    off += nBytes;
  }

  // we usued to have init bits per filter that indicates if the bloom filter
  // is in uninitialized state for idx. now we fake it to maintain backwards
  // compatibility.
  auto initSize = bitsToBytes(numFilters_);
  auto fakeInitBits = std::make_unique<uint8_t[]>(fragmentSize);
  off = 0;
  while (off < initSize) {
    auto nBytes = std::min(initSize - off, fragmentSize);
    auto wbuf = folly::IOBuf::copyBuffer(fakeInitBits.get() + off, nBytes);
    rw.writeRecord(std::move(wbuf));
    off += nBytes;
  }
}

void BloomFilter::deserializeBits(RecordReader& rr) {
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

  // for backward compatibility reasons, we still handle the recover as though
  // we have init bits. In reality we don't. This should be cleaned up
  // appropriately once it is safe to not be backwards compatible without
  // dropping the cache.
  auto initSize = bitsToBytes(numFilters_);
  off = 0;
  while (off < initSize) {
    auto initBuf = rr.readRecord();
    if (!initBuf) {
      throw std::invalid_argument(
          folly::sformat("Failed to recover init_ off: {}", off));
    }
    off += initBuf->length();
  }
}

} // namespace cachelib
} // namespace facebook

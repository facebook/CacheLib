#pragma once

#include <memory>
#include <stdexcept>

namespace facebook {
namespace cachelib {
namespace util {

// A probabilistic counting data structure that never undercounts items. It is
// a table structure with the depth being the number of hashes and the width
// being the number of unique items. When a key is inserted, each row's hash
// function is used to generate the index for that row. Then the element's
// count at that index is incremented. As a result, one key being inserted will
// increment different indicies in each row. Querying the count returns the
// minimum values of these elements since some hashes might collide.
//
// Users are supposed to synchronize concurrent accesses to the data
// structure.
//
// E.g. insert(1)
// hash1(1) = 2 -> increment row 1, index 2
// hash2(1) = 5 -> increment row 2, index 5
// hash3(1) = 3 -> increment row 3, index 3
// etc.
class CountMinSketch {
 public:
  // @param errors        Tolerable error in count given as a fraction of the
  //                      total number of inserts. Must be between 0 and 1.
  // @param probability   The certainty that the count is within the
  //                      error threshold. Must be between 0 and 1.
  // @param maxWidth      Maximum number of elements per row in the table.
  // @param maxDepth      Maximum number of rows.
  // Throws std::exception.
  CountMinSketch(double error,
                 double probability,
                 uint32_t maxWidth,
                 uint32_t maxDepth);

  CountMinSketch(uint32_t width, uint32_t depth);
  CountMinSketch() = default;

  CountMinSketch(const CountMinSketch&) = delete;
  CountMinSketch& operator=(const CountMinSketch&) = delete;

  CountMinSketch(CountMinSketch&& other) noexcept
      : width_(other.width_),
        depth_(other.depth_),
        table_(std::move(other.table_)) {
    other.width_ = 0;
    other.depth_ = 0;
  }

  CountMinSketch& operator=(CountMinSketch&& other) {
    if (this != &other) {
      this->~CountMinSketch();
      new (this) CountMinSketch(std::move(other));
    }
    return *this;
  }

  uint32_t getCount(uint64_t key) const;
  void increment(uint64_t key);
  void resetCount(uint64_t key);

  // decays all counts by the given decay rate. count *= decay
  void decayCountsBy(double decay);

  // Sets count for all keys to zero
  void reset() { decayCountsBy(0.0); }

  uint32_t width() const { return width_; }

  uint32_t depth() const { return depth_; }

  uint64_t getByteSize() const {
    // Each elem in @table_ has 4 bytes
    return width_ * depth_ * 4;
  }

 private:
  static uint32_t calculateWidth(double error, uint32_t maxWidth);
  static uint32_t calculateDepth(double probability, uint32_t maxDepth);

  // Get the index for @hashNumber row in the table
  uint64_t getIndex(uint32_t hashNumber, uint64_t key) const;

  uint32_t width_{0};
  uint32_t depth_{0};

  // Stores counts
  std::unique_ptr<uint32_t[]> table_{};
};
} // namespace util
} // namespace cachelib
} // namespace facebook

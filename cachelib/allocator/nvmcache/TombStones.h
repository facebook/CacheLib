#pragma once
#include <mutex>
#include <unordered_map>

#include <folly/lang/Align.h>
#include <glog/logging.h>

namespace facebook {
namespace cachelib {

// Utility that helps us track in flight deletes by hashing the key. There
// can be multiple concurrent deletes for the same key in flight and also
// possibility of keys colliding on 64 bit hash. To work with this, we
// maintain a count per hash and check for presence against the count.
class alignas(folly::hardware_destructive_interference_size) TombStones {
 public:
  class Guard;

  // adds an instance of  key
  // @param key  key for the record
  // @return a valid Guard representing the tombstone
  Guard add(uint64_t key) {
    std::lock_guard<std::mutex> l(mutex_);
    ++keys_[key];
    return Guard(key, *this);
  }

  // checks if there is a key present and returns true if so.
  bool isPresent(uint64_t key) {
    std::lock_guard<std::mutex> l(mutex_);
    return keys_.count(key) != 0;
  }

  // Guard that wraps around the tombstone record. Removes the key from the
  // tombstone records upon destruction. A valid guard can be only created by
  // adding to the tombstone record.
  class Guard {
   public:
    Guard() {}
    ~Guard() {
      if (tombstones_) {
        tombstones_->remove(hash_);
        tombstones_ = nullptr;
      }
    }

    // disable copying
    Guard(const Guard&) = delete;
    Guard& operator=(const Guard&&) = delete;

    // allow moving
    Guard(Guard&& other) noexcept
        : hash_{other.hash_}, tombstones_(other.tombstones_) {
      other.tombstones_ = nullptr;
    }
    Guard& operator=(Guard&& other) noexcept {
      if (this != &other) {
        this->~Guard();
        new (this) Guard(std::move(other));
      }
      return *this;
    }

   private:
    // only tombstone can create a guard.
    friend TombStones;
    Guard(uint64_t hash, TombStones& t) noexcept
        : hash_(hash), tombstones_(&t) {}

    // key for the tombstone
    const uint64_t hash_{0};

    // tombstone record
    TombStones* tombstones_{nullptr};
  };

 private:
  // removes an instance of key. if the count drops to 0, we remove the key
  void remove(uint64_t key) {
    std::lock_guard<std::mutex> l(mutex_);
    auto it = keys_.find(key);
    if (it == keys_.end() || it->second == 0) {
      // this is not supposed to happen if guards are destroyed appropriately
      throw std::runtime_error("Invalid state");
    }

    if (--(it->second) == 0) {
      keys_.erase(it);
    }
  }

  // mutex protecting the map below
  std::mutex mutex_;
  std::unordered_map<uint64_t, uint64_t> keys_;
};


} // namespace cachelib
} // namespace facebook

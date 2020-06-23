#include <folly/Format.h>

#include "cachelib/navy/block_cache/Index.h"
#include "cachelib/navy/serialization/Serialization.h"

namespace facebook {
namespace cachelib {
namespace navy {
constexpr uint32_t Index::kNumBuckets; // Link error otherwise

Index::LookupResult Index::lookup(uint64_t key) const {
  LookupResult lr;
  auto b = bucket(key);
  auto k = subkey(key);
  const auto& map = buckets_[b];

  std::shared_lock<folly::SharedMutex> lock{getMutex(b)};
  auto it = map.find(k);
  if (it != map.end()) {
    lr.found_ = true;
    lr.record_ = it->second;
  }
  return lr;
}

Index::LookupResult Index::insert(uint64_t key, uint32_t address) {
  LookupResult lr;
  auto b = bucket(key);
  auto k = subkey(key);
  auto& map = buckets_[b];

  std::lock_guard<folly::SharedMutex> lock{getMutex(b)};
  auto it = map.find(k);
  if (it != map.end()) {
    lr.found_ = true;
    lr.record_ = it->second;
    // tsl::sparse_map's `it->second` is immutable, while it.value() is mutable
    it.value().address = address;
  } else {
    map.try_emplace(key, address);
  }
  return lr;
}

bool Index::replaceIfMatch(uint64_t key,
                           uint32_t newAddress,
                           uint32_t oldAddress) {
  auto b = bucket(key);
  auto k = subkey(key);
  auto& map = buckets_[b];

  std::lock_guard<folly::SharedMutex> lock{getMutex(b)};
  auto it = map.find(k);
  if (it != map.end() && it->second.address == oldAddress) {
    // tsl::sparse_map's `it->second` is immutable, while it.value() is mutable
    it.value().address = newAddress;
    return true;
  }
  return false;
}

Index::LookupResult Index::remove(uint64_t key) {
  LookupResult lr;
  auto b = bucket(key);
  auto k = subkey(key);
  auto& map = buckets_[b];

  std::lock_guard<folly::SharedMutex> lock{getMutex(b)};
  auto it = map.find(k);
  if (it != map.end()) {
    lr.found_ = true;
    lr.record_ = it->second;
    map.erase(it);
  }
  return lr;
}

bool Index::removeIfMatch(uint64_t key, uint32_t address) {
  auto b = bucket(key);
  auto k = subkey(key);
  auto& map = buckets_[b];

  std::lock_guard<folly::SharedMutex> lock{getMutex(b)};
  auto it = map.find(k);
  if (it != map.end() && it->second.address == address) {
    map.erase(it);
    return true;
  }
  return false;
}

void Index::reset() {
  for (uint32_t i = 0; i < kNumBuckets; i++) {
    std::lock_guard<folly::SharedMutex> lock{getMutex(i)};
    buckets_[i].clear();
  }
}

size_t Index::computeSize() const {
  size_t size = 0;
  for (uint32_t i = 0; i < kNumBuckets; i++) {
    std::shared_lock<folly::SharedMutex> lock{getMutex(i)};
    size += buckets_[i].size();
  }
  return size;
}

// Todo (aw7): check if other fields needs persist and recover
void Index::persist(RecordWriter& rw) const {
  serialization::IndexBucket bucket;
  for (uint32_t i = 0; i < kNumBuckets; i++) {
    bucket.bucketId = i;
    // Convert index entries to thrift objects
    for (const auto& [key, value] : buckets_[i]) {
      serialization::IndexEntry entry;
      entry.key = key;
      entry.value = value.address;
      bucket.entries.push_back(entry);
    }
    // Serialize bucket then clear contents to reuse memory.
    serializeProto(bucket, rw);
    bucket.entries.clear();
  }
}

void Index::recover(RecordReader& rr) {
  for (uint32_t i = 0; i < kNumBuckets; i++) {
    auto bucket = deserializeProto<serialization::IndexBucket>(rr);
    uint32_t id = bucket.bucketId;
    if (id >= kNumBuckets) {
      throw std::invalid_argument{
          folly::sformat("Invalid bucket id. Max buckets: {}, bucket id: {}",
                         kNumBuckets,
                         id)};
    }
    for (auto& entry : bucket.entries) {
      buckets_[id].try_emplace(entry.key, entry.value);
    }
  }
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

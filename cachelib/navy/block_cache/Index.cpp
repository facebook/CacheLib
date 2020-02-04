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
  std::shared_lock<folly::SharedMutex> lock{getMutex(b)};
  if (buckets_[b].lookup(subkey(key), lr.value_)) {
    lr.found_ = true;
  }
  return lr;
}

Index::LookupResult Index::insert(uint64_t key, uint32_t value) {
  LookupResult lr;
  auto b = bucket(key);
  std::lock_guard<folly::SharedMutex> lock{getMutex(b)};
  auto res = buckets_[b].insert(subkey(key), value);
  if (res) {
    lr.found_ = true;
    lr.value_ = *res;
  }
  return lr;
}

bool Index::replace(uint64_t key, uint32_t newValue, uint32_t oldValue) {
  auto b = bucket(key);
  std::lock_guard<folly::SharedMutex> lock{getMutex(b)};
  uint32_t outValue = 0;
  if (!buckets_[b].lookup(subkey(key), outValue)) {
    return false;
  }

  if (outValue != oldValue) {
    return false;
  }

  buckets_[b].insert(subkey(key), newValue);
  return true;
}

Index::LookupResult Index::remove(uint64_t key) {
  LookupResult lr;
  auto b = bucket(key);
  std::lock_guard<folly::SharedMutex> lock{getMutex(b)};
  auto removed = buckets_[b].remove(subkey(key));
  if (removed) {
    lr.found_ = true;
    lr.value_ = removed.value();
  } else {
    lr.found_ = false;
    lr.value_ = 0;
  }
  return lr;
}

bool Index::remove(uint64_t key, uint32_t value) {
  auto b = bucket(key);
  std::lock_guard<folly::SharedMutex> lock{getMutex(b)};
  uint32_t outValue = 0;
  if (!buckets_[b].lookup(subkey(key), outValue)) {
    return false;
  }

  if (outValue != value) {
    return false;
  }

  buckets_[b].remove(subkey(key));
  return true;
}

void Index::reset() {
  for (uint32_t i = 0; i < kNumBuckets; i++) {
    std::lock_guard<folly::SharedMutex> lock{getMutex(i)};
    buckets_[i].destroy();
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

void Index::persist(RecordWriter& rw) const {
  serialization::IndexBucket bucket;
  for (uint32_t i = 0; i < kNumBuckets; i++) {
    bucket.bucketId = i;
    // Convert index entries to thrift objects
    buckets_[i].traverse([&bucket](uint32_t key, uint32_t value) {
      serialization::IndexEntry entry;
      entry.key = key;
      entry.value = value;
      bucket.entries.push_back(entry);
    });
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
      buckets_[id].insert(entry.key, entry.value);
    }
  }
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

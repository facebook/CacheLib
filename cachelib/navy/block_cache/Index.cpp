#include <folly/Format.h>

#include "cachelib/navy/block_cache/Index.h"
#include "cachelib/navy/serialization/Serialization.h"

namespace facebook {
namespace cachelib {
namespace navy {
constexpr uint32_t Index::kNumBuckets; // Link error otherwise

namespace {
// increase val if no overflow, otherwise do nothing
uint8_t safeInc(uint8_t val) {
  if (val < std::numeric_limits<uint8_t>::max()) {
    return val + 1;
  }
  return val;
}
} // namespace

void Index::setHits(uint64_t key, uint8_t currentHits, uint8_t totalHits) {
  auto& map = getMap(key);
  auto lock = std::lock_guard{getMutex(key)};

  auto it = map.find(subkey(key));
  if (it != map.end()) {
    it.value().currentHits = currentHits;
    it.value().totalHits = totalHits;
  }
}

Index::LookupResult Index::lookup(uint64_t key) {
  LookupResult lr;
  auto& map = getMap(key);
  auto lock = std::lock_guard{getMutex(key)};

  auto it = map.find(subkey(key));
  if (it != map.end()) {
    lr.found_ = true;
    lr.record_ = it->second;
    it.value().totalHits = safeInc(lr.record_.totalHits);
    it.value().currentHits = safeInc(lr.record_.currentHits);
  }
  return lr;
}

Index::LookupResult Index::peek(uint64_t key) const {
  LookupResult lr;
  const auto& map = getMap(key);
  auto lock = std::shared_lock{getMutex(key)};

  auto it = map.find(subkey(key));
  if (it != map.end()) {
    lr.found_ = true;
    lr.record_ = it->second;
  }
  return lr;
}

Index::LookupResult Index::insert(uint64_t key, uint32_t address) {
  LookupResult lr;
  auto& map = getMap(key);
  auto lock = std::lock_guard{getMutex(key)};

  auto it = map.find(subkey(key));
  if (it != map.end()) {
    lr.found_ = true;
    lr.record_ = it->second;
    trackRemove(it->second.totalHits);
    // tsl::sparse_map's `it->second` is immutable, while it.value() is mutable
    it.value().address = address;
    it.value().currentHits = 0;
    it.value().totalHits = 0;
  } else {
    map.try_emplace(key, address);
  }
  return lr;
}

bool Index::replaceIfMatch(uint64_t key,
                           uint32_t newAddress,
                           uint32_t oldAddress) {
  auto& map = getMap(key);
  auto lock = std::lock_guard{getMutex(key)};

  auto it = map.find(subkey(key));
  if (it != map.end() && it->second.address == oldAddress) {
    // tsl::sparse_map's `it->second` is immutable, while it.value() is mutable
    it.value().address = newAddress;
    it.value().currentHits = 0;
    return true;
  }
  return false;
}

void Index::trackRemove(uint8_t totalHits) {
  hitsEstimator_.trackValue(totalHits);
  if (totalHits == 0) {
    unAccessedItems_.inc();
  }
}

Index::LookupResult Index::remove(uint64_t key) {
  LookupResult lr;
  auto& map = getMap(key);
  auto lock = std::lock_guard{getMutex(key)};

  auto it = map.find(subkey(key));
  if (it != map.end()) {
    lr.found_ = true;
    lr.record_ = it->second;

    trackRemove(it->second.totalHits);
    map.erase(it);
  }
  return lr;
}

bool Index::removeIfMatch(uint64_t key, uint32_t address) {
  auto& map = getMap(key);
  auto lock = std::lock_guard{getMutex(key)};

  auto it = map.find(subkey(key));
  if (it != map.end() && it->second.address == address) {
    trackRemove(it->second.totalHits);
    map.erase(it);
    return true;
  }
  return false;
}

void Index::reset() {
  for (uint32_t i = 0; i < kNumBuckets; i++) {
    auto lock = std::lock_guard{getMutexOfBucket(i)};
    buckets_[i].clear();
  }
}

size_t Index::computeSize() const {
  size_t size = 0;
  for (uint32_t i = 0; i < kNumBuckets; i++) {
    auto lock = std::lock_guard{getMutexOfBucket(i)};
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
    for (const auto& [key, record] : buckets_[i]) {
      serialization::IndexEntry entry;
      entry.key_ref() = key;
      entry.address_ref() = record.address;
      entry.size_ref() = record.size;
      entry.totalHits_ref() = record.totalHits;
      entry.currentHits_ref() = record.currentHits;
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
      buckets_[id].try_emplace(*entry.key_ref(),
                               *entry.address_ref(),
                               *entry.size_ref(),
                               *entry.totalHits_ref(),
                               *entry.currentHits_ref());
    }
  }
}

void Index::getCounters(const CounterVisitor& visitor) const {
  hitsEstimator_.visitQuantileEstimator(visitor, "navy_bc_item_{}_{}", "hits");
  visitor("navy_bc_item_removed_with_no_access", unAccessedItems_.get());
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

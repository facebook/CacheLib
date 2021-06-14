// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#pragma once
#include <cstdint>

#include "cachelib/allocator/Cache.h"
#include "cachelib/cachebench/cache/CacheValue.h"

namespace facebook::cachelib::cachebench {
/*
 * ItemRecord and ItemRecords are used for DestructorCheck in cachebench.
 * Every new allocated item has an ItemRecord pushed into ItemRecords, the
 * index of the ItemRecord will be stored with the item.
 * No matter how the state of the item is changed in cache, in RAM, NVM,
 * evicted, or removed, the record will be kept in ItemRecords until the end of
 * test.
 * The version field is the latest version of the item (via in-palce or
 * chianed mutation). This ensures that the destructor is only triggers for
 * the latest version of the item.
 * The destructCount field is incremented in the ItemDestructor, it ensures that
 * each allocation triggers destructor once and only once.
 */
struct ItemRecord {
  uint32_t destructCount{0};
  uint32_t version{0};
  // for debug purpose, if destructor is triggered more than once
  // what is the context for previous removal.
  RemoveContext context;
};

template <typename Allocator>
class ItemRecords {
  using Item = typename Allocator::Item;
  using ItemHandle = typename Allocator::ItemHandle;
  using RemoveCbData = typename Allocator::RemoveCbData;

 public:
  explicit ItemRecords(bool enable,
                       uint64_t threads = std::thread::hardware_concurrency())
      : enable_(enable), itemRecords_(threads), mutexes_(threads) {}

  bool validate(const RemoveCbData& data) {
    if (!enable_) {
      return true;
    }

    auto& item = data.item;
    auto ptr = item.template getWritableMemoryAs<CacheValue>();

    auto [lock, records] = getItemRecords(ptr->getIdx());
    auto& record = records[ptr->getIdx() / itemRecords_.size()];

    bool result = true;
    if (record.destructCount != 0 || record.version != ptr->getVersion()) {
      // item destructor should be called only once
      // and the value must be the latest version
      result = false;
    } else {
      // update context only for valid destructor
      record.context = data.context;
    }
    if (record.destructCount != 0) {
      XLOGF(ERR, "unexpected destructCount {} for item {}, context {}|{}",
            record.destructCount, item.getKey(), data.context, record.context);
    }
    if (record.version != ptr->getVersion()) {
      XLOGF(ERR, "unexpected version {}|{} for item {}", record.version,
            ptr->getVersion(), item.getKey());
    }
    ++record.destructCount;

    return result;
  }

  void addItemRecord(const ItemHandle& handle) {
    if (!enable_ || !handle) {
      return;
    }
    {
      std::lock_guard<std::mutex> l(keysMutex_);
      keys_.insert(handle->getKey().toString());
    }
    auto ptr = handle->template getWritableMemoryAs<CacheValue>();
    auto idx = indexes_++;
    ptr->setIdx(idx);
    ptr->setVersion(0);
    auto [lock, records] = getItemRecords(idx);
    records.resize(std::max(idx / itemRecords_.size() + 1, records.size()));
  }

  size_t count() const {
    size_t count = 0;
    for (uint64_t i = 0; i < itemRecords_.size(); ++i) {
      std::lock_guard<std::mutex> l(mutexes_[i]);
      count += itemRecords_[i].size();
    }
    return count;
  }

  auto getKeys() {
    std::lock_guard<std::mutex> l(keysMutex_);
    return std::move(keys_);
  }

 private:
  std::tuple<std::unique_lock<std::mutex>, std::vector<ItemRecord>&>
  getItemRecords(uint64_t idx) {
    return {std::unique_lock<std::mutex>(mutexes_[idx % itemRecords_.size()]),
            itemRecords_[idx % itemRecords_.size()]};
  }

  bool enable_;
  std::atomic<uint64_t> indexes_{0};
  std::vector<std::vector<ItemRecord>> itemRecords_;
  mutable std::vector<std::mutex> mutexes_;
  std::unordered_set<std::string> keys_;
  mutable std::mutex keysMutex_;
};

} // namespace facebook::cachelib::cachebench

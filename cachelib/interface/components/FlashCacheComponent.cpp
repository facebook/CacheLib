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

#include "cachelib/interface/components/FlashCacheComponent.h"

#include "cachelib/common/Time.h"

using namespace facebook::cachelib::navy;

namespace facebook::cachelib::interface {

/**
 * In-memory buffer for data cached in flash. Reference counting via
 * RegionDescriptors is used to ensure the backing Region is still resident &
 * valid while working on this cache item. The buffer's layout is:
 *
 *   ---------------------------------------------------------------------------
 *   | creation & expiry time ||  value  || (empty) |  key  | entry descriptor |
 *   ---------------------------------------------------------------------------
 */
class FlashCacheItem : public CacheItem {
 public:
  using EntryDesc = BlockCache::EntryDesc;

  static constexpr size_t kMetadataSize = 2 * sizeof(uint32_t);

  // Allocation constructor
  // NOTE: valueSize *must* include space for creation & expiration time
  FlashCacheItem(const HashedKey& hashedKey,
                 uint32_t valueSize,
                 std::tuple<RegionDescriptor, size_t, RelAddress>&& allocation,
                 const uint32_t creationTime,
                 const uint32_t ttlSecs)
      : desc_(std::move(std::get<0>(allocation))),
        relAddr_(std::move(std::get<2>(allocation))),
        buffer_(std::get<1>(allocation)) {
    XDCHECK_GE(valueSize, 2 * sizeof(uint32_t))
        << "valueSize doesn't include space for metadata";
    BlockCache::writeEntryDescAndKey(buffer_, hashedKey, valueSize);
    uint32_t* metadata = reinterpret_cast<uint32_t*>(buffer_.data());
    metadata[0] = creationTime;
    metadata[1] = ttlSecs > 0 ? creationTime + ttlSecs : 0;
  }

  // Find constructor
  explicit FlashCacheItem(BlockCache::LookupData&& ld)
      : desc_(std::move(ld.desc_)),
        // Lookup returns the end address, update the offset to beginning
        relAddr_(ld.addrEnd_.sub(ld.buffer_.size())),
        buffer_(std::move(ld.buffer_)) {}

  RegionDescriptor& getRegionDescriptor() noexcept { return desc_; }
  RelAddress& getRelAddress() noexcept { return relAddr_; }
  Buffer& getBuffer() noexcept { return buffer_; }
  EntryDesc* getEntryDescriptor() const noexcept {
    auto entryEnd = const_cast<uint8_t*>(buffer_.data()) + buffer_.size();
    return reinterpret_cast<EntryDesc*>(entryEnd - sizeof(EntryDesc));
  }

  // ------------------------------ Interface ------------------------------ //

  uint32_t getCreationTime() const noexcept override {
    return reinterpret_cast<const uint32_t*>(buffer_.data())[0];
  }
  uint32_t getExpiryTime() const noexcept override {
    return reinterpret_cast<const uint32_t*>(buffer_.data())[1];
  }
  // Ref-counting for FlashCache is different than RAMCache - multiple
  // RAMCacheItems all refer to a single cache item in memory whereas each
  // FlashCacheItem is separate (even if it references the same cache item in
  // Region memory).
  //
  // The only thing we care about is Region refcounting via RegionDescriptor. We
  // don't need to increment on create (creating the RegionDescriptor does that)
  // and we should always release the cache item to release the descriptor.
  void incrementRefCount() noexcept override {}
  bool decrementRefCount() noexcept override {
    // get cache to release the descriptor (need access to RegionManager)
    return true;
  }
  Key getKey() const noexcept override {
    auto* desc = getEntryDescriptor();
    return Key{reinterpret_cast<const char*>(desc) - desc->keySize,
               desc->keySize};
  }
  void* getMemory() const noexcept override {
    return const_cast<uint8_t*>(buffer_.data() + kMetadataSize);
  }
  uint32_t getMemorySize() const noexcept override {
    return getEntryDescriptor()->valueSize - kMetadataSize;
  }
  uint32_t getTotalSize() const noexcept override { return buffer_.size(); }

 private:
  // Used for region lifecycle management & indexing
  RegionDescriptor desc_;
  RelAddress relAddr_;
  // Data is buffered in RAM
  Buffer buffer_;
};

/* static */ Result<FlashCacheComponent> FlashCacheComponent::create(
    std::string name, navy::BlockCache::Config&& config) noexcept {
  try {
    return FlashCacheComponent(std::move(name), std::move(config));
  } catch (const std::invalid_argument& ia) {
    return makeError(Error::Code::INVALID_CONFIG, ia.what());
  }
}

const std::string& FlashCacheComponent::getName() const noexcept {
  return name_;
}

folly::coro::Task<Result<AllocatedHandle>> FlashCacheComponent::allocate(
    Key key, uint32_t size, uint32_t creationTime, uint32_t ttlSecs) {
  if (key.empty()) {
    co_return makeError(Error::Code::INVALID_ARGUMENTS, "empty key");
  }

  HashedKey hashedKey(key);
  uint32_t valueSize = size + FlashCacheItem::kMetadataSize;
  auto ret = co_await onWorkerThread(
      [=, this]() { return cache_->allocateForInsert(hashedKey, valueSize); },
      [this](auto&& result) {
        if (result.hasValue()) {
          // Successfully allocated Region memory but coroutine was cancelled -
          // close the descriptor to release refcount on the Region
          auto& [desc, slotSize, _] = result.value();
          cache_->addHole(slotSize);
          cache_->regionManager_.close(std::move(desc));
        }
      });
  if (ret.hasError()) {
    co_return makeError(Error::Code::ALLOCATE_FAILED,
                        "could not allocate space in a region");
  }
  // NOTE: we can't checksum until the user has finished writing their data,
  // defer until they call insert()
  auto* item = new FlashCacheItem(hashedKey, valueSize, std::move(ret).value(),
                                  creationTime, ttlSecs);
  co_return AllocatedHandle(*this, *item);
}

folly::coro::Task<UnitResult> FlashCacheComponent::insertImpl(
    AllocatedHandle&& handle, bool allowReplace) {
  if (!handle) {
    co_return makeError(Error::Code::INVALID_ARGUMENTS,
                        "empty AllocatedHandle");
  }

  auto& fccItem = reinterpret_cast<FlashCacheItem&>(*handle);
  auto* desc = fccItem.getEntryDescriptor();
  auto logicalSizeWritten = desc->keySize + desc->valueSize;
  auto totalSize = fccItem.getTotalSize();
  auto keyHash = desc->keyHash;

  // NOTE: we don't need to run this on a worker thread because everything here
  // is synchronous - only allocation is async

  if (cache_->checksumData_) {
    desc->cs = checksum(fccItem.getBuffer().view().slice(0, desc->valueSize));
  }
  cache_->regionManager_.write(fccItem.getRelAddress(),
                               std::move(fccItem.getBuffer()));
  cache_->logicalWrittenCount_.add(logicalSizeWritten);
  auto inserted = cache_->updateIndex(keyHash, totalSize,
                                      fccItem.getRelAddress(), allowReplace);
  if (inserted) {
    setInserted(handle, true);
    auto _ = std::move(handle);
    co_return folly::unit;
  } else {
    // NOTE: do hole accounting in release()
    co_return makeError(Error::Code::ALREADY_INSERTED, "key already inserted");
  }
}

folly::coro::Task<UnitResult> FlashCacheComponent::insert(
    AllocatedHandle&& handle) {
  co_return co_await insertImpl(std::move(handle), /* allowReplace */ false);
}

folly::coro::Task<Result<std::optional<AllocatedHandle>>>
FlashCacheComponent::insertOrReplace(AllocatedHandle&& handle) {
  auto inserted =
      co_await insertImpl(std::move(handle), /* allowReplace */ true);
  XDCHECK(inserted) << "insertOrReplace should never fail";
  // NOTE: returning the old data requires a flash read, don't expose for now
  co_return std::move(inserted).then([](auto&&) {
    return Result<std::optional<AllocatedHandle>>(std::nullopt);
  });
}

folly::coro::Task<Result<std::optional<ReadHandle>>> FlashCacheComponent::find(
    Key key) {
  auto res = co_await onWorkerThread(
      [this, hashedKey = HashedKey(key)]()
          -> folly::Expected<BlockCache::LookupData, navy::Status> {
        auto ld = cache_->lookupInternal(hashedKey);
        if (ld.status_ == Status::Retry) {
          XDCHECK(!ld.desc_.isReady())
              << "should not have a ready descriptor for retry status";
          return folly::makeUnexpected(ld.status_);
        }
        return ld;
      },
      [this](auto&& res) {
        if (res.hasValue() && res->desc_.isReady()) {
          cache_->regionManager_.close(std::move(res->desc_));
        }
      });
  // Retry is handled by onWorkerThread, we should always have a value here
  XDCHECK(res.hasValue()) << "should always get value from lookupInternal()";
  auto& ld = res.value();

  if (ld.status_ == Status::Ok) {
    auto expiryTime = reinterpret_cast<const uint32_t*>(ld.buffer_.data())[1];
    if (util::isExpired(expiryTime)) {
      cache_->regionManager_.close(std::move(ld.desc_));
      co_return std::nullopt;
    }
    co_return ReadHandle(*this, *(new FlashCacheItem(std::move(ld))));
  }

  if (ld.desc_.isReady()) {
    cache_->regionManager_.close(std::move(ld.desc_));
  }
  if (ld.status_ == Status::NotFound) {
    co_return std::nullopt;
  } else {
    co_return makeError(Error::Code::FIND_FAILED,
                        "flash device or checksum error");
  }
}

folly::coro::Task<Result<std::optional<WriteHandle>>>
FlashCacheComponent::findToWrite(Key /* key */) {
  co_return makeError(Error::Code::UNIMPLEMENTED, "not yet implemented");
}

folly::coro::Task<Result<bool>> FlashCacheComponent::remove(Key /* key */) {
  co_return makeError(Error::Code::UNIMPLEMENTED, "not yet implemented");
}

folly::coro::Task<UnitResult> FlashCacheComponent::remove(
    ReadHandle&& /* handle */) {
  co_return makeError(Error::Code::UNIMPLEMENTED, "not yet implemented");
}

FlashCacheComponent::FlashCacheComponent(std::string&& name,
                                         navy::BlockCache::Config&& config)
    : name_(std::move(name)),
      cache_(std::make_unique<navy::BlockCache>(std::move(config))) {}

UnitResult FlashCacheComponent::writeBack(CacheItem& /* item */) {
  return makeError(Error::Code::UNIMPLEMENTED, "not yet implemented");
}

folly::coro::Task<void> FlashCacheComponent::release(CacheItem& item,
                                                     bool inserted) {
  auto& fccItem = reinterpret_cast<FlashCacheItem&>(item);
  if (!inserted) {
    cache_->addHole(fccItem.getTotalSize());
  }
  cache_->regionManager_.close(std::move(fccItem.getRegionDescriptor()));
  delete &item;
  co_return;
}

} // namespace facebook::cachelib::interface

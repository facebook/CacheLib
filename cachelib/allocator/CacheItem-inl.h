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

// CacheItem implementations.
namespace facebook {
namespace cachelib {
template <typename CacheTrait>
uint32_t CacheItem<CacheTrait>::getRequiredSize(Key key,
                                                uint32_t size) noexcept {
  const uint64_t requiredSize =
      static_cast<uint64_t>(size) + key.size() + sizeof(Item);

  XDCHECK_LE(requiredSize,
             static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()));
  if (requiredSize >
      static_cast<uint64_t>(std::numeric_limits<uint32_t>::max())) {
    return 0;
  }
  return static_cast<uint32_t>(requiredSize);
}

template <typename CacheTrait>
uint64_t CacheItem<CacheTrait>::getRefcountMax() noexcept {
  return RefcountWithFlags::kAccessRefMask;
}

template <typename CacheTrait>
CacheItem<CacheTrait>::CacheItem(Key key,
                                 uint32_t size,
                                 uint32_t creationTime,
                                 uint32_t expiryTime)
    : creationTime_(creationTime), expiryTime_(expiryTime), alloc_(key, size) {}

template <typename CacheTrait>
CacheItem<CacheTrait>::CacheItem(Key key, uint32_t size, uint32_t creationTime)
    : CacheItem(key, size, creationTime, 0 /* expiryTime_ */) {}

template <typename CacheTrait>
const typename CacheItem<CacheTrait>::Key CacheItem<CacheTrait>::getKey()
    const noexcept {
  return alloc_.getKey();
}

template <typename CacheTrait>
const void* CacheItem<CacheTrait>::getMemory() const noexcept {
  return getMemoryInternal();
}

template <typename CacheTrait>
void* CacheItem<CacheTrait>::getMemory() noexcept {
  return getMemoryInternal();
}

template <typename CacheTrait>
void* CacheItem<CacheTrait>::getMemoryInternal() const noexcept {
  if (isChainedItem()) {
    return asChainedItem().getMemory();
  } else {
    return alloc_.getMemory();
  }
}

template <typename CacheTrait>
uint32_t CacheItem<CacheTrait>::getOffsetForMemory() const noexcept {
  return reinterpret_cast<uintptr_t>(getMemory()) -
         reinterpret_cast<uintptr_t>(this);
}

template <typename CacheTrait>
uint32_t CacheItem<CacheTrait>::getSize() const noexcept {
  if (isChainedItem()) {
    return asChainedItem().getSize();
  } else {
    return alloc_.getSize();
  }
}

template <typename CacheTrait>
uint32_t CacheItem<CacheTrait>::getTotalSize() const noexcept {
  auto headerSize = reinterpret_cast<uintptr_t>(getMemory()) -
                    reinterpret_cast<uintptr_t>(this);
  return headerSize + getSize();
}

template <typename CacheTrait>
uint32_t CacheItem<CacheTrait>::getExpiryTime() const noexcept {
  return expiryTime_;
}

template <typename CacheTrait>
bool CacheItem<CacheTrait>::isExpired() const noexcept {
  thread_local uint32_t staleTime = 0;

  if (expiryTime_ == 0) {
    return false;
  }

  if (expiryTime_ < staleTime) {
    return true;
  }

  uint32_t currentTime = static_cast<uint32_t>(util::getCurrentTimeSec());
  if (currentTime != staleTime) {
    staleTime = currentTime;
  }
  return expiryTime_ < currentTime;
}

template <typename CacheTrait>
bool CacheItem<CacheTrait>::isExpired(uint32_t currentTimeSec) const noexcept {
  return (expiryTime_ > 0 && expiryTime_ < currentTimeSec);
}

template <typename CacheTrait>
uint32_t CacheItem<CacheTrait>::getCreationTime() const noexcept {
  return creationTime_;
}

template <typename CacheTrait>
std::chrono::seconds CacheItem<CacheTrait>::getConfiguredTTL() const noexcept {
  return std::chrono::seconds(expiryTime_ > 0 ? expiryTime_ - creationTime_
                                              : 0);
}

template <typename CacheTrait>
uint32_t CacheItem<CacheTrait>::getLastAccessTime() const noexcept {
  return mmHook_.getUpdateTime();
}

template <typename CacheTrait>
std::string CacheItem<CacheTrait>::toString() const {
  if (isChainedItem()) {
    return asChainedItem().toString();
  } else {
    return folly::sformat(
        "item: "
        "memory={}:raw-ref={}:size={}:key={}:hex-key={}:"
        "isInMMContainer={}:isAccessible={}:isMarkedForEviction={}:"
        "isMoving={}:references={}:ctime="
        "{}:"
        "expTime={}:updateTime={}:isNvmClean={}:isNvmEvicted={}:hasChainedItem="
        "{}",
        this, getRefCountAndFlagsRaw(), getSize(),
        folly::humanify(getKey().str()), folly::hexlify(getKey()),
        isInMMContainer(), isAccessible(), isMarkedForEviction(), isMoving(),
        getRefCount(), getCreationTime(), getExpiryTime(), getLastAccessTime(),
        isNvmClean(), isNvmEvicted(), hasChainedItem());
  }
}

template <typename CacheTrait>
void CacheItem<CacheTrait>::changeKey(Key key) {
  if (!isChainedItem()) {
    throw std::invalid_argument("Item is not chained type");
  }

  alloc_.changeKey(key);
  XDCHECK_EQ(key, getKey());
}

template <typename CacheTrait>
RefcountWithFlags::Value CacheItem<CacheTrait>::getRefCount() const noexcept {
  return ref_.getAccessRef();
}

template <typename CacheTrait>
RefcountWithFlags::Value CacheItem<CacheTrait>::getRefCountAndFlagsRaw()
    const noexcept {
  return ref_.getRaw();
}

template <typename CacheTrait>
bool CacheItem<CacheTrait>::isDrained() const noexcept {
  return ref_.isDrained();
}

template <typename CacheTrait>
void CacheItem<CacheTrait>::markAccessible() noexcept {
  ref_.markAccessible();
}

template <typename CacheTrait>
void CacheItem<CacheTrait>::unmarkAccessible() noexcept {
  ref_.unmarkAccessible();
}

template <typename CacheTrait>
void CacheItem<CacheTrait>::markInMMContainer() noexcept {
  ref_.markInMMContainer();
}

template <typename CacheTrait>
void CacheItem<CacheTrait>::unmarkInMMContainer() noexcept {
  ref_.unmarkInMMContainer();
}

template <typename CacheTrait>
bool CacheItem<CacheTrait>::isAccessible() const noexcept {
  return ref_.isAccessible();
}

template <typename CacheTrait>
bool CacheItem<CacheTrait>::isInMMContainer() const noexcept {
  return ref_.isInMMContainer();
}

template <typename CacheTrait>
bool CacheItem<CacheTrait>::markForEviction() noexcept {
  return ref_.markForEviction();
}

template <typename CacheTrait>
RefcountWithFlags::Value CacheItem<CacheTrait>::unmarkForEviction() noexcept {
  return ref_.unmarkForEviction();
}

template <typename CacheTrait>
bool CacheItem<CacheTrait>::isMarkedForEviction() const noexcept {
  return ref_.isMarkedForEviction();
}

template <typename CacheTrait>
bool CacheItem<CacheTrait>::markForEvictionWhenMoving() {
  return ref_.markForEvictionWhenMoving();
}

template <typename CacheTrait>
bool CacheItem<CacheTrait>::markMoving() {
  return ref_.markMoving();
}

template <typename CacheTrait>
RefcountWithFlags::Value CacheItem<CacheTrait>::unmarkMoving() noexcept {
  return ref_.unmarkMoving();
}

template <typename CacheTrait>
bool CacheItem<CacheTrait>::isMoving() const noexcept {
  return ref_.isMoving();
}

template <typename CacheTrait>
bool CacheItem<CacheTrait>::isOnlyMoving() const noexcept {
  return ref_.isOnlyMoving();
}

template <typename CacheTrait>
void CacheItem<CacheTrait>::markNvmClean() noexcept {
  ref_.markNvmClean();
}

template <typename CacheTrait>
void CacheItem<CacheTrait>::unmarkNvmClean() noexcept {
  ref_.unmarkNvmClean();
}

template <typename CacheTrait>
bool CacheItem<CacheTrait>::isNvmClean() const noexcept {
  return ref_.isNvmClean();
}

template <typename CacheTrait>
void CacheItem<CacheTrait>::markNvmEvicted() noexcept {
  ref_.markNvmEvicted();
}

template <typename CacheTrait>
void CacheItem<CacheTrait>::unmarkNvmEvicted() noexcept {
  ref_.unmarkNvmEvicted();
}

template <typename CacheTrait>
bool CacheItem<CacheTrait>::isNvmEvicted() const noexcept {
  return ref_.isNvmEvicted();
}

template <typename CacheTrait>
void CacheItem<CacheTrait>::markIsChainedItem() noexcept {
  XDCHECK(!hasChainedItem());
  ref_.markIsChainedItem();
}

template <typename CacheTrait>
void CacheItem<CacheTrait>::unmarkIsChainedItem() noexcept {
  XDCHECK(!hasChainedItem());
  ref_.unmarkIsChainedItem();
}

template <typename CacheTrait>
void CacheItem<CacheTrait>::markHasChainedItem() noexcept {
  XDCHECK(!isChainedItem());
  ref_.markHasChainedItem();
}

template <typename CacheTrait>
void CacheItem<CacheTrait>::unmarkHasChainedItem() noexcept {
  XDCHECK(!isChainedItem());
  ref_.unmarkHasChainedItem();
}

template <typename CacheTrait>
typename CacheItem<CacheTrait>::ChainedItem&
CacheItem<CacheTrait>::asChainedItem() noexcept {
  return *static_cast<ChainedItem*>(this);
}

template <typename CacheTrait>
const typename CacheItem<CacheTrait>::ChainedItem&
CacheItem<CacheTrait>::asChainedItem() const noexcept {
  return *static_cast<const ChainedItem*>(this);
}

template <typename CacheTrait>
bool CacheItem<CacheTrait>::isChainedItem() const noexcept {
  return ref_.isChainedItem();
}

template <typename CacheTrait>
bool CacheItem<CacheTrait>::hasChainedItem() const noexcept {
  return ref_.hasChainedItem();
}

template <typename CacheTrait>
template <typename RefcountWithFlags::Flags flagBit>
void CacheItem<CacheTrait>::setFlag() noexcept {
  ref_.template setFlag<flagBit>();
}

template <typename CacheTrait>
template <typename RefcountWithFlags::Flags flagBit>
void CacheItem<CacheTrait>::unSetFlag() noexcept {
  ref_.template unSetFlag<flagBit>();
}

template <typename CacheTrait>
template <typename RefcountWithFlags::Flags flagBit>
bool CacheItem<CacheTrait>::isFlagSet() const noexcept {
  return ref_.template isFlagSet<flagBit>();
}

template <typename CacheTrait>
bool CacheItem<CacheTrait>::updateExpiryTime(uint32_t expiryTimeSecs) noexcept {
  // check for moving to make sure we are not updating the expiry time while at
  // the same time re-allocating the item with the old state of the expiry time
  // in moveRegularItem(). See D6852328
  if (isMoving() || isMarkedForEviction() || !isInMMContainer() ||
      isChainedItem()) {
    return false;
  }
  // attempt to atomically update the value of expiryTime
  while (true) {
    uint32_t currExpTime = expiryTime_;
    if (__sync_bool_compare_and_swap(&expiryTime_, currExpTime,
                                     expiryTimeSecs)) {
      return true;
    }
  }
}

template <typename CacheTrait>
bool CacheItem<CacheTrait>::extendTTL(std::chrono::seconds ttl) noexcept {
  return updateExpiryTime(util::getCurrentTimeSec() + ttl.count());
}

// Chained items are chained in a single linked list. The payload of each
// chained item is chained to the next item.
template <typename CacheTrait>
class CACHELIB_PACKED_ATTR ChainedItemPayload {
 public:
  using ChainedItem = CacheChainedItem<CacheTrait>;
  using Item = CacheItem<CacheTrait>;
  using PtrCompressor = typename ChainedItem::PtrCompressor;

  // Pointer to the next chained allocation. initialize to nullptr.
  SListHook<Item> hook_{};

  // Payload
  mutable unsigned char data_[0];

  // Usable memory for this allocation. The caller is free to do whatever he
  // wants with it and needs to ensure concurrency for access into this
  // piece of memory.
  void* getMemory() const noexcept { return &data_; }

  ChainedItem* getNext(const PtrCompressor& compressor) const noexcept {
    return static_cast<ChainedItem*>(hook_.getNext(compressor));
  }

  void setNext(const ChainedItem* next,
               const PtrCompressor& compressor) noexcept {
    hook_.setNext(static_cast<const Item*>(next), compressor);
    XDCHECK_EQ(reinterpret_cast<uintptr_t>(getNext(compressor)),
               reinterpret_cast<uintptr_t>(next));
  }
};

template <typename CacheTrait>
uint32_t CacheChainedItem<CacheTrait>::getRequiredSize(uint32_t size) noexcept {
  const uint64_t requiredSize = static_cast<uint64_t>(size) +
                                static_cast<uint64_t>(kKeySize) + sizeof(Item) +
                                sizeof(Payload);
  XDCHECK_LE(requiredSize,
             static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()));
  if (requiredSize >
      static_cast<uint64_t>(std::numeric_limits<uint32_t>::max())) {
    return 0;
  }
  return static_cast<uint32_t>(requiredSize);
}

template <typename CacheTrait>
CacheChainedItem<CacheTrait>::CacheChainedItem(CompressedPtr ptr,
                                               uint32_t size,
                                               uint32_t creationTime)
    : Item(Key{reinterpret_cast<const char*>(&ptr), kKeySize},
           size + sizeof(Payload),
           creationTime) {
  this->markIsChainedItem();

  // Explicitly call ChainedItemPayload's ctor to initialize it properly, since
  // ChainedItemPayload is not a member of CacheChainedItem.
  new (reinterpret_cast<void*>(&getPayload())) Payload();
}

template <typename CacheTrait>
void CacheChainedItem<CacheTrait>::changeKey(CompressedPtr newKey) {
  if (this->isAccessible()) {
    throw std::invalid_argument(folly::sformat(
        "chained item {} is still in access container while modifying the key ",
        toString()));
  }
  Item::changeKey(Key{reinterpret_cast<const char*>(&newKey), kKeySize});
}

template <typename CacheTrait>
typename CacheChainedItem<CacheTrait>::Item&
CacheChainedItem<CacheTrait>::getParentItem(
    const PtrCompressor& compressor) const noexcept {
  const auto compressedPtr =
      *reinterpret_cast<const CompressedPtr*>(this->getKey().begin());
  return *compressor.unCompress(compressedPtr);
}

template <typename CacheTrait>
void* CacheChainedItem<CacheTrait>::getMemory() const noexcept {
  return getPayload().getMemory();
}

template <typename CacheTrait>
uint32_t CacheChainedItem<CacheTrait>::getSize() const noexcept {
  // Chained Item has its own embedded payload in its KAllocation, so we
  // need to deduct its size here to give the user the accurate usable size
  return this->alloc_.getSize() - sizeof(Payload);
}

template <typename CacheTrait>
std::string CacheChainedItem<CacheTrait>::toString() const {
  const auto cPtr =
      *reinterpret_cast<const CompressedPtr*>(Item::getKey().data());
  return folly::sformat(
      "chained item: "
      "memory={}:raw-ref={}:size={}:parent-compressed-ptr={}:"
      "isInMMContainer={}:isAccessible={}:isMarkedForEviction={}:"
      "isMoving={}:references={}:ctime={}"
      ":"
      "expTime={}:updateTime={}",
      this, Item::getRefCountAndFlagsRaw(), Item::getSize(), cPtr.getRaw(),
      Item::isInMMContainer(), Item::isAccessible(),
      Item::isMarkedForEviction(), Item::isMoving(), Item::getRefCount(),
      Item::getCreationTime(), Item::getExpiryTime(),
      Item::getLastAccessTime());
}

template <typename CacheTrait>
void CacheChainedItem<CacheTrait>::appendChain(
    ChainedItem& newChain, const PtrCompressor& compressor) {
  if (getNext(compressor)) {
    throw std::invalid_argument(
        folly::sformat("Item: {} is not the last item in a chain. Next: {}",
                       toString(), getNext(compressor)->toString()));
  }
  setNext(&newChain, compressor);
}

template <typename CacheTrait>
typename CacheChainedItem<CacheTrait>::ChainedItem*
CacheChainedItem<CacheTrait>::getNext(
    const PtrCompressor& compressor) const noexcept {
  return getPayload().getNext(compressor);
}

template <typename CacheTrait>
void CacheChainedItem<CacheTrait>::setNext(
    const ChainedItem* next, const PtrCompressor& compressor) noexcept {
  getPayload().setNext(next, compressor);
  XDCHECK_EQ(reinterpret_cast<uintptr_t>(getNext(compressor)),
             reinterpret_cast<uintptr_t>(next));
}

template <typename CacheTrait>
typename CacheChainedItem<CacheTrait>::Payload&
CacheChainedItem<CacheTrait>::getPayload() {
  return *reinterpret_cast<Payload*>(this->alloc_.getMemory());
}

template <typename CacheTrait>
const typename CacheChainedItem<CacheTrait>::Payload&
CacheChainedItem<CacheTrait>::getPayload() const {
  return *reinterpret_cast<const Payload*>(this->alloc_.getMemory());
}
} // namespace cachelib
} // namespace facebook

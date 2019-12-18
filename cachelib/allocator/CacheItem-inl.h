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
  return RefCount::getMaxRefCount();
}

template <typename CacheTrait>
CacheItem<CacheTrait>::CacheItem(Key key,
                                 uint32_t size,
                                 uint32_t creationTime,
                                 uint32_t expiryTime)
#if FOLLY_X64
    : creationTime_(creationTime), expiryTime_(expiryTime), alloc_(key, size) {
}
#elif FOLLY_AARCH64
    : expiryTime_(expiryTime), creationTime_(creationTime), alloc_(key, size) {
}
#endif

template <typename CacheTrait>
CacheItem<CacheTrait>::CacheItem(Key key, uint32_t size, uint32_t creationTime)
    : CacheItem(key, size, creationTime, 0 /* expiryTime_ */) {}

template <typename CacheTrait>
const typename CacheItem<CacheTrait>::Key CacheItem<CacheTrait>::getKey() const
    noexcept {
  return alloc_.getKey();
}

template <typename CacheTrait>
void* CacheItem<CacheTrait>::getMemory() const noexcept {
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
uint32_t CacheItem<CacheTrait>::getExpiryTime() const noexcept {
  return expiryTime_;
}

template <typename CacheTrait>
bool CacheItem<CacheTrait>::isExpired() const noexcept {
  return (expiryTime_ > 0 &&
          expiryTime_ < static_cast<uint32_t>(util::getCurrentTimeSec()));
}

template <typename CacheTrait>
uint32_t CacheItem<CacheTrait>::getCreationTime() const noexcept {
  return creationTime_;
}

template <typename CacheTrait>
uint32_t CacheItem<CacheTrait>::getConfiguredTTL() const noexcept {
  return expiryTime_ - creationTime_;
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
        "memory={}:flags={}:raw-ref={}:size={}:key={}:hex-key={}:"
        "isInMMContainer={}:isAccessible={}:isMoving={}:references={}:ctime={}:"
        "expTime={}:updateTime={}:isNvmClean={}:isNvmEvicted={}:hasChainedItem="
        "{}",
        this, static_cast<uint32_t>(flags_), getRefCountRaw(), getSize(),
        folly::humanify(getKey().str()), folly::hexlify(getKey()),
        isInMMContainer(), isAccessible(), isMoving(), getRefCount(),
        getCreationTime(), getExpiryTime(), getLastAccessTime(), isNvmClean(),
        isNvmEvicted(), hasChainedItem());
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
typename CacheItem<CacheTrait>::RefCount::Value
CacheItem<CacheTrait>::getRefCount() const noexcept {
  return ref_.getCount();
}

template <typename CacheTrait>
typename CacheItem<CacheTrait>::RefCount::Value
CacheItem<CacheTrait>::getRefCountRaw() const noexcept {
  return ref_.getRaw();
}

template <typename CacheTrait>
void CacheItem<CacheTrait>::markAccessible() noexcept {
  constexpr auto accessFlag = static_cast<uint8_t>(RefFlags::kAccessible);
  ref_.template setFlag<accessFlag>();
}

template <typename CacheTrait>
void CacheItem<CacheTrait>::markInMMContainer() noexcept {
  constexpr auto linkFlag = static_cast<uint8_t>(RefFlags::kLinked);
  ref_.template setFlag<linkFlag>();
}

template <typename CacheTrait>
void CacheItem<CacheTrait>::unmarkInMMContainer() noexcept {
  constexpr auto linkFlag = static_cast<uint8_t>(RefFlags::kLinked);
  ref_.template unsetFlag<linkFlag>();
}

template <typename CacheTrait>
void CacheItem<CacheTrait>::unmarkAccessible() noexcept {
  constexpr auto accessFlag = static_cast<uint8_t>(RefFlags::kAccessible);
  ref_.template unsetFlag<accessFlag>();
}

template <typename CacheTrait>
bool CacheItem<CacheTrait>::isAccessible() const noexcept {
  constexpr auto accessFlag = static_cast<uint8_t>(RefFlags::kAccessible);
  return ref_.template isFlagSet<accessFlag>();
}

template <typename CacheTrait>
bool CacheItem<CacheTrait>::isInMMContainer() const noexcept {
  constexpr auto linkFlag = static_cast<uint8_t>(RefFlags::kLinked);
  return ref_.template isFlagSet<linkFlag>();
}

template <typename CacheTrait>
bool CacheItem<CacheTrait>::markMoving() noexcept {
  constexpr auto linkFlag = static_cast<uint8_t>(RefFlags::kLinked);
  constexpr auto movingFlag = static_cast<uint8_t>(RefFlags::kMoving);
  return ref_.template setFlagConditional<movingFlag, linkFlag>();
}

template <typename CacheTrait>
void CacheItem<CacheTrait>::unmarkMoving() noexcept {
  constexpr auto movingFlag = static_cast<uint8_t>(RefFlags::kMoving);
  ref_.template unsetFlag<movingFlag>();
}

template <typename CacheTrait>
bool CacheItem<CacheTrait>::isMoving() const noexcept {
  constexpr auto movingFlag = static_cast<uint8_t>(RefFlags::kMoving);
  return ref_.template isFlagSet<movingFlag>();
}

template <typename CacheTrait>
bool CacheItem<CacheTrait>::isOnlyMoving() const noexcept {
  constexpr auto movingFlag = static_cast<uint8_t>(RefFlags::kMoving);
  return ref_.template isOnlyFlagSet<movingFlag>();
}

template <typename CacheTrait>
void CacheItem<CacheTrait>::markUnevictable() noexcept {
  setFlag<Flags::UNEVICTABLE>();
}

template <typename CacheTrait>
void CacheItem<CacheTrait>::unmarkUnevictable() noexcept {
  unSetFlag<Flags::UNEVICTABLE>();
}

template <typename CacheTrait>
bool CacheItem<CacheTrait>::isUnevictable() const noexcept {
  return isFlagSet<Flags::UNEVICTABLE>();
}

template <typename CacheTrait>
bool CacheItem<CacheTrait>::isEvictable() const noexcept {
  return !isFlagSet<Flags::UNEVICTABLE>();
}

template <typename CacheTrait>
void CacheItem<CacheTrait>::markNvmClean() noexcept {
  setFlag<Flags::NVM_CLEAN>();
}

template <typename CacheTrait>
void CacheItem<CacheTrait>::unmarkNvmClean() noexcept {
  unSetFlag<Flags::NVM_CLEAN>();
}

template <typename CacheTrait>
bool CacheItem<CacheTrait>::isNvmClean() const noexcept {
  return isFlagSet<Flags::NVM_CLEAN>();
}

template <typename CacheTrait>
void CacheItem<CacheTrait>::markNvmEvicted() noexcept {
  setFlag<Flags::NVM_EVICTED>();
}

template <typename CacheTrait>
void CacheItem<CacheTrait>::unmarkNvmEvicted() noexcept {
  unSetFlag<Flags::NVM_EVICTED>();
}

template <typename CacheTrait>
bool CacheItem<CacheTrait>::isNvmEvicted() const noexcept {
  return isFlagSet<Flags::NVM_EVICTED>();
}

template <typename CacheTrait>
void CacheItem<CacheTrait>::markIsChainedItem() noexcept {
  XDCHECK(!hasChainedItem());
  setFlag<Flags::IS_CHAINED_ITEM>();
}

template <typename CacheTrait>
void CacheItem<CacheTrait>::unmarkIsChainedItem() noexcept {
  XDCHECK(!hasChainedItem());
  unSetFlag<Flags::IS_CHAINED_ITEM>();
}

template <typename CacheTrait>
void CacheItem<CacheTrait>::markHasChainedItem() noexcept {
  XDCHECK(!isChainedItem());
  setFlag<Flags::HAS_CHAINED_ITEM>();
}

template <typename CacheTrait>
void CacheItem<CacheTrait>::unmarkHasChainedItem() noexcept {
  XDCHECK(!isChainedItem());
  unSetFlag<Flags::HAS_CHAINED_ITEM>();
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
  XDCHECK(!(isFlagSet<Flags::HAS_CHAINED_ITEM>() &&
            isFlagSet<Flags::IS_CHAINED_ITEM>()));
  return isFlagSet<Flags::IS_CHAINED_ITEM>();
}

template <typename CacheTrait>
bool CacheItem<CacheTrait>::hasChainedItem() const noexcept {
  XDCHECK(!(isFlagSet<Flags::HAS_CHAINED_ITEM>() &&
            isFlagSet<Flags::IS_CHAINED_ITEM>()));
  return isFlagSet<Flags::HAS_CHAINED_ITEM>();
}

template <typename CacheTrait>
template <typename CacheItem<CacheTrait>::Flags flagBit>
void CacheItem<CacheTrait>::setFlag() noexcept {
  constexpr FlagT bitmask =
      (static_cast<FlagT>(1) << static_cast<unsigned int>(flagBit));
  __sync_or_and_fetch(&flags_, bitmask);
}

template <typename CacheTrait>
template <typename CacheItem<CacheTrait>::Flags flagBit>
void CacheItem<CacheTrait>::unSetFlag() noexcept {
  constexpr FlagT bitmask =
      std::numeric_limits<FlagT>::max() -
      (static_cast<FlagT>(1) << static_cast<unsigned int>(flagBit));
  __sync_fetch_and_and(&flags_, bitmask);
}

template <typename CacheTrait>
template <typename CacheItem<CacheTrait>::Flags flagBit>
bool CacheItem<CacheTrait>::isFlagSet() const noexcept {
  return flags_ & (static_cast<FlagT>(1) << static_cast<unsigned int>(flagBit));
}

template <typename CacheTrait>
bool CacheItem<CacheTrait>::updateExpiryTime(uint32_t expiryTimeSecs) noexcept {
  // check for moving to make sure we are not updating the expiry time while at
  // the same time re-allocating the item with the old state of the expiry time
  // in moveRegularItem(). See D6852328
  if (isMoving() || !isInMMContainer() || isChainedItem()) {
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
      "memory={}:flags={}:raw-ref={}:size={}:parent-compressed-ptr={}:"
      "isInMMContainer={}:isAccessible={}:isMoving={}:references={}:ctime={}:"
      "expTime={}:updateTime={}",
      this, static_cast<uint32_t>(Item::flags_), Item::getRefCountRaw(),
      Item::getSize(), cPtr.getRaw(), Item::isInMMContainer(),
      Item::isAccessible(), Item::isMoving(), Item::getRefCount(),
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
CacheChainedItem<CacheTrait>::getNext(const PtrCompressor& compressor) const
    noexcept {
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

namespace facebook {
namespace cachelib {

// Number of hashes
constexpr size_t kHashCount = 4;
// The error threshold for frequency calculation
constexpr size_t kErrorThreshold = 5;

/* Container Interface Implementation */
template <typename T, MMTinyLFU::Hook<T> T::*HookPtr>
MMTinyLFU::Container<T, HookPtr>::Container(
    serialization::MMTinyLFUObject object, PtrCompressor compressor)
    : lru_(object.lrus, std::move(compressor)), config_(object.config) {
  maybeGrowAccessCountersLocked();
}

template <typename T, MMTinyLFU::Hook<T> T::*HookPtr>
void MMTinyLFU::Container<T,
                          HookPtr>::maybeGrowAccessCountersLocked() noexcept {
  size_t capacity = lru_.size();
  // If the new capacity ask is more than double the current size, recreate
  // the approx frequency counters.
  if (2 * capacity_ > capacity) {
    return;
  }
  capacity_ = std::max(capacity, kDefaultCapacity);
  // The window counter that's incremented on every fetch.
  windowSize_ = 0;
  // The frequency counters are halved every maxWindowSize_ fetches to decay the
  // frequency counts.
  maxWindowSize_ = capacity_ * config_.windowToCacheSizeRatio;
  // Number of frequency counters - roughly equal to the window size divided by
  // error tolerance.
  size_t numCounters =
      static_cast<size_t>(std::exp(1.0) * capacity_ *
                          config_.windowToCacheSizeRatio / kErrorThreshold);
  numCounters = folly::nextPowTwo(numCounters);
  // Number of bits for each frequency counter, determined by ratio of
  // window size to cache size.
  auto numBits = std::ceil(std::log(folly::divCeil(maxWindowSize_, capacity_)));
  // The CountMinSketch frequency counter
  accessFreq_ = std::make_unique<unicorn::datastruct::CountMinSketch>(
      kHashCount, numCounters, numBits);
}

template <typename T, MMTinyLFU::Hook<T> T::*HookPtr>
bool MMTinyLFU::Container<T, HookPtr>::recordAccess(T& node,
                                                    AccessMode mode) noexcept {
  if ((mode == AccessMode::kWrite && !config_.updateOnWrite) ||
      (mode == AccessMode::kRead && !config_.updateOnRead)) {
    return false;
  }

  const auto curr = static_cast<Time>(util::getCurrentTimeSec());
  // check if the node is still being memory managed
  if (node.isInMMContainer() &&
      ((curr >= getUpdateTime(node) + config_.lruRefreshTime) ||
       !isAccessed(node))) {
    if (!isAccessed(node)) {
      markAccessed(node);
    }
    LockHolder l(lruMutex_, std::defer_lock);
    if (config_.tryLockUpdate) {
      l.try_lock();
    } else {
      l.lock();
    }
    if (!l.owns_lock()) {
      return false;
    }
    reconfigureLocked(curr);
    ++numLockByRecordAccesses_;
    if (!node.isInMMContainer()) {
      return false;
    }

    lru_.getList(getLruType(node)).moveToHead(node);
    setUpdateTime(node, curr);
    updateFrequenciesLocked(node);
    return true;
  }
  return false;
}

template <typename T, MMTinyLFU::Hook<T> T::*HookPtr>
cachelib::EvictionAgeStat MMTinyLFU::Container<T, HookPtr>::getEvictionAgeStat(
    uint64_t projectedLength) const noexcept {
  LockHolder l(lruMutex_);
  return getEvictionAgeStatLocked(projectedLength);
}

template <typename T, MMTinyLFU::Hook<T> T::*HookPtr>
cachelib::EvictionAgeStat
MMTinyLFU::Container<T, HookPtr>::getEvictionAgeStatLocked(
    uint64_t projectedLength) const noexcept {
  EvictionAgeStat stat;
  const auto curr = static_cast<Time>(util::getCurrentTimeSec());

  auto& list = lru_.getList(LruType::Main);
  auto it = list.rbegin();
  stat.warmQueueStat.oldestElementAge =
      it != list.rend() ? curr - getUpdateTime(*it) : 0;
  stat.warmQueueStat.size = list.size();
  for (size_t numSeen = 0; numSeen < projectedLength && it != list.rend();
       ++numSeen, ++it) {
  }
  stat.projectedAge = it != list.rend() ? curr - getUpdateTime(*it)
                                        : stat.warmQueueStat.oldestElementAge;
  return stat;
}

template <typename T, MMTinyLFU::Hook<T> T::*HookPtr>
void MMTinyLFU::Container<T, HookPtr>::updateFrequenciesLocked(
    const T& node) noexcept {
  accessFreq_->add(hashNode(node));
  ++windowSize_;
  // Halve frequency counts every maxWindowSize_ to decay the frequencies.
  // This avoids having items that were accessed frequently (were hot) but
  // aren't being accessed anymore (are cold) from staying in cache forever.
  if (windowSize_ == maxWindowSize_) {
    windowSize_ >>= 1;
    accessFreq_->halveAllCounters();
  }
}

template <typename T, MMTinyLFU::Hook<T> T::*HookPtr>
void MMTinyLFU::Container<T, HookPtr>::maybePromoteTailLocked() noexcept {
  // Choose eviction candidate and place it at the tail of tiny cache
  // from where evictions occur.
  auto mainNode = lru_.getList(LruType::Main).getTail();
  if (!mainNode) {
    return;
  }
  XDCHECK(!isTiny(*mainNode));

  auto tinyNode = lru_.getList(LruType::Tiny).getTail();
  if (!tinyNode) {
    return;
  }
  XDCHECK(isTiny(*tinyNode));

  if (admitToMain(*tinyNode, *mainNode)) {
    lru_.getList(LruType::Tiny).remove(*tinyNode);
    lru_.getList(LruType::Main).linkAtHead(*tinyNode);
    unmarkTiny(*tinyNode);

    lru_.getList(LruType::Main).remove(*mainNode);
    lru_.getList(LruType::Tiny).linkAtTail(*mainNode);
    markTiny(*mainNode);
    return;
  }

  // A node with high frequency at the tail of main cache might prevent
  // promotions from tiny cache from happening for a long time. Relocate
  // the tail of main cache to prevent this.
  lru_.getList(LruType::Main).moveToHead(*mainNode);
}

template <typename T, MMTinyLFU::Hook<T> T::*HookPtr>
bool MMTinyLFU::Container<T, HookPtr>::add(T& node) noexcept {
  const auto currTime = static_cast<Time>(util::getCurrentTimeSec());
  LockHolder l(lruMutex_);
  ++numLockByInserts_;
  if (node.isInMMContainer()) {
    return false;
  }

  auto& tinyLru = lru_.getList(LruType::Tiny);
  tinyLru.linkAtHead(node);
  markTiny(node);
  // Initialize the frequency count for this node.
  updateFrequenciesLocked(node);
  // If tiny cache is full, unconditionally promote tail to main cache.
  const auto expectedSize = config_.tinySizePercent * lru_.size() / 100;
  if (lru_.getList(LruType::Tiny).size() > expectedSize) {
    auto tailNode = tinyLru.getTail();
    tinyLru.remove(*tailNode);

    auto& mainLru = lru_.getList(LruType::Main);
    mainLru.linkAtHead(*tailNode);
    unmarkTiny(*tailNode);
  } else {
    // The tiny and main cache are full. Swap the tails of tiny and main cache
    // if the tiny tail has a higher frequency than the main tail.
    maybePromoteTailLocked();
  }
  // If the number of counters are too small for the cache size, double them.
  // TODO: If this shows in latency, we may need to grow the counters
  // asynchronously.
  maybeGrowAccessCountersLocked();

  node.markInMMContainer();
  setUpdateTime(node, currTime);
  unmarkAccessed(node);
  return true;
}

template <typename T, MMTinyLFU::Hook<T> T::*HookPtr>
typename MMTinyLFU::Container<T, HookPtr>::Iterator
MMTinyLFU::Container<T, HookPtr>::getEvictionIterator() const noexcept {
  LockHolder l(lruMutex_);
  return Iterator{std::move(l), *this};
}

template <typename T, MMTinyLFU::Hook<T> T::*HookPtr>
void MMTinyLFU::Container<T, HookPtr>::removeLocked(T& node) noexcept {
  if (isTiny(node)) {
    lru_.getList(LruType::Tiny).remove(node);
    unmarkTiny(node);
  } else {
    lru_.getList(LruType::Main).remove(node);
  }

  unmarkAccessed(node);
  node.unmarkInMMContainer();
  return;
}

template <typename T, MMTinyLFU::Hook<T> T::*HookPtr>
bool MMTinyLFU::Container<T, HookPtr>::remove(T& node) noexcept {
  LockHolder l(lruMutex_);
  ++numLockByRemoves_;
  if (!node.isInMMContainer()) {
    return false;
  }
  removeLocked(node);
  return true;
}

template <typename T, MMTinyLFU::Hook<T> T::*HookPtr>
void MMTinyLFU::Container<T, HookPtr>::remove(Iterator& it) noexcept {
  T& node = *it;
  XDCHECK(node.isInMMContainer());
  ++it;
  removeLocked(node);
}

template <typename T, MMTinyLFU::Hook<T> T::*HookPtr>
bool MMTinyLFU::Container<T, HookPtr>::replace(T& oldNode,
                                               T& newNode) noexcept {
  LockHolder l(lruMutex_);
  if (!oldNode.isInMMContainer() || newNode.isInMMContainer()) {
    return false;
  }
  const auto updateTime = getUpdateTime(oldNode);

  if (isTiny(oldNode)) {
    lru_.getList(LruType::Tiny).replace(oldNode, newNode);
    unmarkTiny(oldNode);
    markTiny(newNode);
  } else {
    lru_.getList(LruType::Main).replace(oldNode, newNode);
  }

  oldNode.unmarkInMMContainer();
  newNode.markInMMContainer();
  setUpdateTime(newNode, updateTime);
  if (isAccessed(oldNode)) {
    markAccessed(newNode);
  } else {
    unmarkAccessed(newNode);
  }
  return true;
}

template <typename T, MMTinyLFU::Hook<T> T::*HookPtr>
typename MMTinyLFU::Config MMTinyLFU::Container<T, HookPtr>::getConfig() const {
  LockHolder l(lruMutex_);
  return config_;
}

template <typename T, MMTinyLFU::Hook<T> T::*HookPtr>
void MMTinyLFU::Container<T, HookPtr>::setConfig(const Config& c) {
  LockHolder l(lruMutex_);
  config_ = c;
  nextReconfigureTime_ = config_.mmReconfigureIntervalSecs.count() == 0
                             ? std::numeric_limits<Time>::max()
                             : static_cast<Time>(util::getCurrentTimeSec()) +
                                   config_.mmReconfigureIntervalSecs.count();
}

template <typename T, MMTinyLFU::Hook<T> T::*HookPtr>
serialization::MMTinyLFUObject MMTinyLFU::Container<T, HookPtr>::saveState()
    const noexcept {
  serialization::MMTinyLFUConfig configObject;
  configObject.lruRefreshTime = config_.lruRefreshTime;
  configObject.lruRefreshRatio = config_.lruRefreshRatio;
  configObject.updateOnWrite = config_.updateOnWrite;
  configObject.updateOnRead = config_.updateOnRead;
  configObject.windowToCacheSizeRatio = config_.windowToCacheSizeRatio;
  configObject.tinySizePercent = config_.tinySizePercent;
  // TODO: May be save/restore the counters.

  serialization::MMTinyLFUObject object;
  object.config = configObject;
  object.lrus = lru_.saveState();
  return object;
}

template <typename T, MMTinyLFU::Hook<T> T::*HookPtr>
MMContainerStat MMTinyLFU::Container<T, HookPtr>::getStats() const noexcept {
  LockHolder l(lruMutex_);
  auto* tail = lru_.size() == 0 ? nullptr : lru_.rbegin().get();
  return {lru_.size(),
          tail == nullptr ? 0 : getUpdateTime(*tail),
          numLockByInserts_,
          numLockByRecordAccesses_,
          numLockByRemoves_,
          config_.lruRefreshTime,
          0,
          0,
          0,
          0};
}

template <typename T, MMTinyLFU::Hook<T> T::*HookPtr>
void MMTinyLFU::Container<T, HookPtr>::reconfigureLocked(const Time& currTime) {
  if (currTime < nextReconfigureTime_) {
    return;
  }
  nextReconfigureTime_ = currTime + config_.mmReconfigureIntervalSecs.count();

  // update LRU refresh time
  auto stat = getEvictionAgeStatLocked(0);
  auto lruRefreshTime =
      std::max(config_.defaultLruRefreshTime,
               static_cast<uint32_t>(stat.warmQueueStat.oldestElementAge *
                                     config_.lruRefreshRatio));

  config_.lruRefreshTime = lruRefreshTime;
}

// Iterator Context Implementation
template <typename T, MMTinyLFU::Hook<T> T::*HookPtr>
MMTinyLFU::Container<T, HookPtr>::Iterator::Iterator(
    LockHolder l, const Container<T, HookPtr>& c) noexcept
    : c_(c),
      tIter_(c.lru_.getList(LruType::Tiny).rbegin()),
      mIter_(c.lru_.getList(LruType::Main).rbegin()),
      l_(std::move(l)) {}
} // namespace cachelib
} // namespace facebook

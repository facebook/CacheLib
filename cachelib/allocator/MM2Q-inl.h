namespace facebook {
namespace cachelib {

/* Container Interface Implementation */
template <typename T, MM2Q::Hook<T> T::*HookPtr>
MM2Q::Container<T, HookPtr>::Container(const serialization::MM2QObject& object,
                                       PtrCompressor compressor)
    : lru_(object.lrus, compressor),
      tailTrackingEnabled_(*object.tailTrackingEnabled_ref()),
      config_(object.config) {
  // We need to adjust list positions if the previous version does not have
  // tail lists (WarmTail & ColdTail), in order to potentially avoid cold roll
  if (object.lrus.lists.size() < LruType::NumTypes) {
    XDCHECK_EQ(false, tailTrackingEnabled_);
    XDCHECK_EQ(object.lrus.lists.size() + 2, LruType::NumTypes);
    lru_.insertEmptyListAt(LruType::WarmTail, compressor);
    lru_.insertEmptyListAt(LruType::ColdTail, compressor);
  }
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
bool MM2Q::Container<T, HookPtr>::recordAccess(T& node,
                                               AccessMode mode) noexcept {
  if ((mode == AccessMode::kWrite && !config_.updateOnWrite) ||
      (mode == AccessMode::kRead && !config_.updateOnRead)) {
    return false;
  }

  const auto curr = static_cast<Time>(util::getCurrentTimeSec());
  // check if the node is still being memory managed
  if (node.isInMMContainer() &&
      (curr >= getUpdateTime(node) + config_.lruRefreshTime)) {
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
    if (isHot(node)) {
      lru_.getList(LruType::Hot).moveToHead(node);
      ++numHotAccesses_;
    } else if (isCold(node)) {
      if (inTail(node)) {
        unmarkTail(node);
        lru_.getList(LruType::ColdTail).remove(node);
        ++numColdTailAccesses_;
      } else {
        lru_.getList(LruType::Cold).remove(node);
      }
      lru_.getList(LruType::Warm).linkAtHead(node);
      unmarkCold(node);
      ++numColdAccesses_;
      // only rebalance if config says so. recordAccess is called mostly on
      // latency sensitive cache get operations.
      if (config_.rebalanceOnRecordAccess) {
        rebalance();
      }
    } else {
      if (inTail(node)) {
        unmarkTail(node);
        lru_.getList(LruType::WarmTail).remove(node);
        lru_.getList(LruType::Warm).linkAtHead(node);
        ++numWarmTailAccesses_;
      } else {
        lru_.getList(LruType::Warm).moveToHead(node);
      }
      ++numWarmAccesses_;
    }
    setUpdateTime(node, curr);
    return true;
  }
  return false;
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
cachelib::EvictionAgeStat MM2Q::Container<T, HookPtr>::getEvictionAgeStat(
    uint64_t projectedLength) const noexcept {
  LockHolder l(lruMutex_);
  return getEvictionAgeStatLocked(projectedLength);
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
cachelib::EvictionAgeStat MM2Q::Container<T, HookPtr>::getEvictionAgeStatLocked(
    uint64_t projectedLength) const noexcept {
  EvictionAgeStat stat;
  const auto curr = static_cast<Time>(util::getCurrentTimeSec());

  stat.hotQueueStat.oldestElementAge = getOldestAgeLocked(LruType::Hot, curr);
  stat.hotQueueStat.size = lru_.getList(LruType::Hot).size();

  // sum the tail and the main list for the ones that have tail.
  stat.warmQueueStat.oldestElementAge =
      getOldestAgeLocked(LruType::WarmTail, curr);
  stat.warmQueueStat.size = lru_.getList(LruType::Warm).size() +
                            lru_.getList(LruType::WarmTail).size();

  stat.coldQueueStat.oldestElementAge =
      getOldestAgeLocked(LruType::ColdTail, curr);
  stat.coldQueueStat.size = lru_.getList(LruType::ColdTail).size() +
                            lru_.getList(LruType::Cold).size();

  auto it = lru_.rbegin(LruType::WarmTail);
  for (size_t numSeen = 0; numSeen < projectedLength && it != lru_.rend();
       ++numSeen, ++it) {
  }
  stat.projectedAge = (it != lru_.rend()) ? curr - getUpdateTime(*it)
                                          : stat.warmQueueStat.oldestElementAge;
  return stat;
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
uint32_t MM2Q::Container<T, HookPtr>::getOldestAgeLocked(LruType lruType,
                                                         Time currentTime) const
    noexcept {
  auto it = lru_.rbegin(lruType);
  return it != lru_.rend() ? currentTime - getUpdateTime(*it) : 0;
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
typename MM2Q::LruType MM2Q::Container<T, HookPtr>::getLruType(
    const T& node) const noexcept {
  if (isHot(node)) {
    return LruType::Hot;
  }
  if (isCold(node)) {
    return inTail(node) ? LruType::ColdTail : LruType::Cold;
  }
  return inTail(node) ? LruType::WarmTail : LruType::Warm;
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
void MM2Q::Container<T, HookPtr>::rebalance() noexcept {
  // shrink Warm (and WarmTail) if their total size is larger than expected
  size_t expectedSize = config_.getWarmSizePercent() * lru_.size() / 100;
  while (lru_.getList(LruType::Warm).size() +
             lru_.getList(LruType::WarmTail).size() >
         expectedSize) {
    auto popFrom = [&](LruType lruType) -> T* {
      auto& lru = lru_.getList(lruType);
      T* node = lru.getTail();
      XDCHECK(node);
      lru.remove(*node);
      if (lruType == WarmTail || lruType == ColdTail) {
        unmarkTail(*node);
      }
      return node;
    };
    // remove from warm tail if it is not empty. if empty, remove from warm.
    T* node = lru_.getList(LruType::WarmTail).size() > 0
                  ? popFrom(LruType::WarmTail)
                  : popFrom(LruType::Warm);
    XDCHECK(isWarm(*node));
    lru_.getList(LruType::Cold).linkAtHead(*node);
    markCold(*node);
  }

  // shrink Hot if its size is larger than expected
  expectedSize = config_.hotSizePercent * lru_.size() / 100;
  while (lru_.getList(LruType::Hot).size() > expectedSize) {
    auto node = lru_.getList(LruType::Hot).getTail();
    XDCHECK(node);
    XDCHECK(isHot(*node));
    lru_.getList(LruType::Hot).remove(*node);
    lru_.getList(LruType::Cold).linkAtHead(*node);
    unmarkHot(*node);
    markCold(*node);
  }

  // adjust tail sizes for Cold and Warm
  adjustTail(LruType::Cold);
  adjustTail(LruType::Warm);
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
bool MM2Q::Container<T, HookPtr>::add(T& node) noexcept {
  const auto currTime = static_cast<Time>(util::getCurrentTimeSec());
  LockHolder l(lruMutex_);
  ++numLockByInserts_;
  if (node.isInMMContainer()) {
    return false;
  }

  markHot(node);
  unmarkCold(node);
  unmarkTail(node);
  lru_.getList(LruType::Hot).linkAtHead(node);
  rebalance();

  node.markInMMContainer();
  setUpdateTime(node, currTime);
  return true;
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
typename MM2Q::Container<T, HookPtr>::Iterator
MM2Q::Container<T, HookPtr>::getEvictionIterator() const noexcept {
  LockHolder l(lruMutex_);
  return Iterator{std::move(l), lru_.rbegin()};
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
void MM2Q::Container<T, HookPtr>::removeLocked(T& node,
                                               bool doRebalance) noexcept {
  LruType type = getLruType(node);
  lru_.getList(type).remove(node);
  if (doRebalance) {
    rebalance();
  }

  node.unmarkInMMContainer();
  return;
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
void MM2Q::Container<T, HookPtr>::setConfig(const Config& newConfig) {
  if (!tailTrackingEnabled_ && newConfig.tailSize > 0) {
    throw std::invalid_argument(
        "Cannot turn on tailHitsTracking (cache drop needed)");
  }
  if (tailTrackingEnabled_ && !newConfig.tailSize) {
    throw std::invalid_argument(
        "Cannot turn off tailHitsTracking (cache drop needed)");
  }
  LockHolder l(lruMutex_);
  config_ = newConfig;
  nextReconfigureTime_ = config_.mmReconfigureIntervalSecs.count() == 0
                             ? std::numeric_limits<Time>::max()
                             : static_cast<Time>(util::getCurrentTimeSec()) +
                                   config_.mmReconfigureIntervalSecs.count();
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
typename MM2Q::Config MM2Q::Container<T, HookPtr>::getConfig() const {
  LockHolder l(lruMutex_);
  return config_;
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
bool MM2Q::Container<T, HookPtr>::remove(T& node) noexcept {
  LockHolder l(lruMutex_);
  ++numLockByRemoves_;
  if (!node.isInMMContainer()) {
    return false;
  }
  removeLocked(node);
  return true;
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
void MM2Q::Container<T, HookPtr>::remove(Iterator& it) noexcept {
  T& node = *it;
  XDCHECK(node.isInMMContainer());
  ++it;
  // rebalance should not be triggered inside this remove, because changing the
  // queues while having the EvictionIterator can cause inconsistency problem
  // for the iterator. Also, this remove is followed by an insertion into the
  // same container, which will trigger rebalance.
  removeLocked(node, /* doRebalance = */ false);
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
bool MM2Q::Container<T, HookPtr>::replace(T& oldNode, T& newNode) noexcept {
  LockHolder l(lruMutex_);
  if (!oldNode.isInMMContainer() || newNode.isInMMContainer()) {
    return false;
  }
  const auto updateTime = getUpdateTime(oldNode);

  LruType type = getLruType(oldNode);
  lru_.getList(type).replace(oldNode, newNode);
  switch (type) {
  case LruType::Hot:
    markHot(newNode);
    break;
  case LruType::ColdTail:
    markTail(newNode); // pass through to also mark cold
  case LruType::Cold:
    markCold(newNode);
    break;
  case LruType::WarmTail:
    markTail(newNode);
    break; // warm is indicated by not marking hot or cold
  case LruType::Warm:
    break;
  case LruType::NumTypes:
    XDCHECK(false);
  }

  oldNode.unmarkInMMContainer();
  newNode.markInMMContainer();
  setUpdateTime(newNode, updateTime);
  return true;
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
MM2Q::LruType MM2Q::Container<T, HookPtr>::getTailLru(LruType list) const {
  switch (list) {
  case LruType::Warm:
    return LruType::WarmTail;
  case LruType::Cold:
    return LruType::ColdTail;
  default:
    XDCHECK(false);
    throw std::invalid_argument("The LRU list does not have a tail list");
  }
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
void MM2Q::Container<T, HookPtr>::adjustTail(LruType list) {
  auto tailList = getTailLru(list);
  auto ptr = lru_.getList(list).getTail();
  while (ptr && lru_.getList(tailList).size() + 1 <= config_.tailSize) {
    markTail(*ptr);
    lru_.getList(list).remove(*ptr);
    lru_.getList(tailList).linkAtHead(*ptr);
    ptr = lru_.getList(list).getTail();
  }
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
serialization::MM2QObject MM2Q::Container<T, HookPtr>::saveState() const
    noexcept {
  serialization::MM2QConfig configObject;
  configObject.lruRefreshTime = config_.lruRefreshTime;
  *configObject.lruRefreshRatio_ref() = config_.lruRefreshRatio;
  configObject.updateOnWrite = config_.updateOnWrite;
  *configObject.updateOnRead_ref() = config_.updateOnRead;
  configObject.hotSizePercent = config_.hotSizePercent;
  configObject.coldSizePercent = config_.coldSizePercent;
  *configObject.rebalanceOnRecordAccess_ref() = config_.rebalanceOnRecordAccess;

  serialization::MM2QObject object;
  object.config = configObject;
  *object.tailTrackingEnabled_ref() = tailTrackingEnabled_;
  object.lrus = lru_.saveState();
  return object;
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
MMContainerStat MM2Q::Container<T, HookPtr>::getStats() const noexcept {
  LockHolder l(lruMutex_);
  auto* tail = lru_.size() == 0 ? nullptr : lru_.rbegin().get();
  auto computeWeightedAccesses = [&](size_t warm, size_t cold) {
    return (warm * config_.getWarmSizePercent() +
            cold * config_.coldSizePercent) /
           100;
  };
  return {lru_.size(),
          tail == nullptr ? 0 : getUpdateTime(*tail),
          numLockByInserts_,
          numLockByRecordAccesses_,
          numLockByRemoves_,
          config_.lruRefreshTime,
          numHotAccesses_,
          numColdAccesses_,
          numWarmAccesses_,
          computeWeightedAccesses(numWarmTailAccesses_, numColdTailAccesses_)};
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
void MM2Q::Container<T, HookPtr>::reconfigureLocked(const Time& currTime) {
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
template <typename T, MM2Q::Hook<T> T::*HookPtr>
MM2Q::Container<T, HookPtr>::Iterator::Iterator(
    LockHolder l, const typename LruList::Iterator& iter) noexcept
    : LruList::Iterator(iter), l_(std::move(l)) {}
} // namespace cachelib
} // namespace facebook

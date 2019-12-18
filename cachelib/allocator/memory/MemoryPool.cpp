#include "cachelib/allocator/memory/MemoryPool.h"

#include "cachelib/allocator/memory/AllocationClass.h"
#include "cachelib/allocator/memory/SlabAllocator.h"

#include <algorithm>
#include <string>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#include <folly/Format.h>
#pragma GCC diagnostic pop

using namespace facebook::cachelib;
using LockHolder = std::unique_lock<std::mutex>;

/* static */
std::vector<uint32_t> MemoryPool::createMcSizesFromSerialized(
    const serialization::MemoryPoolObject& object) {
  std::vector<uint32_t> acSizes;
  for (auto size : object.acSizes) {
    acSizes.push_back(static_cast<uint32_t>(size));
  }
  return acSizes;
}

/* static */
MemoryPool::ACVector MemoryPool::createMcFromSerialized(
    const serialization::MemoryPoolObject& object,
    PoolId poolId,
    SlabAllocator& alloc) {
  MemoryPool::ACVector ac;
  for (const auto& allocClassObject : object.ac) {
    ac.emplace_back(new AllocationClass(allocClassObject, poolId, alloc));
  }
  return ac;
}

MemoryPool::MemoryPool(PoolId id,
                       size_t poolSize,
                       SlabAllocator& alloc,
                       const std::set<uint32_t>& allocSizes)
    : id_(id),
      maxSize_{poolSize},
      slabAllocator_(alloc),
      acSizes_(allocSizes.begin(), allocSizes.end()),
      ac_(createAllocationClasses()) {
  checkState();
}

MemoryPool::MemoryPool(const serialization::MemoryPoolObject& object,
                       SlabAllocator& alloc)
    : id_(object.id),
      maxSize_(object.maxSize),
      currSlabAllocSize_(object.currSlabAllocSize),
      currAllocSize_(object.currAllocSize),
      slabAllocator_(alloc),
      acSizes_(createMcSizesFromSerialized(object)),
      ac_(createMcFromSerialized(object, getId(), alloc)),
      curSlabsAdvised_{static_cast<uint64_t>(object.numSlabsAdvised)},
      nSlabResize_{static_cast<unsigned int>(object.numSlabResize)},
      nSlabRebalance_{static_cast<unsigned int>(object.numSlabRebalance)} {
  if (!slabAllocator_.isRestorable()) {
    throw std::logic_error(
        "Memory Pool can not be restored with this slab allocator");
  }

  for (auto freeSlabIdx : object.freeSlabIdxs) {
    freeSlabs_.push_back(slabAllocator_.getSlabForIdx(freeSlabIdx));
  }
  checkState();
}

void MemoryPool::checkState() const {
  if (id_ < 0) {
    throw std::invalid_argument(
        folly::sformat("Invaild MemoryPool id {}", id_));
  }

  const size_t currAlloc = currAllocSize_;
  const size_t currSlabAlloc = currSlabAllocSize_;
  if (currAlloc > currSlabAlloc) {
    throw std::invalid_argument(
        folly::sformat("Alloc size {} is more than total slab alloc size {}",
                       currAlloc,
                       currSlabAlloc));
  }

  if (acSizes_.size() == 0 || ac_.size() == 0) {
    throw std::invalid_argument("Empty alloc sizes");
  }

  if (acSizes_.size() != ac_.size()) {
    throw std::invalid_argument(folly::sformat(
        "Allocation classes are not setup correctly. acSize.size = {}, but "
        "ac.size() = {}",
        acSizes_.size(), ac_.size()));
  }

  if (!std::is_sorted(acSizes_.begin(), acSizes_.end())) {
    throw std::invalid_argument("Allocation sizes are not sorted.");
  }

  const auto firstDuplicate =
      std::adjacent_find(acSizes_.begin(), acSizes_.end());
  if (firstDuplicate != acSizes_.end()) {
    throw std::invalid_argument(
        folly::sformat("Duplicate allocation size: {}", *firstDuplicate));
  }

  for (size_t i = 0; i < acSizes_.size(); i++) {
    if (acSizes_[i] != ac_[i]->getAllocSize() ||
        acSizes_[i] < Slab::kMinAllocSize || acSizes_[i] > Slab::kSize) {
      throw std::invalid_argument(folly::sformat(
          "Allocation Class with id {} and size {}, does not match the "
          "allocation size we expect {}",
          ac_[i]->getId(), ac_[i]->getAllocSize(), acSizes_[i]));
    }
  }

  for (const auto slab : freeSlabs_) {
    if (!slabAllocator_.isValidSlab(slab)) {
      throw std::invalid_argument(folly::sformat("Invalid free slab {}", slab));
    }
  }
}

MemoryPool::ACVector MemoryPool::createAllocationClasses() const {
  ACVector ac;
  ClassId id = 0;
  for (const auto size : acSizes_) {
    if (size < Slab::kMinAllocSize || size > Slab::kSize) {
      throw std::invalid_argument(
          folly::sformat("Invalid allocation class size {}", size));
    }
    ac.emplace_back(new AllocationClass(id++, getId(), size, slabAllocator_));
  }
  XDCHECK(std::is_sorted(ac.begin(),
                         ac.end(),
                         [](const std::unique_ptr<AllocationClass>& a,
                            const std::unique_ptr<AllocationClass>& b) {
                           return a->getAllocSize() < b->getAllocSize();
                         }));

  return ac;
}

size_t MemoryPool::getCurrentUsedSize() const noexcept {
  LockHolder l(lock_);
  return currSlabAllocSize_ + freeSlabs_.size() * Slab::kSize;
}

AllocationClass& MemoryPool::getAllocationClassFor(uint32_t size) const {
  const auto classId = getAllocationClassId(size);
  return *ac_[classId];
}

AllocationClass& MemoryPool::getAllocationClassFor(void* memory) const {
  const auto classId = getAllocationClassId(memory);
  return *ac_[classId];
}

AllocationClass& MemoryPool::getAllocationClassFor(ClassId cid) const {
  if (cid < static_cast<ClassId>(ac_.size())) {
    XDCHECK(ac_[cid] != nullptr);
    return *ac_[cid];
  }
  throw std::invalid_argument(folly::sformat("Invalid classId {}", cid));
}

const AllocationClass& MemoryPool::getAllocationClass(ClassId cid) const {
  return getAllocationClassFor(cid);
}

ClassId MemoryPool::getAllocationClassId(uint32_t size) const {
  if (size > acSizes_.back() || size == 0) {
    throw std::invalid_argument(
        folly::sformat("Invalid size for alloc {} ", size));
  }

  // can operate without holding the mutex since the vector does not change.
  const auto it = std::lower_bound(acSizes_.begin(), acSizes_.end(), size);

  // we already checked for the bounds.
  XDCHECK(it != acSizes_.end());

  const auto idx = std::distance(acSizes_.begin(), it);
  XDCHECK_LT(static_cast<size_t>(idx), ac_.size());
  return static_cast<ClassId>(idx);
}

ClassId MemoryPool::getAllocationClassId(const void* memory) const {
  // find the slab for this allocation and the header for the slab. None of
  // the following needs to be serialized with the mutex.
  const auto* header = slabAllocator_.getSlabHeader(memory);

  // unallocated slab or slab not allocated to this pool or slab that is
  // allocated to this pool but not any allocation class is a failure.
  if (header == nullptr || header->poolId != id_) {
    throw std::invalid_argument(folly::sformat(
        "Memory {} [PoolId = {}] does not belong to this pool with id {}",
        memory,
        header ? header->poolId : Slab::kInvalidPoolId,
        id_));
  } else if (header->classId == Slab::kInvalidClassId) {
    throw std::invalid_argument("Memory does not belong to any valid class Id");
  }

  const auto classId = header->classId;
  if (classId >= static_cast<ClassId>(ac_.size()) || classId < 0) {
    // at this point, the slab indicates that it belongs to a bogus classId and
    // things are corrupt and the caller cant do anything about it. so throw an
    // exception to abort.
    throw std::runtime_error(folly::sformat(
        "corrupt slab header/memory pool with class id {}", classId));
  }

  return classId;
}

Slab* MemoryPool::getSlab() noexcept {
  {
    // atomically see if we can acquire a slab by checking if we have
    // reached the limit by size. If not, then they can be acquired from
    // either the slab allocator or our free list. It is important to check
    // this before we grab it from the slab allocator or free list. Things
    // that release slab, bump down the currSlabAllocSize_ after actually
    // releasing and adding it to free list or slab allocator.
    if (allSlabsAllocated()) {
      return nullptr;
    }

    LockHolder l(lock_);

    // check again after getting the lock.
    if (allSlabsAllocated()) {
      return nullptr;
    }

    // increment the size under lock to serialize. This ensures that only one
    // thread can possibly go above the limit and fetch it from free list or
    // the slab allocator. If we dont get it from the free list or slab
    // allocator, we bump it down.
    currSlabAllocSize_ += Slab::kSize;

    if (!freeSlabs_.empty()) {
      auto slab = freeSlabs_.back();
      freeSlabs_.pop_back();
      return slab;
    }
  }

  auto slab = slabAllocator_.makeNewSlab(id_);
  // if slab allocator failed to allocate, decrement the size.
  if (slab == nullptr) {
    currSlabAllocSize_ -= Slab::kSize;
  }
  return slab;
}

void* MemoryPool::allocate(uint32_t size) {
  auto& ac = getAllocationClassFor(size);

  auto alloc = ac.allocate();
  const auto allocSize = ac.getAllocSize();
  XDCHECK_GE(allocSize, size);

  if (alloc != nullptr) {
    currAllocSize_ += allocSize;
    return alloc;
  }

  // see if we have a slab to add to the allocation class.
  auto slab = getSlab();
  if (slab == nullptr) {
    // out of memory
    return nullptr;
  }

  // add it to the allocation class and try to allocate.
  alloc = ac.addSlabAndAllocate(slab);
  XDCHECK_NE(nullptr, alloc);

  currAllocSize_ += allocSize;
  return alloc;
}

void* MemoryPool::allocateZeroedSlab() { return allocate(Slab::kSize); }

void MemoryPool::free(void* alloc) {
  auto& ac = getAllocationClassFor(alloc);
  ac.free(alloc);
  currAllocSize_ -= ac.getAllocSize();
}

serialization::MemoryPoolObject MemoryPool::saveState() const {
  if (!slabAllocator_.isRestorable()) {
    throw std::logic_error("Memory Pool can not be restored");
  }

  serialization::MemoryPoolObject object;

  object.id = id_;
  object.maxSize = maxSize_;
  object.currSlabAllocSize = currSlabAllocSize_;
  object.currAllocSize = currAllocSize_;

  for (auto slab : freeSlabs_) {
    object.freeSlabIdxs.push_back(slabAllocator_.slabIdx(slab));
  }

  for (auto size : acSizes_) {
    object.acSizes.push_back(size);
  }

  for (const std::unique_ptr<AllocationClass>& allocClass : ac_) {
    object.ac.push_back(allocClass->saveState());
  }

  object.numSlabResize = nSlabResize_;
  object.numSlabRebalance = nSlabRebalance_;
  object.numSlabsAdvised = curSlabsAdvised_;

  return object;
}

void MemoryPool::releaseSlab(SlabReleaseMode mode,
                             const Slab* slab,
                             bool zeroOnRelease,
                             ClassId receiverClassId) {
  if (zeroOnRelease) {
    memset(slab->memoryAtOffset(0), 0, Slab::kSize);
  }

  // If we are doing a resize, we need to release the slab back to the
  // allocator since the pool is being resized. But, if we are doing a
  // rebalance, we are resizing the allocation classes in the pool. Hence we
  // need to retain the slabs within the pool.
  switch (mode) {
  case SlabReleaseMode::kResize:
    slabAllocator_.freeSlab(const_cast<Slab*>(slab));
    // decrement after actually releasing the slab.
    currSlabAllocSize_ -= Slab::kSize;
    ++nSlabResize_;
    break;

  case SlabReleaseMode::kAdvise:
    if (slabAllocator_.adviseSlab(const_cast<Slab*>(slab))) {
      ++curSlabsAdvised_;
    } else {
      LockHolder l(lock_);
      freeSlabs_.push_back(const_cast<Slab*>(slab));
    }
    currSlabAllocSize_ -= Slab::kSize;
    break;

  case SlabReleaseMode::kRebalance:
    if (receiverClassId != Slab::kInvalidClassId) {
      // Pool's current size does not change since this slab is
      // given to another allocation class within the same pool
      auto& receiverAC = getAllocationClassFor(receiverClassId);
      receiverAC.addSlab(const_cast<Slab*>(slab));
    } else {
      {
        LockHolder l(lock_);
        freeSlabs_.push_back(const_cast<Slab*>(slab));
      }

      // decrememnt after adding to the free list and not before. This ensures
      // that threads which observe the result of this atomic can always grab
      // it from the free list.
      currSlabAllocSize_ -= Slab::kSize;
    }
    ++nSlabRebalance_;
    break;
  }
}

size_t MemoryPool::reclaimSlabsAndGrow(size_t numSlabs) {
  unsigned int numReclaimed = 0;
  for (size_t i = 0; i < numSlabs; i++) {
    auto slab = slabAllocator_.reclaimSlab(id_);
    if (!slab) {
      break;
    }
    XDCHECK(slabAllocator_.getSlabHeader(slab)->poolId == getId());
    LockHolder l(lock_);
    freeSlabs_.push_back(slab);
    --curSlabsAdvised_;
    ++numReclaimed;
  }
  return numReclaimed;
}

SlabReleaseContext MemoryPool::releaseFromFreeSlabs() {
  LockHolder l(lock_);
  if (freeSlabs_.empty()) {
    throw std::invalid_argument(
        "Pool does not have any free slabs outside of allocation class ");
  }

  auto slab = freeSlabs_.back();
  freeSlabs_.pop_back();
  return SlabReleaseContext{slab, id_, Slab::kInvalidClassId,
                            SlabReleaseMode::kResize};
}

SlabReleaseContext MemoryPool::startSlabRelease(
    ClassId victim,
    ClassId receiver,
    SlabReleaseMode mode,
    const void* hint,
    bool zeroOnRelease,
    SlabReleaseAbortFn shouldAbortFn) {
  if (receiver != Slab::kInvalidClassId &&
      mode != SlabReleaseMode::kRebalance) {
    throw std::invalid_argument(folly::sformat(
        "A valid receiver {} is specified but the rebalancing mode is "
        "not SlabReleaseMode::kRebalance",
        receiver));
  }

  if (victim == Slab::kInvalidClassId && mode != SlabReleaseMode::kResize) {
    throw std::invalid_argument(
        "can not obtain from free slab pool when not using resizing mode");
  }

  auto context = victim == Slab::kInvalidClassId
                     ? releaseFromFreeSlabs()
                     : getAllocationClassFor(victim).startSlabRelease(
                           mode, hint, shouldAbortFn);
  context.setReceiver(receiver);
  context.setZeroOnRelease(zeroOnRelease);

  // if the context is already in the released state, add it to the free
  // slabs. the caller should not have to call completeSlabRelease()
  if (context.isReleased()) {
    XDCHECK(context.getActiveAllocations().empty());
    // The caller does not need to call completeSlabRelease.
    releaseSlab(context.getMode(), context.getSlab(), zeroOnRelease, receiver);
  }
  return context;
}

void MemoryPool::abortSlabRelease(const SlabReleaseContext& context) {
  auto classId = context.getClassId();
  auto& allocClass = getAllocationClassFor(classId);

  // abort the slab release process.
  allocClass.abortSlabRelease(context);
  nSlabReleaseAborted_++;
}

void MemoryPool::completeSlabRelease(const SlabReleaseContext& context) {
  if (context.isReleased()) {
    // the slab release is already completed.
    return;
  }

  if (context.getReceiverClassId() != Slab::kInvalidClassId &&
      context.getMode() != SlabReleaseMode::kRebalance) {
    throw std::invalid_argument(folly::sformat(
        "A valid receiver {} is specified but the rebalancing mode is "
        "not SlabReleaseMode::kRebalance",
        context.getReceiverClassId()));
  }

  auto slab = context.getSlab();
  auto mode = context.getMode();
  auto classId = context.getClassId();
  auto zeroOnRelease = context.shouldZeroOnRelease();
  auto& allocClass = getAllocationClassFor(classId);

  // complete the slab release process.
  allocClass.completeSlabRelease(context);

  XDCHECK_EQ(slabAllocator_.getSlabHeader(slab)->poolId, getId());
  XDCHECK_EQ(slabAllocator_.getSlabHeader(slab)->classId,
             Slab::kInvalidClassId);
  XDCHECK_EQ(slabAllocator_.getSlabHeader(slab)->allocSize, 0u);

  releaseSlab(mode, slab, zeroOnRelease, context.getReceiverClassId());
}

MPStats MemoryPool::getStats() const {
  LockHolder l(lock_);
  std::unordered_map<ClassId, ACStats> acStats;
  std::set<ClassId> classIds;
  for (const auto& ac : ac_) {
    acStats.insert({ac->getId(), ac->getStats()});
    classIds.insert(ac->getId());
  }

  const auto availableMemory = getUnAllocatedSlabMemory();
  const auto slabsUnAllocated =
      availableMemory > 0 ? availableMemory / Slab::kSize - freeSlabs_.size()
                          : 0;
  return MPStats{std::move(classIds), std::move(acStats), freeSlabs_.size(),
                 slabsUnAllocated,    nSlabResize_,       nSlabRebalance_,
                 curSlabsAdvised_};
}

unsigned int MemoryPool::provision() {
  unsigned int required = 0;
  for (auto& ac : ac_) {
    // does not need provisioning for this allocation class.
    if (ac->getNumSlabs() > 0) {
      continue;
    }
    // see if we have a slab to add to the allocation class.
    auto slab = getSlab();
    if (slab) {
      ac->addSlab(slab);
    } else {
      required++;
    }
  }

  return required;
}

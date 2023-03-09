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

#pragma once

#include <folly/CPortability.h>
#include <folly/Likely.h>
#include <folly/logging/xlog.h>
#include <folly/portability/Asm.h>

#include <cstdint>
#include <cstdlib>
#include <limits>
#include <stdexcept>
#include <type_traits>

#include "cachelib/common/CompilerUtils.h"
#include "cachelib/common/Exceptions.h"

namespace facebook {
namespace cachelib {
// refcount and flag in the CacheItem.
class FOLLY_PACK_ATTR RefcountWithFlags {
 public:
  /**
   * Layout of refcount type. This includes the flags, admin ref bits, and
   * refcount. Admin ref encodes special reference counts that contain
   * information on who holds the reference count (like AccessContainer or
   * MMContainer). Flags encode item state such as whether or not it's
   * a chained item.
   *
   * The layout is as follows.
   * |-- flags --|-- admin ref --|-- access ref--|
   * - Flags:  11 bits
   * - Admin Ref: 3 bits
   * - Access Ref: 18 bits
   */
  using Value = uint32_t;
  static_assert(std::is_unsigned<Value>::value,
                "Unsigned Integral type required");

  static constexpr uint8_t kNumFlags = 11;
  static constexpr uint8_t kNumAdminRefBits = 3;
  static constexpr uint8_t kNumAccessRefBits = 18;
  static_assert(kNumAccessRefBits <= NumBits<Value>::value, "Invalid type");
  static_assert(kNumAccessRefBits >= 1, "Need at least one bit for refcount");
  static_assert(
      NumBits<Value>::value == kNumFlags + kNumAdminRefBits + kNumAccessRefBits,
      "Value must be exactly the number of all the bits added together.");
  static constexpr Value kAccessRefMask = std::numeric_limits<Value>::max() >>
                                          (NumBits<Value>::value -
                                           kNumAccessRefBits);
  static constexpr Value kRefMask = std::numeric_limits<Value>::max() >>
                                    (NumBits<Value>::value - kNumAdminRefBits -
                                     kNumAccessRefBits);

  /**
   * Access reference counts indicate wthere there are an outstanding
   * references, but not who owns it. These control bits are special refcounts
   * that enable Cache operations to infer who owns a reference count and  take
   * appropriate action in the lifetime management of the Item within the cache.
   */
  enum AdminRef {
    // is linked through memory container
    kLinked = kNumAccessRefBits,

    // exists in hash table
    kAccessible,

    // this flag indicates the allocation is being evicted or moved elsewhere
    // (can be triggered by a resize, rebalance or normal eviction operation)
    kExclusive,
  };

  /**
   * Flags indicating item state. These are not used in the eviction or moving
   * synchronization. They are used to indicate what sort of item this is in
   * cache operations. For example, kNvmClean == false is the initial state of
   * an item in ram cache. It means we'll need to insert it into the NVM cache.
   * When we load an item from NVM cache, kNvmClean is set to true. However,
   * after user mutates the content, they will invalidate the copy of the item
   * in NVM cache and sets the clean bit to false.
   */
  enum Flags {
    // 3 bits reserved for MMContainer
    kMMFlag0 = kNumAccessRefBits + kNumAdminRefBits,
    kMMFlag1,
    kMMFlag2,

    // Whether or not an item is a regular item or chained alloc
    kIsChainedItem,

    // If a regular item has chained allocs
    kHasChainedItem,

    // Item hasn't been modified after loading from nvm
    kNvmClean,

    // Item was evicted from NVM while it was in RAM.
    kNvmEvicted,

    // A deprecated and noop flag that was used to mark whether the item is
    // unevictable in the past.
    kUnevictable_NOOP,

    // Unused. This is just to indciate the maximum number of flags
    kFlagMax,
  };
  static_assert(static_cast<uint8_t>(kMMFlag0) >
                    static_cast<uint8_t>(kExclusive),
                "Flags and control bits cannot overlap in bit range.");
  static_assert(kFlagMax <= NumBits<Value>::value, "Too many flags.");

  constexpr explicit RefcountWithFlags() = default;

  RefcountWithFlags(const RefcountWithFlags&) = delete;
  RefcountWithFlags& operator=(const RefcountWithFlags&) = delete;
  RefcountWithFlags(RefcountWithFlags&&) = delete;
  RefcountWithFlags& operator=(RefcountWithFlags&&) = delete;

  // Bumps up the reference count only if the new count will be strictly less
  // than or equal to the maxCount and the item is not exclusive
  // @return true if refcount is bumped. false otherwise (if item is exclusive)
  // @throw  exception::RefcountOverflow if new count would be greater than
  // maxCount
  FOLLY_ALWAYS_INLINE bool incRef() {
    auto predicate = [](const Value curValue) {
      Value bitMask = getAdminRef<kExclusive>();

      const bool exlusiveBitIsSet = curValue & bitMask;
      if (UNLIKELY((curValue & kAccessRefMask) == (kAccessRefMask))) {
        throw exception::RefcountOverflow("Refcount maxed out.");
      }

      // Check if the item is not marked for eviction
      return !exlusiveBitIsSet || ((curValue & kAccessRefMask) != 0);
    };

    auto newValue = [](const Value curValue) {
      return (curValue + static_cast<Value>(1));
    };

    return atomicUpdateValue(predicate, newValue);
  }

  // Bumps down the reference count
  //
  // @return Refcount with control bits. When it is zero, we know for
  //         certain no one has access to it.
  // @throw  RefcountUnderflow when we are trying to decremenet from 0
  //         refcount and have a refcount leak.
  FOLLY_ALWAYS_INLINE Value decRef() {
    auto predicate = [](const Value curValue) {
      if ((curValue & kAccessRefMask) == 0) {
        throw exception::RefcountUnderflow(
            "Trying to decRef with no refcount. RefCount Leak!");
      }
      return true;
    };

    Value retValue;
    auto newValue = [&retValue](const Value curValue) {
      retValue = (curValue - static_cast<Value>(1));
      return retValue;
    };

    auto updated = atomicUpdateValue(predicate, newValue);
    XDCHECK(updated);

    return retValue & kRefMask;
  }

  // Return refcount excluding moving refcount, control bits and flags.
  Value getAccessRef() const noexcept {
    auto raw = getRaw();
    auto accessRef = raw & kAccessRefMask;

    if ((raw & getAdminRef<kExclusive>()) && accessRef >= 1) {
      // if item is moving, ignore the extra ref
      return accessRef - static_cast<Value>(1);
    } else {
      return accessRef;
    }
  }

  // Return access ref and the admin ref bits
  Value getRefWithAccessAndAdmin() const noexcept {
    return getRaw() & kRefMask;
  }

  // Returns the raw refcount with control bits and flags
  Value getRaw() const noexcept {
    // This used to be just 'return refCount_'.
    // On intel it's the same as an atomic load on primitive types,
    // but it's better to be explicit and do an atomic load with
    // relaxed ordering.
    return __atomic_load_n(&refCount_, __ATOMIC_RELAXED);
  }

  /**
   * The following three functions correspond to the state of the allocation
   * in the memory management container. This is protected by the
   * MMContainer's internal locking. Inspecting this outside the mm
   * container will be racy.
   */
  void markInMMContainer() noexcept {
    Value bitMask = getAdminRef<kLinked>();
    __atomic_or_fetch(&refCount_, bitMask, __ATOMIC_ACQ_REL);
  }
  void unmarkInMMContainer() noexcept {
    Value bitMask = ~getAdminRef<kLinked>();
    __atomic_and_fetch(&refCount_, bitMask, __ATOMIC_ACQ_REL);
  }
  bool isInMMContainer() const noexcept {
    return getRaw() & getAdminRef<kLinked>();
  }

  /**
   * The following three functions correspond to the state of the allocation
   * in the access container. This will be protected by the access container
   * lock. Depending on their state outside of the access container might be
   * racy
   */
  void markAccessible() noexcept {
    Value bitMask = getAdminRef<kAccessible>();
    __atomic_or_fetch(&refCount_, bitMask, __ATOMIC_ACQ_REL);
  }
  void unmarkAccessible() noexcept {
    Value bitMask = ~getAdminRef<kAccessible>();
    __atomic_and_fetch(&refCount_, bitMask, __ATOMIC_ACQ_REL);
  }
  bool isAccessible() const noexcept {
    return getRaw() & getAdminRef<kAccessible>();
  }

  /**
   * The following two functions correspond to whether or not an item is
   * currently in the process of being evicted.
   *
   * An item that is marked for eviction prevents from obtaining a handle to
   * the item (incRef() will return false). This guarantees that eviction of
   * marked item will always suceed.
   *
   * An item can only be marked for eviction when `isInMMContainer` returns true
   * and item does not have `kExclusive` bit set and access ref count is 0.
   * This operation is atomic.
   *
   * When item is marked for  eviction, `kExclusive` bit is set and ref count is
   * zero.
   *
   * Unmarking for eviction clears the `kExclusive` bit. `unamrkForEviction`
   * does not depend on `isInMMContainer` nor `isAccessible`
   */
  bool markForEviction() noexcept {
    Value linkedBitMask = getAdminRef<kLinked>();
    Value exclusiveBitMask = getAdminRef<kExclusive>();

    auto predicate = [linkedBitMask, exclusiveBitMask](const Value curValue) {
      const bool unlinked = !(curValue & linkedBitMask);
      const bool alreadyExclusive = curValue & exclusiveBitMask;

      if (unlinked || alreadyExclusive) {
        return false;
      }
      if ((curValue & kAccessRefMask) != 0) {
        return false;
      }

      return true;
    };

    auto newValue = [exclusiveBitMask](const Value curValue) {
      return curValue | exclusiveBitMask;
    };

    return atomicUpdateValue(predicate, newValue);
  }

  Value unmarkForEviction() noexcept {
    XDCHECK(isMarkedForEviction());
    Value bitMask = ~getAdminRef<kExclusive>();
    return __atomic_and_fetch(&refCount_, bitMask, __ATOMIC_ACQ_REL) & kRefMask;
  }

  bool isMarkedForEviction() const noexcept {
    auto raw = getRaw();
    return (raw & getAdminRef<kExclusive>()) && ((raw & kAccessRefMask) == 0);
  }

  /**
   * The following functions correspond to whether or not an item is
   * currently in the processed of being moved.
   *
   * A `moving` item cannot be recycled nor freed to the allocator. It has
   * to be unmarked first.
   *
   * When moving, internal ref count is always >= 1 and `kExclusive` bit is set
   * getRefCount does not return the extra ref (it may return 0).
   *
   * An item can only be marked moving when `isInMMContainer` returns true
   * and does not have `kExclusive` bit set.
   *
   * User can also query if an item "isOnlyMoving". This returns true only
   * if the refcount is one and only the exlusive bit is set.
   *
   * Unmarking clears `kExclusive` bit and decreses the interanl refCount by 1.
   * `unmarkMoving` does does not depend on `isInMMContainer`
   */
  bool markMoving() {
    Value linkedBitMask = getAdminRef<kLinked>();
    Value exclusiveBitMask = getAdminRef<kExclusive>();

    auto predicate = [linkedBitMask, exclusiveBitMask](const Value curValue) {
      const bool unlinked = !(curValue & linkedBitMask);
      const bool alreadyExclusive = curValue & exclusiveBitMask;

      if (unlinked || alreadyExclusive) {
        return false;
      }
      if (UNLIKELY((curValue & kAccessRefMask) == (kAccessRefMask))) {
        throw exception::RefcountOverflow("Refcount maxed out.");
      }

      return true;
    };

    auto newValue = [exclusiveBitMask](const Value curValue) {
      // Set exclusive flag and make the ref count non-zero (to distinguish
      // from exclusive case). This extra ref will not be reported to the
      // user
      return (curValue + static_cast<Value>(1)) | exclusiveBitMask;
    };

    return atomicUpdateValue(predicate, newValue);
  }

  Value unmarkMoving() noexcept {
    XDCHECK(isMoving());
    auto predicate = [](const Value curValue) {
      XDCHECK((curValue & kAccessRefMask) != 0);
      return true;
    };

    Value retValue;
    auto newValue = [&retValue](const Value curValue) {
      retValue =
          (curValue - static_cast<Value>(1)) & ~getAdminRef<kExclusive>();
      return retValue;
    };

    auto updated = atomicUpdateValue(predicate, newValue);
    XDCHECK(updated);

    return retValue & kRefMask;
  }

  bool isMoving() const noexcept {
    auto raw = getRaw();
    return (raw & getAdminRef<kExclusive>()) && ((raw & kAccessRefMask) != 0);
  }

  /**
   * This function attempts to mark item for eviction.
   * Can only be called on the item that is moving.
   *
   * Returns true and marks the item for eviction only if item isOnlyMoving.
   * Leaves the item marked as moving and returns false otherwise.
   */
  bool markForEvictionWhenMoving() {
    XDCHECK(isMoving());

    auto predicate = [](const Value curValue) {
      return (curValue & kAccessRefMask) == 1;
    };

    auto newValue = [](const Value curValue) {
      XDCHECK((curValue & kAccessRefMask) == 1);
      return (curValue - static_cast<Value>(1));
    };

    return atomicUpdateValue(predicate, newValue);
  }

  bool isOnlyMoving() const noexcept {
    // An item is only moving when its refcount is one and only the exclusive
    // bit among all the control bits is set. This indicates an item is already
    // on its way out of cache.
    auto ref = getRefWithAccessAndAdmin();
    Value valueWithoutExclusiveBit = ref & ~getAdminRef<kExclusive>();
    if (valueWithoutExclusiveBit != 1) {
      return false;
    }
    return ref & getAdminRef<kExclusive>();
  }

  /**
   * Item cannot be marked both chained allocation and
   * marked as having chained allocations at the same time
   */
  void markIsChainedItem() noexcept { setFlag<kIsChainedItem>(); }
  void unmarkIsChainedItem() noexcept { unSetFlag<kIsChainedItem>(); }
  bool isChainedItem() const noexcept { return isFlagSet<kIsChainedItem>(); }
  void markHasChainedItem() noexcept { setFlag<kHasChainedItem>(); }
  void unmarkHasChainedItem() noexcept { unSetFlag<kHasChainedItem>(); }
  bool hasChainedItem() const noexcept { return isFlagSet<kHasChainedItem>(); }

  /**
   * Keep track of whether the item was modified while in ram cache
   */
  void markNvmClean() noexcept { return setFlag<kNvmClean>(); }
  void unmarkNvmClean() noexcept { return unSetFlag<kNvmClean>(); }
  bool isNvmClean() const noexcept { return isFlagSet<kNvmClean>(); }

  /**
   * Marks that the item was potentially evicted from the nvmcache and might
   * need to be rewritten even if it was nvm-clean
   */
  void markNvmEvicted() noexcept { return setFlag<kNvmEvicted>(); }
  void unmarkNvmEvicted() noexcept { return unSetFlag<kNvmEvicted>(); }
  bool isNvmEvicted() const noexcept { return isFlagSet<kNvmEvicted>(); }

  // Whether or not an item is completely drained of access
  // Refcount is 0 and the item is not linked, accessible, nor exclusive
  bool isDrained() const noexcept { return getRefWithAccessAndAdmin() == 0; }

  /**
   * Functions to set, unset and check flag presence. This is exposed
   * for external components to manipulate flags. E.g. MMContainer will
   * need to access reserved flags for MM.
   * We will not allow anyone to explicitly manipulate control bits via
   * these functions (compiler error).
   */
  template <Flags flagBit>
  void setFlag() noexcept {
    static_assert(flagBit >= kNumAccessRefBits + kNumAdminRefBits,
                  "incorrect flag");
    static_assert(flagBit < NumBits<Value>::value, "incorrect flag");
    constexpr Value bitMask = (static_cast<Value>(1) << flagBit);
    __atomic_or_fetch(&refCount_, bitMask, __ATOMIC_ACQ_REL);
  }
  template <Flags flagBit>
  void unSetFlag() noexcept {
    static_assert(flagBit >= kNumAccessRefBits + kNumAdminRefBits,
                  "incorrect flag");
    static_assert(flagBit < NumBits<Value>::value, "incorrect flag");
    constexpr Value bitMask =
        std::numeric_limits<Value>::max() - (static_cast<Value>(1) << flagBit);
    __atomic_and_fetch(&refCount_, bitMask, __ATOMIC_ACQ_REL);
  }
  template <Flags flagBit>
  bool isFlagSet() const noexcept {
    return getRaw() & getFlag<flagBit>();
  }

 private:
  /**
   * Helper function to modify refCount_ atomically.
   *
   * If predicate(currentValue) is true, then it atomically assigns result
   * of newValueF(currentValue) to refCount_ and returns true. Otherwise
   * returns false and leaves refCount_ unmodified.
   */
  template <typename P, typename F>
  bool atomicUpdateValue(P&& predicate, F&& newValueF) {
    Value* const refPtr = &refCount_;
    unsigned int nCASFailures = 0;
    constexpr bool isWeak = false;
    Value curValue = __atomic_load_n(refPtr, __ATOMIC_RELAXED);
    while (true) {
      if (!predicate(curValue)) {
        return false;
      }

      const Value newValue = newValueF(curValue);
      if (__atomic_compare_exchange_n(refPtr, &curValue, newValue, isWeak,
                                      __ATOMIC_ACQ_REL, __ATOMIC_RELAXED)) {
        return true;
      }

      if ((++nCASFailures % 4) == 0) {
        // this pause takes up to 40 clock cycles on intel and the lock cmpxchgl
        // above should take about 100 clock cycles. we pause once every 400
        // cycles or so if we are extremely unlucky.
        folly::asm_volatile_pause();
      }
    }
  }

  template <Flags flagBit>
  static Value getFlag() noexcept {
    static_assert(flagBit >= kNumAccessRefBits + kNumAdminRefBits,
                  "incorrect flag");
    static_assert(flagBit < NumBits<Value>::value, "incorrect flag");
    return static_cast<Value>(1) << flagBit;
  }

  template <AdminRef adminRef>
  static Value getAdminRef() noexcept {
    static_assert(adminRef >= kNumAccessRefBits, "incorrect control bit");
    static_assert(adminRef < kNumAccessRefBits + kNumAdminRefBits,
                  "incorrect control bit");
    return static_cast<Value>(1) << adminRef;
  }

  // why not use std::atomic? Because std::atomic does not work well with
  // padding and requires alignment.
  __attribute__((__aligned__(alignof(Value)))) Value refCount_{0};
};
} // namespace cachelib
} // namespace facebook

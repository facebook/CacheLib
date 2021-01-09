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
/* Provides refcounts that are atomic integrals. The refcount bit can be used
 * for two purposes. One to count the number of references and other, to mark
 * specific flags that are outside of the refcount range. The refcount range.
 * is specified through the template parameter RefCountBits.
 *
 * Why not std::atomic ?
 *
 * packing and aligning - we need this struct inside other packed structs
 * which forces us to use packed here with the native type and not
 * std::atomic. With packing, we get default alignment of 1 and that does not
 * work well with some compilers when they implement the __sync or __atomic
 * primitives. Hence we sprinkle the alignment. The user of this (CacheItem)
 * ensures that this is laid out in the right byte boundary and not misaligned
 * or straddled across cache line boundaries. Also, the penalty for misaligned
 * atomics on modern day processors (sandybridge and higher) is non-existent.
 * There is a penalty for atomics across cache lines and we avoid that by
 * ensuring that this does not straddle across cache lines.
 * https://fburl.com/hxztz462 for more recent discussion. If some day, gcc and
 * clang decide to support std::atomics and packing, we can dream about
 * replacing this with an std::atomic implementation.
 *
 * to play around with the generate assembly and resulting size, you can try
 * https://gcc.godbolt.org/z/dKgHF-
 */
template <typename T = uint16_t, uint8_t RefCountBits = NumBits<T>::value>
class __attribute__((__packed__)) RefCountWithFlags {
  static_assert(std::is_unsigned<T>::value, "Unsigned Integral type required");
  static_assert(RefCountBits <= NumBits<T>::value, "Invalid type");
  static_assert(RefCountBits >= 1, "Need at least one bit for refcount");

 private:
  static constexpr T kRefCountMask = std::numeric_limits<T>::max() >>
                                     (NumBits<T>::value - RefCountBits);

 public:
  using Value = T;

  // construct with initial refcount.
  constexpr explicit RefCountWithFlags(T startRefCount)
      : refCount_{startRefCount} {}

  // non copyable / non movable.
  RefCountWithFlags(const RefCountWithFlags&) = delete;
  RefCountWithFlags& operator=(const RefCountWithFlags&) = delete;
  RefCountWithFlags(RefCountWithFlags&&) = delete;
  RefCountWithFlags& operator=(RefCountWithFlags&&) = delete;

  // bumps up the reference count  only if the new count will be strictly less
  // than or equal to the maxCount.
  //
  // @param maxCount  the maxCount that the number of reference can reach.
  // @return  true if a reference was acquired. false if the reference count
  //          has maxed out.
  FOLLY_ALWAYS_INLINE bool inc(const T maxCount = kRefCountMask) noexcept;

  // bumps down the reference count and returns the new refcount in its raw
  // state.
  //
  // @throw RefcountUnderflow when we are trying to decremenet from 0
  // refcount and have a refcount leak.
  FOLLY_ALWAYS_INLINE T dec();

  // returns  the raw refcount. 0 means it is not referenced and released from
  // all containers.
  T getRaw() const noexcept { return refCount_; }

  // refcount of the actual number of accessors.
  T getCount() const noexcept { return refCount_ & kRefCountMask; }

  // sets the flagBit and returns the new value
  template <uint8_t flagBit>
  void setFlag() noexcept;

  // sets the flagBit conditional on another bit
  // return true if set successfully, false otherwise
  template <uint8_t flagBit, uint8_t conditionalFlagBit>
  bool setFlagConditional() noexcept;

  // unsets the flagBit and returns the new value
  template <uint8_t flagBit>
  T unsetFlag() noexcept;

  // checks if flagBit is set.
  template <uint8_t flagBit>
  bool isFlagSet() const noexcept;

  // checks if only the flagBit is set.
  template <uint8_t flagBit>
  bool isOnlyFlagSet() const noexcept;

  // reset the refcount to 0. This is not atomic.
  void resetNonAtomic() noexcept { refCount_ = 0; }

  static constexpr T getMaxRefCount() noexcept { return kRefCountMask; }

 private:
  // return the value corresponding to the flag.
  template <uint8_t flagBit>
  static constexpr T getFlagVal() noexcept;

  // why not use std::atomic ? because std::atomic does not work well with
  // padding and requires alignment. See the comment at the top of this class.
  __attribute__((__aligned__(alignof(T)))) T refCount_{0};
};
} // namespace cachelib
} // namespace facebook

#include "cachelib/allocator/Refcount-inl.h"

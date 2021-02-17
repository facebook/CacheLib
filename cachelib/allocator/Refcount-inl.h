
namespace facebook {
namespace cachelib {
template <typename T, uint8_t RefCountBits>
constexpr T RefCountWithFlags<T, RefCountBits>::kRefCountMask;

template <typename T, uint8_t RefCountBits>
bool RefCountWithFlags<T, RefCountBits>::inc(const T maxCount) noexcept {
  XDCHECK_LE(maxCount, kRefCountMask);

  T* const refPtr = &refCount_;
  unsigned int nCASFailures = 0;
  constexpr bool isWeak = false;
  T oldCount = *refPtr;

  while (true) {
    const T newCount = oldCount + static_cast<T>(1);
    if (UNLIKELY((oldCount & kRefCountMask) >= (maxCount & kRefCountMask))) {
      return false;
    }

    {
      // TODO(T83749172): Fix data race exposed by TSAN.
      folly::annotate_ignore_thread_sanitizer_guard g(__FILE__, __LINE__);
      if (__atomic_compare_exchange_n(refPtr, &oldCount, newCount, isWeak,
                                      __ATOMIC_RELEASE, __ATOMIC_ACQUIRE)) {
        return true;
      }
    }

    if ((++nCASFailures % 4) == 0) {
      // this pause takes up to 40 clock cycles on intel and the lock cmpxchgl
      // above should take about 100 clock cycles. we pause once every 400
      // cycles or so if we are extremely unlucky.
      folly::asm_volatile_pause();
    }
  }
}

template <typename T, uint8_t RefCountBits>
T RefCountWithFlags<T, RefCountBits>::dec() {
  T* const refPtr = &refCount_;
  unsigned int nCASFailures = 0;
  constexpr bool isWeak = false;

  T oldCount = *refPtr;
  while (true) {
    const T newCount = oldCount - static_cast<T>(1);
    if ((oldCount & kRefCountMask) == 0) {
      throw exception::RefcountUnderflow(
          "Trying to decRef with no refcount. RefCount Leak!");
    }

    {
      // TODO(T83749172): Fix data race exposed by TSAN.
      folly::annotate_ignore_thread_sanitizer_guard g(__FILE__, __LINE__);
      if (__atomic_compare_exchange_n(refPtr, &oldCount, newCount, isWeak,
                                      __ATOMIC_RELEASE, __ATOMIC_ACQUIRE)) {
        return newCount;
      }
    }
    if ((++nCASFailures % 4) == 0) {
      folly::asm_volatile_pause();
    }
  }
}

template <typename T, uint8_t RefCountBits>
template <uint8_t flagBit>
constexpr T RefCountWithFlags<T, RefCountBits>::getFlagVal() noexcept {
  static_assert(flagBit >= RefCountBits, "incorrect flag");
  static_assert(flagBit < NumBits<T>::value, "incorrect flag");
  return static_cast<T>(1) << flagBit;
}

template <typename T, uint8_t RefCountBits>
template <uint8_t flagBit>
bool RefCountWithFlags<T, RefCountBits>::isFlagSet() const noexcept {
  return refCount_ & getFlagVal<flagBit>();
}

template <typename T, uint8_t RefCountBits>
template <uint8_t flagBit>
bool RefCountWithFlags<T, RefCountBits>::isOnlyFlagSet() const noexcept {
  constexpr T bitMask = std::numeric_limits<T>::max() - (((T)1) << flagBit);
  const auto anyOtherBitSet = bitMask & getRaw();
  return anyOtherBitSet ? false : isFlagSet<flagBit>();
}

template <typename T, uint8_t RefCountBits>
template <uint8_t flagBit>
void RefCountWithFlags<T, RefCountBits>::setFlag() noexcept {
  static_assert(flagBit >= RefCountBits, "incorrect flag");
  static_assert(flagBit < NumBits<T>::value, "incorrect flag");
  constexpr T bitMask = (((T)1) << flagBit);
  // TODO(T83749172): Fix data race exposed by TSAN.
  folly::annotate_ignore_thread_sanitizer_guard g(__FILE__, __LINE__);
  __atomic_or_fetch(&refCount_, bitMask, __ATOMIC_ACQ_REL);
}

template <typename T, uint8_t RefCountBits>
template <uint8_t flagBit, uint8_t conditionalFlagBit>
bool RefCountWithFlags<T, RefCountBits>::setFlagConditional() noexcept {
  static_assert(flagBit >= RefCountBits, "incorrect flag");
  static_assert(flagBit < NumBits<T>::value, "incorrect flag");
  constexpr T bitMask = getFlagVal<flagBit>();
  constexpr T conditionBitMask = getFlagVal<conditionalFlagBit>();

  T* const refPtr = &refCount_;
  unsigned int nCASFailures = 0;
  constexpr bool isWeak = false;
  T curValue = *refPtr;
  while (true) {
    const bool flagSet = curValue & conditionBitMask;
    if (!flagSet) {
      return false;
    }

    const T newValue = curValue | bitMask;
    {
      // TODO(T83749172): Fix data race exposed by TSAN.
      folly::annotate_ignore_thread_sanitizer_guard g(__FILE__, __LINE__);
      if (__atomic_compare_exchange_n(refPtr, &curValue, newValue, isWeak,
                                      __ATOMIC_RELEASE, __ATOMIC_ACQUIRE)) {
        XDCHECK(newValue & conditionBitMask);
        return true;
      }
    }

    if ((++nCASFailures % 4) == 0) {
      // this pause takes up to 40 clock cycles on intel and the lock cmpxchgl
      // above should take about 100 clock cycles. we pause once every 400
      // cycles or so if we are extremely unlucky.
      folly::asm_volatile_pause();
    }
  }
}

template <typename T, uint8_t RefCountBits>
template <uint8_t flagBit>
T RefCountWithFlags<T, RefCountBits>::unsetFlag() noexcept {
  static_assert(flagBit >= RefCountBits, "incorrect flag");
  static_assert(flagBit < NumBits<T>::value, "incorrect flag");
  constexpr T bitMask = std::numeric_limits<T>::max() - (((T)1) << flagBit);
  // TODO(T83749172): Fix data race exposed by TSAN.
  folly::annotate_ignore_thread_sanitizer_guard g(__FILE__, __LINE__);
  return __atomic_and_fetch(&refCount_, bitMask, __ATOMIC_ACQ_REL);
}
} // namespace cachelib
} // namespace facebook

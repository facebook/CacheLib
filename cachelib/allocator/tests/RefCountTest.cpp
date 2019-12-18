#include <folly/Random.h>
#include <thread>
#include "TestBase.h"
#include "cachelib/allocator/Refcount.h"

namespace facebook {
namespace cachelib {
namespace tests {

namespace {
class RefCountTest : public AllocTestBase {
 public:
  // The tests all take the following templated arguments
  // T = width of the refcount
  // Bits = number of bits to be used for the count
  // FlagBit1 = bit for the first flag
  // FlabBit2 = bit for the second flag
  //
  // Each test has a corresponding  struct that enables it to be passed as a
  // template function to testFuncForAll
  template <typename T,
            uint8_t Bits,
            uint8_t FlagBit1,
            uint8_t FlagBit2,
            uint8_t FlagBit3>
  static void testOverflow();

  template <typename T, uint8_t Bits>
  static void testMaxCount();

  template <typename T,
            uint8_t Bits,
            uint8_t FlagBit1,
            uint8_t FlagBit2,
            uint8_t FlagBit3>
  static void testUnderflow();

  template <typename T,
            uint8_t Bits,
            uint8_t FlagBit1,
            uint8_t FlagBit2,
            uint8_t FlagBit3>
  static void testMultiThreaded();

  template <typename T,
            uint8_t Bits,
            uint8_t FlagBit1,
            uint8_t FlagBit2,
            uint8_t FlagBit3>
  static void testBasic();

  struct Overflow {
    template <typename T,
              int B,
              uint8_t FlagBit1 = B,
              uint8_t FlagBit2 = B,
              uint8_t FlagBit3 = B>
    static void func() {
      testOverflow<T, B, FlagBit1, FlagBit2, FlagBit3>();
    }
  };

  struct MaxCount {
    template <typename T,
              int B,
              uint8_t FlagBit1 = B,
              uint8_t FlagBit2 = B,
              uint8_t FlagBit3 = B>
    static void func() {
      testMaxCount<T, B>();
    }
  };

  struct Underflow {
    template <typename T,
              int B,
              uint8_t FlagBit1 = B,
              uint8_t FlagBit2 = B,
              uint8_t FlagBit3 = B>
    static void func() {
      testUnderflow<T, B, FlagBit1, FlagBit2, FlagBit3>();
    }
  };

  struct MultiThreaded {
    template <typename T,
              int B,
              uint8_t FlagBit1 = B,
              uint8_t FlagBit2 = B,
              uint8_t FlagBit3 = B>
    static void func() {
      testMultiThreaded<T, B, FlagBit1, FlagBit2, FlagBit3>();
    }
  };
  struct Basic {
    template <typename T,
              int B,
              uint8_t FlagBit1 = B,
              uint8_t FlagBit2 = B,
              uint8_t FlagBit3 = B>
    static void func() {
      testBasic<T, B, FlagBit1, FlagBit2, FlagBit3>();
    }
  };
};

template <typename F>
void testFuncForAll() {
  // 64 bit
  F::template func<uint64_t, 63>();
  F::template func<uint64_t, 62, 62, 63>();
  F::template func<uint64_t, 61, 62, 61>();
  F::template func<uint64_t, 31, 55, 31>();
  F::template func<uint64_t, 20, 43, 21>();
  F::template func<uint64_t, 2, 63, 2>();
  F::template func<uint64_t, 2, 63, 2, 62>();

  // 32 bit
  F::template func<uint32_t, 31>();
  F::template func<uint32_t, 20, 20, 21>();
  F::template func<uint32_t, 16, 20, 25>();
  F::template func<uint32_t, 15, 15, 20>();
  F::template func<uint32_t, 9, 9, 19>();
  F::template func<uint32_t, 4, 31, 5>();
  F::template func<uint32_t, 4, 31, 5, 6>();

  // 16 bit
  F::template func<uint16_t, 15>();
  F::template func<uint16_t, 14, 14, 15>();
  F::template func<uint16_t, 13, 14, 15>();
  F::template func<uint16_t, 12, 13, 15>();
  F::template func<uint16_t, 11, 11, 15>();
  F::template func<uint16_t, 10, 11, 15>();
  F::template func<uint16_t, 9, 9, 15>();
  F::template func<uint16_t, 8, 8, 15>();
  F::template func<uint16_t, 7, 7, 15>();
  F::template func<uint16_t, 6, 6, 15>();
  F::template func<uint16_t, 5, 5, 15>();
  F::template func<uint16_t, 4, 4, 15>();
  F::template func<uint16_t, 3, 3, 15>();
  F::template func<uint16_t, 2, 2, 15>();
  F::template func<uint16_t, 2, 2, 15, 14>();

  // 8 bit
  F::template func<uint8_t, 7>();
  F::template func<uint8_t, 6, 7, 6>();
  F::template func<uint8_t, 5, 7, 5>();
  F::template func<uint8_t, 4, 7, 4>();
  F::template func<uint8_t, 3, 7, 3>();
  F::template func<uint8_t, 2, 7, 2>();
  F::template func<uint8_t, 2, 7, 2, 3>();
}

// start with initial refcount and ensure that the going over the limit
// throws.
template <typename T,
          uint8_t Bits,
          uint8_t FlagBit1,
          uint8_t FlagBit2,
          uint8_t FlagBit3>
void RefCountTest::testOverflow() {
  T nBump = (1 << (std::max(1, Bits / 3))) - 1;
  const T refMax = RefCountWithFlags<T, Bits>::getMaxRefCount();
  T startCount = Bits > 16 ? refMax - nBump : 0;

  ASSERT_LT(startCount, refMax);

  RefCountWithFlags<T, Bits> ref{startCount};
  ASSERT_EQ(startCount, ref.getCount());
  while (ref.getCount() < refMax) {
    ref.inc();

    if (ref.getCount() % 7 == 0) {
      ref.template setFlag<FlagBit1>();
      ASSERT_TRUE(ref.template isFlagSet<FlagBit1>());
      ref.template setFlag<FlagBit2>();
      ASSERT_TRUE(ref.template isFlagSet<FlagBit2>());
      ref.template setFlag<FlagBit3>();
      ASSERT_TRUE(ref.template isFlagSet<FlagBit3>());
    } else if (ref.getCount() % 7 == 1) {
      ref.template unsetFlag<FlagBit1>();
      ASSERT_FALSE(ref.template isFlagSet<FlagBit1>());
      ref.template unsetFlag<FlagBit2>();
      ASSERT_FALSE(ref.template isFlagSet<FlagBit2>());
      ref.template unsetFlag<FlagBit3>();
      ASSERT_FALSE(ref.template isFlagSet<FlagBit3>());
    }
  }

  ASSERT_EQ(refMax, ref.getCount());
  ASSERT_FALSE(ref.inc());

  // trying to increment with any max value that we already exceeded should
  // fail.
  ASSERT_FALSE(ref.inc(ref.getCount()));
  ASSERT_FALSE(ref.inc(ref.getCount() - 1));

  // setting/unsetting flags should not affect this.
  ref.template setFlag<FlagBit1>();
  ASSERT_TRUE(ref.template isFlagSet<FlagBit1>());
  ref.template setFlag<FlagBit2>();
  ASSERT_TRUE(ref.template isFlagSet<FlagBit2>());
  ref.template setFlag<FlagBit3>();
  ASSERT_TRUE(ref.template isFlagSet<FlagBit3>());
  ASSERT_EQ(refMax, ref.getCount());
  ASSERT_LT(refMax, ref.getRaw());

  // it should throw even if the flags are set.
  ASSERT_FALSE(ref.inc());
}

// going below 0 should throw.
template <typename T,
          uint8_t Bits,
          uint8_t FlagBit1,
          uint8_t FlagBit2,
          uint8_t FlagBit3>
void RefCountTest::testUnderflow() {
  T nBump = (1 << (std::max(1, Bits / 3))) - 1;
  const T refMax = RefCountWithFlags<T, Bits>::getMaxRefCount();
  T startCount = Bits > 16 ? refMax - nBump : (1 << Bits) - 1;

  ASSERT_LT(startCount, refMax);

  RefCountWithFlags<T, Bits> ref{startCount};
  ASSERT_EQ(startCount, ref.getCount());
  while (ref.getCount() != 0) {
    ref.dec();

    if (ref.getCount() % 7 == 0) {
      ref.template setFlag<FlagBit1>();
      ASSERT_TRUE(ref.template isFlagSet<FlagBit1>());
      ref.template setFlag<FlagBit2>();
      ASSERT_TRUE(ref.template isFlagSet<FlagBit2>());
      ref.template setFlag<FlagBit3>();
      ASSERT_TRUE(ref.template isFlagSet<FlagBit3>());
    } else if (ref.getCount() % 7 == 1) {
      ref.template unsetFlag<FlagBit1>();
      ASSERT_FALSE(ref.template isFlagSet<FlagBit1>());
      ref.template unsetFlag<FlagBit2>();
      ASSERT_FALSE(ref.template isFlagSet<FlagBit2>());
      ref.template unsetFlag<FlagBit3>();
      ASSERT_FALSE(ref.template isFlagSet<FlagBit3>());
    }
  }

  ASSERT_EQ(0, ref.getCount());
  ASSERT_THROW(ref.dec(), std::underflow_error);

  // setting/unsetting flags should not affect this.
  ref.template setFlag<FlagBit1>();
  ASSERT_TRUE(ref.template isFlagSet<FlagBit1>());
  ref.template setFlag<FlagBit2>();
  ASSERT_TRUE(ref.template isFlagSet<FlagBit2>());
  ref.template setFlag<FlagBit3>();
  ASSERT_TRUE(ref.template isFlagSet<FlagBit3>());
  ASSERT_EQ(0, ref.getCount());
  ASSERT_NE(ref.getRaw(), 0);

  // error should be detected even with flags set/unset.
  ASSERT_THROW(ref.dec(), std::underflow_error);
}

template <typename T, uint8_t Bits>
void RefCountTest::testMaxCount() {
  T maxExpected = (Bits == NumBits<T>::value) ? std::numeric_limits<T>::max()
                                              : (((uint64_t)1) << Bits) - 1;
  const T maxCount = RefCountWithFlags<T, Bits>::getMaxRefCount();
  ASSERT_EQ(maxExpected, maxCount);
}

template <typename T,
          uint8_t Bits,
          uint8_t FlagBit1,
          uint8_t FlagBit2,
          uint8_t FlagBit3>
void RefCountTest::testMultiThreaded() {
  const T maxCount = RefCountWithFlags<T, Bits>::getMaxRefCount();
  T startCount = Bits > 25 ? maxCount - 1e6 : 0;
  RefCountWithFlags<T, Bits> ref{startCount};

  const T remaining = maxCount - startCount;

  ASSERT_EQ(startCount, ref.getCount());

  const unsigned int nThreads = 32;
  const T perThread = remaining / nThreads;

  auto doInThread = [&ref, perThread]() {
    // we have perThread number of references. we can try to bump it up and
    // bump it down randomly.
    unsigned int iter = 0;
    unsigned int nLocalRef = 0;
    while (nLocalRef < perThread) {
      if (iter++ % 3 == 0 && nLocalRef > 0) {
        ref.dec();
        nLocalRef--;
        ref.template setFlag<FlagBit1>();
      } else {
        ref.inc();
        nLocalRef++;
        ref.template unsetFlag<FlagBit1>();
      }

      if (nLocalRef % 10000) {
        std::this_thread::yield();
      }
    }
  };

  std::vector<std::thread> threads;
  for (unsigned int i = 0; i < nThreads; i++) {
    threads.emplace_back(doInThread);
  }

  for (auto& thread : threads) {
    if (thread.joinable()) {
      thread.join();
    }
  }
  ASSERT_EQ(startCount + perThread * nThreads, ref.getCount());
}

template <typename T,
          uint8_t Bits,
          uint8_t FlagBit1,
          uint8_t FlagBit2,
          uint8_t FlagBit3>
void RefCountTest::testBasic() {
  RefCountWithFlags<T, Bits> ref{0};
  ASSERT_EQ(0, ref.getCount());
  ASSERT_EQ(0, ref.getRaw());
  ASSERT_FALSE(ref.template isFlagSet<FlagBit1>());
  ASSERT_FALSE(ref.template isFlagSet<FlagBit2>());
  ASSERT_FALSE(ref.template isFlagSet<FlagBit3>());

  // set first flag and ensure second flag is not affected as long as flag1
  // and flag2 are  different.
  ref.template setFlag<FlagBit1>();
  ASSERT_TRUE(ref.template isFlagSet<FlagBit1>());
  ASSERT_TRUE(!ref.template isFlagSet<FlagBit2>() || FlagBit1 == FlagBit2);
  ASSERT_TRUE(!ref.template isFlagSet<FlagBit3>() || FlagBit1 == FlagBit3);

  for (unsigned long i = 0; i < ref.getMaxRefCount(); i++) {
    if (i > 1000) {
      break;
    }
    ref.inc();
  }

  // bumping up the refcount should not affect this.
  ASSERT_TRUE(ref.template isFlagSet<FlagBit1>());
  ASSERT_TRUE(!ref.template isFlagSet<FlagBit2>() || FlagBit1 == FlagBit2);
  ASSERT_TRUE(!ref.template isFlagSet<FlagBit3>() || FlagBit1 == FlagBit3);

  for (unsigned long i = 0; i < ref.getMaxRefCount(); i++) {
    if (i > 1000) {
      break;
    }
    ref.dec();
  }

  // bumping down the refcount should not affect this.
  ASSERT_TRUE(ref.template isFlagSet<FlagBit1>());
  ASSERT_TRUE(!ref.template isFlagSet<FlagBit2>() || FlagBit1 == FlagBit2);
  ASSERT_TRUE(!ref.template isFlagSet<FlagBit3>() || FlagBit1 == FlagBit3);

  ref.template unsetFlag<FlagBit1>();
  ASSERT_FALSE(ref.template isFlagSet<FlagBit1>());
  // setting or unsetting more than once should not affect.
  for (unsigned long i = 0; i < folly::Random::rand32() % 97 + 1; i++) {
    ref.template setFlag<FlagBit1>();
  }
  ASSERT_TRUE(ref.template isFlagSet<FlagBit1>());

  for (unsigned long i = 0; i < folly::Random::rand32() % 97 + 1; i++) {
    ref.template unsetFlag<FlagBit1>();
  }
  ASSERT_FALSE(ref.template isFlagSet<FlagBit1>());

  // conditionally set flags
  ASSERT_FALSE((ref.template setFlagConditional<FlagBit3, FlagBit1>()));
  ref.template setFlag<FlagBit1>();
  ASSERT_TRUE((ref.template setFlagConditional<FlagBit3, FlagBit1>()));
  ref.template unsetFlag<FlagBit1>();
  ASSERT_FALSE((ref.template setFlagConditional<FlagBit3, FlagBit1>()));
}
}

TEST_F(RefCountTest, MultiThreaded) { testFuncForAll<MultiThreaded>(); }
TEST_F(RefCountTest, Overflow) { testFuncForAll<Overflow>(); }
TEST_F(RefCountTest, Underflow) { testFuncForAll<Overflow>(); }
TEST_F(RefCountTest, MaxCount) { testFuncForAll<MaxCount>(); }
TEST_F(RefCountTest, Flags) { testFuncForAll<Basic>(); }
}
}
}

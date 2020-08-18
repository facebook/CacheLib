#include "cachelib/allocator/tests/BaseAllocatorTest.h"
#include "cachelib/allocator/tests/TestBase.h"

namespace facebook {
namespace cachelib {
namespace tests {

TYPED_TEST_CASE(BaseAllocatorTest, AllocatorTypes);

// test all the error scenarios with respect to allocating a new key.
TYPED_TEST(BaseAllocatorTest, AllocateAccessible) {
  this->testAllocateAccessible();
}

// Test adding and looking up a mix of evictable and unevictable items
TYPED_TEST(BaseAllocatorTest, MixedItems) { this->testMixedItems(); }

// fill up the memory and test that making further allocations causes
// evictions from the cache.
TYPED_TEST(BaseAllocatorTest, Evictions) { this->testEvictions(); }

// hold on to an item with active handle and ensure that we can still evict
// from the cache
TYPED_TEST(BaseAllocatorTest, EvictionsWithActiveHandles) {
  this->testEvictionsWithActiveHandles();
}

TYPED_TEST(BaseAllocatorTest, TestMMapFileAllocator) {
  this->testMMapFileAllocator();
}

// test the free callback, which should only be invoked once the item is removed
// and the last handle is dropped
TYPED_TEST(BaseAllocatorTest, Removals) { this->testRemovals(); }

// fill up one pool and ensure that memory can still be allocated from the
// other pool without evictions.
TYPED_TEST(BaseAllocatorTest, Pools) { this->testPools(); }

// make some allocations without evictions and ensure that we are able to
// fetch them.
TYPED_TEST(BaseAllocatorTest, Find) { this->testFind(); }

// make some allocations without evictions, remove them and ensure that they
// cannot be accessed through find.
TYPED_TEST(BaseAllocatorTest, Remove) { this->testRemove(); }

// trigger evictions and ensure that the eviction call back gets called.
TYPED_TEST(BaseAllocatorTest, RemoveCb) { this->testRemoveCb(); }

TYPED_TEST(BaseAllocatorTest, RemoveCbSlabReleaseMoving) {
  this->testRemoveCbSlabReleaseMoving();
}

TYPED_TEST(BaseAllocatorTest, RemoveCbSlabRelease) {
  this->testRemoveCbSlabRelease();
}

// fill up the pool with allocations and ensure that the evictions then cycle
// through the lru and the lru is fixed in length.
TYPED_TEST(BaseAllocatorTest, LruLength) { this->testTestLruLength(); }

TYPED_TEST(BaseAllocatorTest, AttachDetachOnExit) {
  this->testAttachDetachOnExit();
}

TYPED_TEST(BaseAllocatorTest, AttachWithDifferentCacheName) {
  this->testAttachWithDifferentName();
}

TYPED_TEST(BaseAllocatorTest, AttachWithLargerSizeSysV) {
  this->testAttachWithDifferentSize(false, false);
}

TYPED_TEST(BaseAllocatorTest, AttachWithLargerSizePosix) {
  this->testAttachWithDifferentSize(true, false);
}
TYPED_TEST(BaseAllocatorTest, AttachWithSmallerSizeSysV) {
  this->testAttachWithDifferentSize(false, true);
}

TYPED_TEST(BaseAllocatorTest, AttachWithSmallerSizePosix) {
  this->testAttachWithDifferentSize(true, true);
}

TYPED_TEST(BaseAllocatorTest, CleanupDirRemoved) {
  this->testCacheCleanupDirRemoved();
}

TYPED_TEST(BaseAllocatorTest, CleanupDirExists) {
  this->testCacheCleanupDirExists();
}

TYPED_TEST(BaseAllocatorTest, CleanupAttached) {
  this->testCacheCleanupAttached();
}

TYPED_TEST(BaseAllocatorTest, DropFile) { this->testDropFile(); }

TYPED_TEST(BaseAllocatorTest, ShmTemporary) { this->testShmTemporary(); }

TYPED_TEST(BaseAllocatorTest, Serialization) { this->testSerialization(); }

TYPED_TEST(BaseAllocatorTest, SerializationMMConfig) {
  this->testSerializationMMConfig();
}

TYPED_TEST(BaseAllocatorTest, testSerializationWithFragmentation) {
  this->testSerializationWithFragmentation();
}

// make some allocations and access them and record explicitly the time it was
// accessed. Ensure that the items that are evicted are descending in order of
// time. To ensure the lru property, lets only allocate objects of fixed size.
TYPED_TEST(BaseAllocatorTest, LruRecordAccess) { this->testLruRecordAccess(); }

TYPED_TEST(BaseAllocatorTest, ApplyAll) { this->testApplyAll(); }

TYPED_TEST(BaseAllocatorTest, IterateAndRemoveWithKey) {
  this->testIterateAndRemoveWithKey();
}

TYPED_TEST(BaseAllocatorTest, IterateAndRemoveWithIter) {
  this->testIterateAndRemoveWithIter();
}

TYPED_TEST(BaseAllocatorTest, IterateWithEvictions) {
  this->testIterateWithEvictions();
}

TYPED_TEST(BaseAllocatorTest, IOBufItemHandle) { this->testIOBufItemHandle(); }

TYPED_TEST(BaseAllocatorTest, IOBufItemHandleForChainedItems) {
  this->testIOBufItemHandleForChainedItems();
}

TYPED_TEST(BaseAllocatorTest, HandleTracking) { this->testHandleTracking(); }

TYPED_TEST(BaseAllocatorTest, TLHandleTracking) {
  this->testTLHandleTracking();
}

// ensure that when we call allocate and get an exception, we dont leak any
// memory. We do so by keeping track of the number of active allocations and
// ensuring that the number stays the same.
TYPED_TEST(BaseAllocatorTest, AllocException) { this->testAllocException(); }

// Fill up the allocator with items of the same size.
// Release a slab. Afterwards, ensure the allocator only has enough space
// for allocate for the same number of items as evicted by releasing the
// slab. Any more allocation should result in new items being evicted.
TYPED_TEST(BaseAllocatorTest, Rebalancing) { this->testRebalancing(); }

// Test releasing a slab while one item from the slab being released
// is held by the user. Eventually the user drops the item handle.
// The slab release should not finish before the item handle is dropped.
TYPED_TEST(BaseAllocatorTest, RebalancingWithAllocationsHeldByUser) {
  this->testRebalancingWithAllocationsHeldByUser();
}

// Test releasing a slab while items are being evicted from the allocator.
TYPED_TEST(BaseAllocatorTest, RebalancingWithEvictions) {
  this->testRebalancingWithEvictions();
}

// Test releasing a slab while some items are already removed from the
// allocator,
// but they are still held by the user.
TYPED_TEST(BaseAllocatorTest, RebalancingWithItemsAlreadyRemoved) {
  this->testRebalancingWithItemsAlreadyRemoved();
}

TYPED_TEST(BaseAllocatorTest, RebalancingWithSerialization) {
  this->testRebalancingWithSerialization();
}

TYPED_TEST(BaseAllocatorTest, FastShutdownTestWithAbortedPoolRebalancer) {
  this->testFastShutdownWithAbortedPoolRebalancer();
}

// test item sampling by getting a random item from memory
TYPED_TEST(BaseAllocatorTest, ItemSampling) { this->testItemSampling(); }

// test a cache worker post work handler is triggered
TYPED_TEST(BaseAllocatorTest, CacheWorkerPostWorkHandler) {
  this->testCacheWorkerPostWorkHandler();
}

TYPED_TEST(BaseAllocatorTest, AllocateWithoutEviction) {
  this->testAllocateWithoutEviction();
}

TYPED_TEST(BaseAllocatorTest, AllocateWithTTL) { this->testAllocateWithTTL(); }

TYPED_TEST(BaseAllocatorTest, ExpiredFind) { this->testExpiredFind(); }

TYPED_TEST(BaseAllocatorTest, AllocateWithItemsReaper) {
  this->testAllocateWithItemsReaper();
}

TYPED_TEST(BaseAllocatorTest, ReaperNoWaitUntilEvictions) {
  this->testReaperNoWaitUntilEvictions();
}

TYPED_TEST(BaseAllocatorTest, ReaperOutOfBound) {
  this->testReaperOutOfBound();
}

TYPED_TEST(BaseAllocatorTest, ReaperShutDown) { this->testReaperShutDown(); }

TYPED_TEST(BaseAllocatorTest, UnevictableItems) {
  this->testUnevictableItems();
}

TYPED_TEST(BaseAllocatorTest, BasicFreeMemStrategy) {
  this->testBasicFreeMemStrategy();
}

TYPED_TEST(BaseAllocatorTest, AllocSizes) { this->testAllocSizes(); }

TYPED_TEST(BaseAllocatorTest, CacheCreationTime) {
  this->testCacheCreationTime();
}

TYPED_TEST(BaseAllocatorTest, AddChainedItemSimple) {
  this->testAddChainedItemSimple();
}

TYPED_TEST(BaseAllocatorTest, PopChainedItemSimple) {
  this->testPopChainedItemSimple();
}

TYPED_TEST(BaseAllocatorTest, AddChainedItemSlabRelease) {
  this->testAddChainedItemSlabRelease();
}

TYPED_TEST(BaseAllocatorTest, ChainedAllocTransfer) {
  this->testChainedAllocsTransfer();
}

TYPED_TEST(BaseAllocatorTest, ChainedAllocReplaceInChain) {
  this->testChainedAllocsReplaceInChain();
}

TYPED_TEST(BaseAllocatorTest, ChainedAllocReplaceInChainMultithread) {
  this->testChainedAllocsReplaceInChainMultithread();
}

// Two threads allocating
// One thread slab rebalancing
TYPED_TEST(BaseAllocatorTest, AddChainedItemMultithread) {
  this->testAddChainedItemMultithread();
}

TYPED_TEST(BaseAllocatorTest, AddChainedItemMultiThreadWithMoving) {
  this->testAddChainedItemMultithreadWithMoving();
}

TYPED_TEST(BaseAllocatorTest, AddChainedItemMultiThreadWithMovingAndSync) {
  this->testAddChainedItemMultithreadWithMovingAndSync();
}

TYPED_TEST(BaseAllocatorTest, AddAndPopChainedItemMultithread) {
  this->testAddAndPopChainedItemMultithread();
}

TYPED_TEST(BaseAllocatorTest, ChainedItemSerialization) {
  this->testChainedItemSerialization();
}

TYPED_TEST(BaseAllocatorTest, AddChainedItemUntilEviction) {
  this->testAddChainedItemUntilEviction();
}

TYPED_TEST(BaseAllocatorTest, SerializationWithDifferentHasher) {
  this->testSerializationWithDifferentHasher();
}

TYPED_TEST(BaseAllocatorTest, IsOnShm) { this->testIsOnShm(); }

TYPED_TEST(BaseAllocatorTest, ItemSize) { this->testItemSize(); }

TYPED_TEST(BaseAllocatorTest, IOBufChainCaching) {
  this->testIOBufChainCaching();
}

TYPED_TEST(BaseAllocatorTest, IOBufWrap) { this->testIOBufWrapOnItem(); }
TYPED_TEST(BaseAllocatorTest, ChainedAllocsIteration) {
  this->testChainedAllocsIteration();
}

TYPED_TEST(BaseAllocatorTest, ReplaceChainedItem) {
  this->testReplaceChainedItem();
}

TYPED_TEST(BaseAllocatorTest, MovingSyncCorrectness) {
  this->testMovingSyncCorrectness();
}

TYPED_TEST(BaseAllocatorTest, StatsPermanentCount) {
  this->testAllocPermanentCount();
}
TYPED_TEST(BaseAllocatorTest, StatsChainCount) {
  this->testAllocChainedCount();
}
TYPED_TEST(BaseAllocatorTest, StatsPermanentChainCountMultiThread) {
  this->testCountItemsMultithread();
}
TYPED_TEST(BaseAllocatorTest, StatsPermanentChainCountRestore) {
  this->testItemCountCreationTime();
}

TYPED_TEST(BaseAllocatorTest, EvictionAgeStats) {
  this->testEvictionAgeStats();
}

TYPED_TEST(BaseAllocatorTest, ReplaceInMMContainer) {
  this->testReplaceInMMContainer();
}

TYPED_TEST(BaseAllocatorTest, ReplaceIfAccessible) {
  this->testReplaceIfAccessible();
}

TYPED_TEST(BaseAllocatorTest, PermanentItems) { this->testPermanentItems(); }

TYPED_TEST(BaseAllocatorTest, ChainedItemIterator) {
  this->testChainedItemIterator();
}

TYPED_TEST(BaseAllocatorTest, ChainedItemIteratorInvalidArg) {
  this->testChainIteratorInvalidArg();
}

TYPED_TEST(BaseAllocatorTest, RemoveCbChainedItems) {
  this->testRemoveCbChainedItems();
}

TYPED_TEST(BaseAllocatorTest, RemoveCbNoChainedItems) {
  this->testRemoveCbNoChainedItems();
}

TYPED_TEST(BaseAllocatorTest, DumpEvictionIterator) {
  this->testDumpEvictionIterator();
}

// test config validation
TYPED_TEST(BaseAllocatorTest, ConfigValidation) {
  this->testConfigValidation();
}

TYPED_TEST(BaseAllocatorTest, CackeKeyValidity) {
  this->testCacheKeyValidity();
}

TYPED_TEST(BaseAllocatorTest, RefcountOverflow) {
  this->testRefcountOverflow();
}

TYPED_TEST(BaseAllocatorTest, CCacheWarmRoll) { this->testCCacheWarmRoll(); }

TYPED_TEST(BaseAllocatorTest, ReadOnlyCacheView) {
  this->testReadOnlyCacheView();
}

TYPED_TEST(BaseAllocatorTest, RebalanceByAllocFailure) {
  this->testRebalanceByAllocFailure();
}

namespace { // the tests that cannot be done by TYPED_TEST.

using LruAllocatorTest = BaseAllocatorTest<LruAllocator>;
using Lru2QAllocatorTest = BaseAllocatorTest<Lru2QAllocator>;
using TinyLFUAllocatorTest = BaseAllocatorTest<TinyLFUAllocator>;

// test all the error scenarios with respect to allocating a new key where it
// is not accessible right away.
TEST_F(LruAllocatorTest, AllocateInAccessible) {
  LruAllocator::MMConfig config;
  testAllocateInAccessible(config);
}
TEST_F(Lru2QAllocatorTest, AllocateInAccessible) {
  // Set warm queue size to 0 to avoid the key from being stuck in warm
  // queue and never being evicted which leads to this test's failure.
  Lru2QAllocator::MMConfig config;
  config.coldSizePercent = 50;
  config.hotSizePercent = 50;
  testAllocateInAccessible(config);
}
TEST_F(TinyLFUAllocatorTest, AllocateInAccessible) {
  TinyLFUAllocator::MMConfig config;
  testAllocateInAccessible(config);
}

TEST_F(LruAllocatorTest, EvictionSearchLimit) {
  LruAllocator::MMConfig config;
  testEvictionSearchLimit(config);
}
TEST_F(Lru2QAllocatorTest, EvictionSearchLimit) {
  // Set warm queue size to 0 to avoid the key from being stuck in warm
  // queue and never being evicted which leads to this test's failure.
  Lru2QAllocator::MMConfig config;
  testEvictionSearchLimit(config);
}
TEST_F(TinyLFUAllocatorTest, EvictionSearchLimit) {
  TinyLFUAllocator::MMConfig config;
  config.tinySizePercent = 0;
  testEvictionSearchLimit(config);
}

// create some allocation and hold the references to them. These allocations
// should not be ever evicted. removing the keys while we have handle should
// not mess up anything. Ensures that evict call backs are called when we hold
// references and then later delete the items.
TEST_F(LruAllocatorTest, RefCountEvictCB) {
  LruAllocator::MMConfig config;
  testRefCountEvictCB(config);
}
TEST_F(Lru2QAllocatorTest, RefCountEvictCB) {
  // Set warm queue size to 0 to avoid the key from being stuck in warm
  // queue and never being evicted which leads to this test's failure.
  Lru2QAllocator::MMConfig config;
  config.coldSizePercent = 50;
  config.hotSizePercent = 50;
  testRefCountEvictCB(config);
}
TEST_F(TinyLFUAllocatorTest, RefCountEvictCB) {
  TinyLFUAllocator::MMConfig config;
  testRefCountEvictCB(config);
}

TEST_F(Lru2QAllocatorTest, SerializationMMConfigExtra) {
  testSerializationMMConfigExtra();
}

// test that multiple instance of lru allocator dont pollute the thread local
// stats
TEST_F(LruAllocatorTest, Stats) { this->testStats(false); }
TEST_F(Lru2QAllocatorTest, Stats) { this->testStats(true); }
TEST_F(TinyLFUAllocatorTest, Stats) { this->testStats(false); }

// Try moving a single item from one slab to another
TEST_F(LruAllocatorTest, MoveItem) { this->testMoveItem(true); }
TEST_F(Lru2QAllocatorTest, MoveItem) { this->testMoveItem(true); }
TEST_F(TinyLFUAllocatorTest, MoveItem) { this->testMoveItem(false); }

// Test fragmentation size stats
TEST_F(LruAllocatorTest, FragmentationSizeStat) {
  this->testFragmentationSize();
}
TEST_F(Lru2QAllocatorTest, FragmentationSizeStat) {
  this->testFragmentationSize();
}
TEST_F(TinyLFUAllocatorTest, FragmentationSizeStat) {
  this->testFragmentationSize();
}

// test automatic MMReconfigure behavior: lru refresh time update
TEST_F(LruAllocatorTest, MMReconfigure) { this->testMMReconfigure(); }
TEST_F(TinyLFUAllocatorTest, MMReconfigure) { this->testMMReconfigure(); }
TEST_F(Lru2QAllocatorTest, MMReconfigure) {
  typename Lru2QAllocator::MMConfig mmConfig;
  mmConfig.hotSizePercent = 0;
  mmConfig.coldSizePercent = 0;
  this->testMM2QReconfigure(mmConfig);
}

using LruAllocatorWithMovingTest = BaseAllocatorTest<LruAllocator>;
using TinyLFUAllocatorWithMovingTest = BaseAllocatorTest<TinyLFUAllocator>;

TEST_F(LruAllocatorWithMovingTest, UnevictableItemsWithMoving) {
  testUnevictableItemsWithMoving();
}
TEST_F(TinyLFUAllocatorWithMovingTest, UnevictableItemsWithMoving) {
  testUnevictableItemsWithMoving();
}

} // namespace

} // end of namespace tests
} // end of namespace cachelib
} // end of namespace facebook

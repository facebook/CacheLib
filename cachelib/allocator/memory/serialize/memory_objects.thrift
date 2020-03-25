namespace cpp2 facebook.cachelib.serialization

include "cachelib/allocator/datastruct/serialize/datastruct_objects.thrift"

// Adding a new "required" field will cause the cache to be dropped
// in the next release for our users. If the field needs to be required,
// make sure to communicate that with our users.

struct SlabAllocatorObject {
  2: required i64 memorySize,
  4: required bool canAllocate,
  5: required map<byte, i64> memoryPoolSize,
  7: required i64 slabSize,
  8: required i64 minAllocSize,
  9: required i32 nextSlabIdx,
  10: required list<i32> freeSlabIdxs,
  11: list<i32> advisedSlabIdxs,
}

struct AllocationClassObject {
  1: required byte classId,
  2: required i64 allocationSize, // to accommodate uint32_t allocationSize
  4: required i64 currOffset,
  8: required bool canAllocate,
  9: datastruct_objects.SListObject freedAllocationsObject,
  10: required i32 currSlabIdx,
  11: required list<i32> allocatedSlabIdxs,
  12: required list<i32> freeSlabIdxs,
}

struct MemoryPoolObject {
  1: required byte id,
  2: required i64 maxSize,
  3: required i64 currSlabAllocSize,
  4: required i64 currAllocSize,
  6: required list<i64> acSizes,
  7: required list<AllocationClassObject> ac,
  8: i64 numSlabResize = 0,
  9: i64 numSlabRebalance = 0,
  10: required list<i32> freeSlabIdxs,
  11: i64 numSlabsAdvised = 0,
}

struct MemoryPoolManagerObject {
  1: list<MemoryPoolObject> pools,
  2: map<string, byte> poolsByName,
  3: byte nextPoolId,
  4: i64 unused1,
}

struct MemoryAllocatorObject {
  // fields in MemoryAllocator::Config
  1: required set<i64> allocSizes,
  4: required bool enableZeroedSlabAllocs,
  5: bool lockMemory = false,

  2: required SlabAllocatorObject slabAllocator,
  3: required MemoryPoolManagerObject memoryPoolManager,
}

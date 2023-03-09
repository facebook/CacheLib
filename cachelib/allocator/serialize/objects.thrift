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

namespace cpp2 facebook.cachelib.serialization

include "cachelib/allocator/datastruct/serialize/objects.thrift"

// Adding a new "required" field will cause the cache to be dropped
// in the next release for our users. If the field needs to be required,
// make sure to communicate that with our users.

struct CacheAllocatorMetadata {
  1: required i64 allocatorVersion; // version of cache alloctor
  2: i64 cacheCreationTime = 0; // time when the cache was created.
  3: required i64 accessType = 0; // default chained alloc
  4: required i64 mmType = 0; // default LRU
  5: map<byte, map<byte, i64>> fragmentationSize;
  6: list<byte> compactCachePools;
  7: i64 numPermanentItems;
  8: i64 numChainedParentItems;
  9: i64 numChainedChildItems;
  10: i64 ramFormatVersion = 0; // format version of ram cache
  11: i64 numAbortedSlabReleases = 0; // number of times slab release is aborted
}

struct NvmCacheMetadata {
  1: i64 nvmFormatVersion = 0;
  2: i64 creationTime = 0;
  3: bool safeShutDown = false;
  4: bool encryptionEnabled = false;
  5: bool truncateAllocSize = false;
}

struct CompactCacheMetadataObject {
  1: required i64 keySize;
  2: required i64 valueSize;
}

struct CompactCacheAllocatorObject {
  1: required list<i64> chunks;
  2: required CompactCacheMetadataObject ccMetadata;
}

struct CompactCacheAllocatorManagerObject {
  1: required map<string, CompactCacheAllocatorObject> allocators;
}

struct MMLruConfig {
  1: required i32 lruRefreshTime;
  2: required bool updateOnWrite;
  3: required i32 lruInsertionPointSpec;
  4: bool updateOnRead = true;
  5: bool tryLockUpdate = false;
  6: double lruRefreshRatio = 0.0;
}

struct MMLruObject {
  1: required MMLruConfig config;

  // number of evictions for this MM object.
  5: i64 evictions = 0;

  6: required i64 insertionPoint;
  7: required i64 tailSize;
  8: required DListObject lru;
  9: required i64 compressedInsertionPoint;
}

struct MMLruCollection {
  1: required map<i32, map<i32, MMLruObject>> pools;
}

struct MM2QConfig {
  1: required i32 lruRefreshTime;
  2: required bool updateOnWrite;
  3: required i32 hotSizePercent;
  4: required i32 coldSizePercent;
  5: bool updateOnRead = true;
  6: bool tryLockUpdate = false;
  7: bool rebalanceOnRecordAccess = true;
  8: double lruRefreshRatio = 0.0;
}

struct MM2QObject {
  1: required MM2QConfig config;
  13: bool tailTrackingEnabled = false;

  // number of evictions for this MM object.
  11: i64 evictions = 0;

  // Warm, hot and cold lrus
  12: required MultiDListObject lrus;
}

struct MM2QCollection {
  1: required map<i32, map<i32, MM2QObject>> pools;
}

struct MMTinyLFUConfig {
  1: required i32 lruRefreshTime;
  2: required bool updateOnWrite;
  3: required i32 windowToCacheSizeRatio;
  4: required i32 tinySizePercent;
  5: bool updateOnRead = true;
  6: bool tryLockUpdate = false;
  7: double lruRefreshRatio = 0.0;
}

struct MMTinyLFUObject {
  1: required MMTinyLFUConfig config;

  // number of evictions for this MM object.
  2: i64 evictions = 0;

  // Warm, hot and cold lrus
  3: required MultiDListObject lrus;
}

struct MMTinyLFUCollection {
  1: required map<i32, map<i32, MMTinyLFUObject>> pools;
}

struct ChainedHashTableObject {
  // fields in ChainedHashTable::Config
  1: required i32 bucketsPower;
  2: required i32 locksPower;
  3: i64 numKeys;

  // this magic id ensures on a warm roll, user cannot
  // start the cache with a different hash function
  4: i32 hasherMagicId = 0;
}

struct MMTTLBucketObject {
  4: i64 expirationTime;
  5: i64 creationTime;
  6: required DListObject dList;
}

struct TTLBucketCollection {
  1: required map<i64, MMTTLBucketObject> buckets;
  2: i64 minEpoch = 0;
  3: i64 maxTTL = 0;
  4: i64 interval = 0;
}

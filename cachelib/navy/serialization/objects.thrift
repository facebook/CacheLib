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

namespace cpp2 facebook.cachelib.navy.serialization

struct IndexEntry {
  1: i32 key = 0;
  2: i32 address = 0;
  3: i16 sizeHint = 0;
  4: byte totalHits = 0;
  5: byte currentHits = 0;
}

struct IndexBucket {
  1: i32 bucketId = 0;
  2: list<IndexEntry> entries;
}

struct Region {
  1: i32 regionId = 0;
  2: i32 lastEntryEndOffset = 0;
  3: i32 classId = 0;
  4: i32 numItems = 0;
  5: bool pinned = false;
  6: i32 priority = 0;
}

struct RegionData {
  1: list<Region> regions;
  2: i32 regionSize = 0;
}

struct FifoPolicyNodeData {
  1: i32 idx;
  2: i64 trackTime;
}

struct FifoPolicyData {
  1: list<FifoPolicyNodeData> queue;
}

struct AccessStats {
  1: byte totalHits = 0;
  2: byte currHits = 0;
  3: byte numReinsertions = 0;
}

struct AccessStatsPair {
  1: i64 key;
  2: AccessStats stats;
}

struct AccessTracker {
  1: map<i64, AccessStats> deprecated_data;
  2: list<AccessStatsPair> data;
}

struct BlockCacheConfig {
  1: i64 version = 0;
  2: i64 cacheBaseOffset = 0;
  3: i64 cacheSize = 0;
  4: i32 allocAlignSize = 0;
  5: set<i32> deprecated_sizeClasses;
  6: bool checksum = false;
  7: map<i64, i64> deprecated_sizeDist;
  8: i64 holeCount = 0;
  9: i64 holeSizeTotal = 0;
  10: bool reinsertionPolicyEnabled = false;
  11: i64 usedSizeBytes = 0;
}

struct ValidBucketCheckerState {
  1: i32 numBuckets = 0;
  2: i32 numBucketsPerBit = 0;
  3: i32 numDisabledBuckets = 0;
  4: list<byte> bytes;
}

struct BigHashPersistentData {
  1: i32 version = 0;
  2: i64 generationTime = 0;
  3: i64 itemCount = 0;
  4: i64 bucketSize = 0;
  5: i64 cacheBaseOffset = 0;
  6: i64 numBuckets = 0;
  7: map<i64, i64> deprecated_sizeDist;
  8: i64 usedSizeBytes = 0;
  9: ValidBucketCheckerState validBucketCheckerState;
}

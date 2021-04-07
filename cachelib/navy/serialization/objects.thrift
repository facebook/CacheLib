namespace cpp2 facebook.cachelib.navy.serialization

struct IndexEntry {
  1: required i32 key = 0,
  2: required i32 address = 0,
  3: i16 sizeHint = 0,
  4: byte totalHits = 0,
  5: byte currentHits = 0,
}

struct IndexBucket {
  1: required i32 bucketId = 0,
  2: required list<IndexEntry> entries,
}

struct Region {
  1: required i32 regionId = 0,
  2: required i32 lastEntryEndOffset = 0,
  3: required i32 classId = 0,
  4: required i32 numItems = 0,
  5: required bool pinned = false,
  6: i32 priority = 0,
}

struct RegionData {
  1: required list<Region> regions,
  2: required i32 regionSize = 0,
}

struct FifoPolicyNodeData {
  1: required i32 idx,
  2: required i64 trackTime,
}

struct FifoPolicyData{
  1: required list<FifoPolicyNodeData> queue,
}

struct AccessStats {
  1: byte totalHits = 0,
  2: byte currHits = 0,
  3: byte numReinsertions = 0,
}

struct AccessStatsPair {
  1: i64 key,
  2: AccessStats stats,
}

struct AccessTracker {
  1: map<i64, AccessStats> deprecated_data,
  2: list<AccessStatsPair> data,
}

struct BlockCacheConfig {
  1: required i64 version = 0,
  2: required i64 cacheBaseOffset = 0,
  3: required i64 cacheSize = 0,
  4: required i32 allocAlignSize = 0,
  5: required set<i32> sizeClasses,
  6: required bool checksum = false,
  7: map<i64, i64> sizeDist,
  8: i64 holeCount = 0,
  9: i64 holeSizeTotal = 0,
  10: bool reinsertionPolicyEnabled = false,
}

struct BigHashPersistentData {
  1: required i32 version = 0,
  2: required i64 generationTime = 0,
  3: required i64 itemCount = 0,
  4: required i64 bucketSize = 0,
  5: required i64 cacheBaseOffset = 0,
  6: required i64 numBuckets = 0,
  7: map<i64, i64> sizeDist,
}

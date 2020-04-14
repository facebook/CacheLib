namespace cpp2 facebook.cachelib.serialization

// metadata corresponding to the configuration for
// bits representing a bloom filter blob.
struct BloomFilterPersistentData {
  1: required i32 numFilters = 0;
  2: required i64 hashTableBitSize = 0;
  3: required i64 filterByteSize = 0;
  4: required i32 fragmentSize = 0;
  5: required list<i64> seeds;
}

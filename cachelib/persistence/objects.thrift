namespace cpp2 facebook.cachelib.persistence

enum PersistenceType {
  Versions = 0,
  Configs = 1,
  NvmCacheState = 2,
  ShmInfo = 3,
  ShmHT = 4,
  ShmChainedItemHT = 5,
  ShmData = 6,
  NavyPartition = 7,
}

struct CacheLibVersions {
  1: required i32 persistenceVersion;
  2: required i64 allocatorVersion;
  3: required i64 ramFormatVerson;
  4: optional i64 nvmFormatVersion;
}

struct PersistCacheLibConfig {
  1: required string cacheName;
}

struct PersistenceHeader {
  1: required PersistenceType type;
  // for ShmData and NavyFlash, length is number of block
  // for others, length is the binary data length
  2: required i64 length;
}

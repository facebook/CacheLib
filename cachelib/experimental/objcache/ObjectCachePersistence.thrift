namespace cpp2 facebook.cachelib.objcache.serialization

struct Item {
  1: byte poolId
  2: i32 creationTime,
  3: i32 expiryTime,
  4: string key,
  5: string payload,
}

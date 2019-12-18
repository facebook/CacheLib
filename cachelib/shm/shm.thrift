namespace cpp2 facebook.cachelib.serialization

struct ShmManagerObject {
  1: required byte shmVal,
  3: required map<string, string> nameToKeyMap,
}

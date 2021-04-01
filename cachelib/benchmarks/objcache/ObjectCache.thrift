namespace cpp2 facebook.cachelib.benchmark

cpp_include "cachelib/benchmarks/objcache/Common.h"

// Mimicking an object similar to Feed Leaf's thrift schema
//  struct ObjectSummary {
//    ...
//    7: MFPayloadIntMap intPayload,
//    8: MFPayloadStringMap strPayload,
//    11: MFPayloadVecMap vecPayload,
//    12: optional MFPayloadMMap mapPayload,
//    ...
//  }
// https://fburl.com/diffusion/mmkxam3y

struct StdObject {
  1: map<i16, i64> m1;
  2: map<i16, string> m2;
  3: map<i16, list<i64>> m3;
  4: map<i16, map<i64, string>> m4;

  // This allocator is not used. Instead this is only here to
  // use the "object cache API" so we can run benchmarks
} (cpp.allocator="facebook::cachelib::benchmark::LruObjectCacheAlloc<char>")

struct StdObjectWithAlloc {
  1: map<i16, i64>
      (cpp.use_allocator,
       cpp.template = "facebook::cachelib::benchmark::Map") m1;
  2: map<
      i16,
      string
       (cpp.use_allocator,
        cpp.type = "facebook::cachelib::benchmark::String")
      >
      (cpp.use_allocator,
       cpp.template = "facebook::cachelib::benchmark::Map") m2;
  3: map<
      i16,
      list<i64>
        (cpp.use_allocator,
         cpp.template = "facebook::cachelib::benchmark::Vector")
     >
      (cpp.use_allocator,
       cpp.template = "facebook::cachelib::benchmark::Map") m3;
  4: map<
      i16,
      map<
       i64,
       string
        (cpp.use_allocator,
         cpp.type = "facebook::cachelib::benchmark::String")
      >
      (cpp.use_allocator,
       cpp.template = "facebook::cachelib::benchmark::Map")
     >
      (cpp.use_allocator,
       cpp.template = "facebook::cachelib::benchmark::Map") m4;
} (cpp.allocator="facebook::cachelib::benchmark::LruObjectCacheAlloc<char>")

struct CustomObjectWithAlloc {
  1: map<i16, i64>
      (cpp.use_allocator,
       cpp.template = "facebook::cachelib::benchmark::F14Map") m1;
  2: map<
      i16,
      string
       (cpp.use_allocator,
        cpp.type = "facebook::cachelib::benchmark::String")
      >
      (cpp.use_allocator,
       cpp.template = "facebook::cachelib::benchmark::F14Map") m2;
  3: map<
      i16,
      list<i64>
        (cpp.use_allocator,
         cpp.template = "facebook::cachelib::benchmark::Vector")
     >
      (cpp.use_allocator,
       cpp.template = "facebook::cachelib::benchmark::F14Map") m3;
  4: map<
      i16,
      map<
       i64,
       string
        (cpp.use_allocator,
         cpp.type = "facebook::cachelib::benchmark::String")
      >
      (cpp.use_allocator,
       cpp.template = "facebook::cachelib::benchmark::F14Map")
     >
      (cpp.use_allocator,
       cpp.template = "facebook::cachelib::benchmark::F14Map") m4;
} (cpp.allocator="facebook::cachelib::benchmark::LruObjectCacheAlloc<char>")

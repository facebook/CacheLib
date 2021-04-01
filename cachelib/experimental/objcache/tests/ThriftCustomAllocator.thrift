namespace cpp2 facebook.cachelib.objcache

cpp_include "cachelib/experimental/objcache/tests/Common.h"

struct UseSimpleCustomAllocator {
  // map<string, string>
  // We want to use custom allocator for it

  // A template type like map needs to use "cpp.template" to specify a replacement template
  1: map<

      // A concrete type like string needs to use "cpp.type" to specify a replacement type
      string
        (cpp.use_allocator,
         cpp.type = "facebook::cachelib::objcache::test::TestString"),

      string
        (cpp.use_allocator,
         cpp.type = "facebook::cachelib::objcache::test::TestString")

     > (cpp.use_allocator,
        cpp.template = "facebook::cachelib::objcache::test::TestMap") m;

   // Native types or types that do not allocate memory do NOT need custom allocator
   2: i32 m2;
} (cpp.allocator="facebook::cachelib::objcache::test::ScopedTestAllocator", cpp.allocator_via="m")

struct UseTwoF14Maps {
  1: map<i32, i32>
       (cpp.use_allocator,
        cpp.template = "facebook::cachelib::objcache::test::TestFollyF14FastMap") m1;
  2: map<i32, double>
       (cpp.use_allocator,
        cpp.template = "facebook::cachelib::objcache::test::TestFollyF14FastMap") m2;
} (cpp.allocator=
     "facebook::cachelib::objcache::test::TestF14TemplateAllocator<std::pair<const int32_t, int32_t>>")

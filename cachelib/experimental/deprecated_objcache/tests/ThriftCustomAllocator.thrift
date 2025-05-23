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

namespace cpp2 facebook.cachelib.objcache

cpp_include "cachelib/experimental/deprecated_objcache/tests/Common.h"

include "thrift/annotation/cpp.thrift"
include "thrift/annotation/thrift.thrift"

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {
    "cpp.allocator": "facebook::cachelib::objcache::test::ScopedTestAllocator",
    "cpp.allocator_via": "m",
  },
}
struct UseSimpleCustomAllocator {
  // map<string, string>
  // We want to use custom allocator for it

  // A template type like map needs to use "cpp.template" to specify a replacement template
  1: map_string_cpptype_facebookcachelibobjcachetestTestString_cppuse_allocator_1_string_cpptype_facebookcachelibobjcachetestTestString_cppuse_allocator_1_2760 m;

  // Native types or types that do not allocate memory do NOT need custom allocator
  2: i32 m2;
}
// TODO: thrift allocator propagation behavior is broken. Right now, for the following
//          myObj1 = myObj2;
//       even if the allocator copy-assignment propagation is false, myObj2's
//       allocator will still be propagated to myObj1. The mitigation is to use
//       "cpp.allocator_via" to obtain allocator from a member that is a std::* container
//       or any member that implements allocator propagation correctly
// } (cpp.allocator="facebook::cachelib::objcache::test::ScopedTestAllocator")

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {
    "cpp.allocator": "facebook::cachelib::objcache::test::ScopedTestAllocator",
  },
}
union UnionWithCustomAllocator {
  1: map_i32_string_cpptype_facebookcachelibobjcachetestTestString_cppuse_allocator_1_4964 m1;
  2: string_9300 m2;
  3: i32 m3;
}
// TODO: even though thrift union does not support allocator. We still need to
//       annotate it with allocator so it has a `get_allocator()` method so
//       that when deserializing it will be able to pass an allocator an inner
//       member that requires an allocator. This can be resolved by adding
//       proper allocator support in thrift union. In practice, today this
//       means we need to annontate a union type with allocator, but must
//       keep in mind that any copy-assignment/move-assingment, or
//       deserializing will over-write the allocator that is associated with
//       the inner member inside the union. Because of this, I would recommend
//       user do NOT cache union types into object-cache. Only use this as
//       a structure for process/responding to client requests.
//          MyUnion myUnion;
//          myUnion.member = cache->find<MyMemberType>("a key");
//          // allocator is cachelib-backed allocator
//          protocol::deserialize<MyUnion>(myUnion, someDataFromTheWire);
//          myUnion.member.get_allocator();
//          // ^ This is NOT a cachelib-backed allocator anymore. It now falls back onto heap
//
//       To deserialize from wire data and then insert an inner member
//       directly into cache:
//          MyUnion myUnion = Protocol::deserialize<MyUnion>(dataFromWire);
//          if (myUnion.hasMember()) {
//            auto objectHandle = cache->create<MyMember>(
//                "my key", myUnion.member_ref().value());
//            cache->insertOrReplace(objectHandle);
//          }

@thrift.DeprecatedUnvalidatedAnnotations{
  items = {
    "cpp.allocator": "facebook::cachelib::objcache::test::TestF14TemplateAllocator<std::pair<const int32_t, int32_t>>",
  },
}
struct UseTwoF14Maps {
  1: map_i32_i32_5945 m1;
  2: map_i32_double_308 m2;
}

// The following were automatically generated and may benefit from renaming.
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{name = "facebook::cachelib::objcache::test::TestString"}
typedef string string_cpptype_facebookcachelibobjcachetestTestString_cppuse_allocator_1_607

// The following were automatically generated and may benefit from renaming.
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "facebook::cachelib::objcache::test::TestFollyF14FastMap"}
typedef map<i32, double> map_i32_double_308
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "facebook::cachelib::objcache::test::TestFollyF14FastMap"}
typedef map<i32, i32> map_i32_i32_5945
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "facebook::cachelib::objcache::test::TestMap"}
typedef map<
  i32,
  string_cpptype_facebookcachelibobjcachetestTestString_cppuse_allocator_1_607
> map_i32_string_cpptype_facebookcachelibobjcachetestTestString_cppuse_allocator_1_4964
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{template = "facebook::cachelib::objcache::test::TestMap"}
typedef map<
  string_cpptype_facebookcachelibobjcachetestTestString_cppuse_allocator_1_607,
  string_cpptype_facebookcachelibobjcachetestTestString_cppuse_allocator_1_607
> map_string_cpptype_facebookcachelibobjcachetestTestString_cppuse_allocator_1_string_cpptype_facebookcachelibobjcachetestTestString_cppuse_allocator_1_2760
@thrift.DeprecatedUnvalidatedAnnotations{items = {"cpp.use_allocator": "1"}}
@cpp.Type{name = "facebook::cachelib::objcache::test::TestString"}
typedef string string_9300

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

include "thrift/annotation/thrift.thrift"

@thrift.AllowLegacyMissingUris
package;

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
  @thrift.AllowUnsafeRequiredFieldQualifier
  1: required i32 persistenceVersion;
  @thrift.AllowUnsafeRequiredFieldQualifier
  2: required i64 allocatorVersion;
  @thrift.AllowUnsafeRequiredFieldQualifier
  3: required i64 ramFormatVerson;
  4: optional i64 nvmFormatVersion;
}

struct PersistCacheLibConfig {
  @thrift.AllowUnsafeRequiredFieldQualifier
  1: required string cacheName;
}

struct PersistenceHeader {
  @thrift.AllowUnsafeRequiredFieldQualifier
  1: required PersistenceType type;
  // total length of data, if the data is split
  // in blocks, it also includes checksum and length
  // of each block.
  @thrift.AllowUnsafeRequiredFieldQualifier
  2: required i64 length;
}

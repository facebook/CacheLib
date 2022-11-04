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

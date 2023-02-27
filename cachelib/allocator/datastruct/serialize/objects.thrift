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

// Adding a new "required" field will cause the cache to be dropped
// in the next release for our users. If the field needs to be required,
// make sure to communicate that with our users.

// Saved state for an SList
struct SListObject {
  2: required i64 size;
  3: required i64 compressedHead; // Pointer to the head element
  // TODO(bwatling): remove the default value and clean up SList::SList() once
  // we can rely on 'compressedTail' always being valid.
  4: i64 compressedTail = -1; // Pointer to the tail element
}

struct DListObject {
  1: required i64 compressedHead;
  2: required i64 compressedTail;
  3: required i64 size;
}

struct MultiDListObject {
  1: required list<DListObject> lists;
}

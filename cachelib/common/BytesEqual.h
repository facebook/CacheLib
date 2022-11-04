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

#pragma once
#include <folly/CPortability.h>
#include <folly/Likely.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>

namespace facebook {
namespace cachelib {

// Dereferencing the pointer through uintX_t* will violate strict aliasing
// rules. To get around, @ngbronson suggested this hack which cheats by
// tricking the compiler. This should be generate the same machine
// instructions as (*(uint64_t*)a) == (*(uint64_t*)b)
template <typename T>
inline bool eq(const char* a, const char* b) {
  static_assert(std::is_integral<T>::value, "Non integral type");
  T lhs;
  T rhs;
  memcpy(&lhs, a, sizeof(T));
  memcpy(&rhs, b, sizeof(T));
  return lhs == rhs;
}

// Compare the two byte arrays up to the len and return if they are equal or
// not. Defaults to using memcmp for more than 1024 byte comparisons. Use this
// only when you are comparing less than 64 byte buffers for getting a win.
//
// @param a   first buffer
// @param b   second buffer
// @param len length of the buffers
// @return true if the byte arrays are the same. False if not.
// bool bytesEqual(const void* a_, const void* b_, size_t len);

FOLLY_ALWAYS_INLINE bool bytesEqual(const void* a_,
                                    const void* b_,
                                    size_t len) {
  const char* a = (const char*)a_;
  const char* b = (const char*)b_;

  if (UNLIKELY(len >= 1024)) {
    return memcmp(a_, b_, len) == 0;
  }

  while (len >= 8 && eq<uint64_t>(a, b)) {
    a += 8;
    b += 8;
    len -= 8;
  }

  if (len >= 8) {
    return false;
  }

  if (len >= 4 && eq<uint32_t>(a, b)) {
    a += 4;
    b += 4;
    len -= 4;
  }

  if (len >= 4) {
    return false;
  }

  if (len >= 2 && eq<uint16_t>(a, b)) {
    a += 2;
    b += 2;
    len -= 2;
  }

  return (len == 1 && *a == *b) || len == 0;
}

} // namespace cachelib
} // namespace facebook

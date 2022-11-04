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

#include "cachelib/navy/testing/BufferGen.h"

namespace facebook {
namespace cachelib {
namespace navy {
const char BufferGen::kAlphabet[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz"
    "0123456789=+";

Buffer BufferGen::gen(uint32_t size) {
  Buffer buf{size};
  auto p = buf.data();
  for (uint32_t i = 0; i < size; i++) {
    p[i] = static_cast<uint8_t>(kAlphabet[rg_() % (sizeof(kAlphabet) - 1)]);
  }
  return buf;
}

Buffer BufferGen::gen(uint32_t sizeMin, uint32_t sizeMax) {
  if (sizeMin == sizeMax) {
    return gen(sizeMin);
  } else {
    return gen(sizeMin + static_cast<uint32_t>(rg_() % (sizeMax - sizeMin)));
  }
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

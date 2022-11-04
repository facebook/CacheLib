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

#include <random>

#include "cachelib/navy/common/Buffer.h"

namespace facebook {
namespace cachelib {
namespace navy {
// Helper class that generates an arbitrarily long buffer
class BufferGen {
 public:
  // @param seed  seed value
  explicit BufferGen(uint32_t seed = 1) : rg_(seed) {}

  // @param size    size of the buffer to generate
  Buffer gen(uint32_t size);

  // Randomly generate a buffer between [sizeMin, sizeMax)
  // @param sizeMin   mininum size of the new buffer
  // @param sizeMax   maximum size of the new buffer
  Buffer gen(uint32_t sizeMin, uint32_t sizeMax);

 private:
  static const char kAlphabet[65];
  std::minstd_rand rg_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

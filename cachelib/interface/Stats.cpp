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

#include "cachelib/interface/Stats.h"

namespace facebook::cachelib::interface::detail {

bool shouldSample(size_t sampleRate) {
  thread_local size_t counter = 0;
  if (sampleRate > 0) {
    return counter++ % sampleRate == 0;
  } else {
    return false;
  }
}

} // namespace facebook::cachelib::interface::detail

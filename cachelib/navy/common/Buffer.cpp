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

#include "cachelib/navy/common/Buffer.h"

#include <algorithm>
#include <cstdio>
#include <cstring>

namespace facebook {
namespace cachelib {
namespace navy {
namespace {
bool isLikeText(BufferView view) {
  for (size_t i = 0; i < view.size(); i++) {
    if (!between(view.byteAt(i), 32, 127)) {
      return false;
    }
  }
  return true;
}
} // namespace

std::string toString(BufferView view, bool compact) {
  std::string rv;
  if (isLikeText(view)) {
    rv.append("BufferView \"")
        .append(reinterpret_cast<const char*>(view.data()), view.size())
        .append("\"");
  } else {
    rv.append("BufferView size=")
        .append(std::to_string(view.size()))
        .append(" <");
    char buf[16]{};
    auto maxVisible = compact ? std::min(view.size(), size_t{80}) : view.size();
    for (size_t i = 0; i < maxVisible; i++) {
      std::snprintf(buf, sizeof(buf), "%02x", view.byteAt(i));
      rv.append(buf);
    }
    if (view.size() > maxVisible) {
      rv.append("...");
    }
    rv.append(">");
  }
  return rv;
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

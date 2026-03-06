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

#include <folly/logging/xlog.h>

#define EXPECT_OK(result)               \
  {                                     \
    auto&& __result__ = (result);       \
    EXPECT_TRUE(__result__.hasValue()); \
  }
#define ASSERT_OK(result)          \
  ({                               \
    auto&& __result__ = (result);  \
    XCHECK(__result__.hasValue()); \
    std::move(__result__).value(); \
  })
#define EXPECT_ERROR(result, code)             \
  ({                                           \
    auto&& __result__ = (result);              \
    EXPECT_TRUE(__result__.hasError());        \
    EXPECT_EQ(__result__.error().code_, code); \
  })

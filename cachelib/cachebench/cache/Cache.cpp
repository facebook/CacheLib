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

#include "cachelib/cachebench/cache/Cache.h"

DEFINE_bool(report_api_latency,
            false,
            "Enable reporting cache API latency tracking");

DEFINE_string(
    report_ac_memory_usage_stats,
    "",
    "Enable reporting statistics for each allocation class. Set to"
    "'human_readable' to print KB/MB/GB or to 'raw' to print in bytes.");

namespace facebook {
namespace cachelib {
namespace cachebench {} // namespace cachebench
} // namespace cachelib
} // namespace facebook

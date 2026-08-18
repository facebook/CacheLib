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

#include <folly/Conv.h>
#include <folly/FileUtil.h>
#include <folly/String.h>

#include <cstdlib>

#include "cachelib/shm/ShmCommon.h"

namespace facebook {
namespace cachelib {
namespace tests {

namespace detail {

// Number of free huge pages of the given size, from sysfs. Returns 0 if the
// sysfs entry doesn't exist or can't be read.
inline size_t freeHugePages(size_t pageSize) {
  const auto path =
      fmt::format("/sys/kernel/mm/hugepages/hugepages-{}kB/free_hugepages",
                  pageSize / 1024);
  std::string content;
  if (!folly::readFile(path.c_str(), content)) {
    return 0;
  }
  return folly::tryTo<size_t>(folly::trimWhitespace(content)).value_or(0);
}

inline std::string hugetlbfsMountFromEnv() {
  constexpr const char* kHugetlbfsMountEnv = "CACHELIB_TEST_HUGETLBFS_MOUNT";
  const char* dir = std::getenv(kHugetlbfsMountEnv);
  return dir != nullptr ? std::string{dir} : std::string{};
}

} // namespace detail

// Returns true if huge pages of pageSize can actually be allocated on this
// host: the kernel supports the size, there are free pages in the pool, and
// (for POSIX) a hugetlbfs mount was supplied via env.
inline bool hugePagesUsable(size_t pageSize, bool posix = false) {
  if (!PageSize::supportedHugePageSizes().contains(pageSize) ||
      detail::freeHugePages(pageSize) == 0) {
    return false;
  }
  return !posix || !detail::hugetlbfsMountFromEnv().empty();
}

// Overload taking PageSize directly
inline bool hugePagesUsable(const PageSize& ps, bool posix = false) {
  return hugePagesUsable(ps.getPageSize(), posix);
}

// Checks if enough free huge pages exist to cover bytes (rounded up). Useful
// for anonymous HugeTLB tests that use MAP_NORESERVE (which would succeed at
// mmap time even with 0 free pages but SIGBUS on fault).
inline bool canReserveHugePages(size_t bytes, const PageSize& ps) {
  if (!ps.isHugePage()) {
    return true;
  }
  const size_t needPages =
      (ps.getPageAlignedSize(bytes) + ps.getPageSize() - 1) / ps.getPageSize();
  return detail::freeHugePages(ps.getPageSize()) >= needPages &&
         hugePagesUsable(ps);
}

} // namespace tests
} // namespace cachelib
} // namespace facebook

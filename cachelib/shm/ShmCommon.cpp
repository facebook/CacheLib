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

#include "cachelib/shm/ShmCommon.h"

#include <folly/FileUtil.h>
#include <folly/Random.h>
#include <folly/Range.h>
#include <folly/String.h>
#include <folly/logging/xlog.h>
#include <sys/types.h>

namespace facebook {
namespace cachelib {

namespace detail {
size_t getPageSize(PageSizeT pageSize) {
  static size_t sizes[] = {static_cast<size_t>(sysconf(_SC_PAGESIZE)),
                           2 * 1024 * 1024, 1024 * 1024 * 1024};
  const long pagesize = sizes[pageSize];
  XDCHECK_NE(pagesize, -1);
  XDCHECK_GT(pagesize, 0);
  return pagesize;
}

bool isPageAlignedSize(size_t size, PageSizeT p) {
  return ((size != 0) && (size % getPageSize(p) == 0));
}

size_t getPageAlignedSize(size_t size, PageSizeT p) {
  const auto pageSize = getPageSize(p);
  if (size == 0) {
    return pageSize;
  }

  auto delta = size % pageSize;
  return delta == 0 ? size : size + pageSize - delta;
}

bool isPageAlignedAddr(void* addr, PageSizeT p) {
  return ((uintptr_t)addr) % getPageSize(p) == 0;
}

size_t pageAligned(size_t size, PageSizeT p) {
  const auto pageSize = getPageSize(p);
  XDCHECK(!(pageSize & (pageSize - 1)));
  return 1 + ((size - 1) | (pageSize - 1));
}

namespace {
std::vector<folly::StringPiece> getSmapLines(const std::string& smapContent) {
  std::vector<folly::StringPiece> lines;
  folly::split('\n', smapContent, lines, true);
  XDCHECK(!lines.empty());
  return lines;
}

size_t getAddressVal(folly::StringPiece addr) {
  // addresses are in base 16
  const size_t ret = strtoull(addr.data(), nullptr, 16);
  XDCHECK_NE(ret, 0u);
  return ret;
}

bool lineAddressMatches(folly::StringPiece line, uintptr_t addr) {
  // line should be of form
  // 006de000-01397000 rw-p 00000000 00:00 0                          [heap]

  std::vector<folly::StringPiece> tokens;
  // split into tokens by space
  folly::split(' ', line, tokens, /* ignore empty */ true);

  XDCHECK(!tokens.empty());
  folly::StringPiece startAddr;
  folly::StringPiece endAddr;

  // split the first token using the '-' separator
  if (!folly::split('-', tokens[0], startAddr, endAddr)) {
    throw std::invalid_argument(
        folly::sformat("Invalid address field {}", tokens[0]));
  }

  // parse the address values.
  size_t start = getAddressVal(startAddr);
  size_t end = getAddressVal(endAddr);
  return start <= addr && end >= addr;
}

bool isAddressLine(folly::StringPiece line) {
  // address lines contain lots of fields before the first :
  // 006de000-01397000 rw-p 00000000 00:00 0                          [heap]
  folly::StringPiece first, second;
  folly::split(':', line, first, second);
  return first.find(' ') != std::string::npos;
}

} // namespace

PageSizeT getPageSizeInSMap(void* addr) {
  std::string smapContent;
  folly::readFile("/proc/self/smaps", smapContent);
  const auto smapLines = getSmapLines(smapContent);

  bool foundMatching = false;
  for (auto line : smapLines) {
    const bool isAddr = isAddressLine(line);
    if (!foundMatching && isAddr &&
        lineAddressMatches(line, reinterpret_cast<uintptr_t>(addr))) {
      foundMatching = true;
      continue;
    }

    if (!foundMatching) {
      continue;
    }

    XDCHECK(foundMatching);
    XDCHECK(!isAddr);

    // Format is the following
    // KernelPageSize:        4 kB
    folly::StringPiece fieldName, value;
    folly::split(':', line, fieldName, value);
    if (fieldName != "MMUPageSize") {
      continue;
    }

    value = folly::skipWhitespace(value);

    folly::StringPiece sizeVal;
    folly::StringPiece unitVal;
    folly::split(' ', value, sizeVal, unitVal);
    XDCHECK_EQ(unitVal, "kB");
    size_t size = folly::to<size_t>(sizeVal) * 1024;
    if (size == getPageSize(PageSizeT::TWO_MB)) {
      return PageSizeT::TWO_MB;
    } else if (size == getPageSize(PageSizeT::ONE_GB)) {
      return PageSizeT::ONE_GB;
    } else {
      XDCHECK_EQ(size, getPageSize());
      return PageSizeT::NORMAL;
    }
  }
  throw std::invalid_argument("address mapping not found in /proc/self/smaps");
}

} // namespace detail
} // namespace cachelib
} // namespace facebook

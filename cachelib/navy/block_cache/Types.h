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

#include <cassert>
#include <ostream>

#include "cachelib/navy/common/CompilerUtils.h"

namespace facebook {
namespace cachelib {
namespace navy {
// Represents the identifier of a @Region.
class RegionId {
 public:
  RegionId() = default;
  // @param idx   the unique index
  explicit RegionId(uint32_t idx) : idx_{idx} {}

  // Checks whether the index is valid.
  bool valid() const noexcept { return idx_ != kInvalid; }

  // Returns the unique index.
  uint32_t index() const noexcept { return idx_; }

  bool operator==(const RegionId& other) const noexcept {
    return idx_ == other.idx_;
  }

  bool operator!=(const RegionId& other) const noexcept {
    return !(*this == other);
  }

 private:
  static constexpr uint32_t kInvalid{~0u};
  uint32_t idx_{kInvalid};
};

inline std::ostream& operator<<(std::ostream& os, RegionId rid) {
  os << "RegionId{";
  if (rid.valid()) {
    os << rid.index();
  } else {
    os << "invalid";
  }
  return os << "}";
}

// @AbsAddress and @RelAddress are two way to represent address on the device.
// Region manager converts between representations.

// @AbsAddress represents device address as flat 64-bit byte offset of the
// device.
class AbsAddress {
 public:
  AbsAddress() = default;
  // @param o   64-bit offset of the device
  explicit AbsAddress(uint64_t o) : offset_{o} {}

  // Returns the 64-bit offset.
  uint64_t offset() const { return offset_; }

  // Adds a 64-bit offset to the existing offset and returns the new
  // @AbsAddress.
  // @param ofs   the 64-bit offset to be added
  AbsAddress add(uint64_t ofs) const { return AbsAddress{offset_ + ofs}; }

  // Subtracts a 64-bit offset from the existing offset and returns the new
  // @AbsAddress.
  // @param ofs   the 64-bit offset subtrahend, less than the existing offset
  AbsAddress sub(uint64_t ofs) const {
    XDCHECK_LE(ofs, offset_);
    return AbsAddress{offset_ - ofs};
  }

  bool operator==(AbsAddress other) const { return offset_ == other.offset_; }
  bool operator!=(AbsAddress other) const { return !(*this == other); }

 private:
  uint64_t offset_{};
};

inline std::ostream& operator<<(std::ostream& os, AbsAddress addr) {
  return os << "AbsAddress{" << addr.offset() << "}";
}

// @RelAddress represents device address as 32-bit region id and 32-bit offset
// inside that region.
class RelAddress {
 public:
  RelAddress() = default;
  // @param r   32-bit region id
  // @param o   32-bit offset inside the region
  explicit RelAddress(RegionId r, uint32_t o = 0) : rid_{r}, offset_{o} {}

  // Returns the region id.
  RegionId rid() const { return rid_; }

  // Returns the 32-bit offset.
  uint32_t offset() const { return offset_; }

  // Adds a 32-bit offset to the existing offset and returns the new
  // @RelAddress.
  // @param ofs   the 32-bit offset to be added
  RelAddress add(uint32_t ofs) const { return RelAddress{rid_, offset_ + ofs}; }

  // Subtracts a 32-bit offset from the existing offset and returns the new
  // @RelAddress.
  // @param ofs   the 32-bit offset subtrahend, less than the existing offset
  RelAddress sub(uint32_t ofs) const {
    XDCHECK_LE(ofs, offset_);
    return RelAddress{rid_, offset_ - ofs};
  }

  bool operator==(RelAddress other) const {
    return offset_ == other.offset_ && rid_ == other.rid_;
  }
  bool operator!=(RelAddress other) const { return !(*this == other); }

 private:
  RegionId rid_;
  uint32_t offset_{};
};

inline std::ostream& operator<<(std::ostream& os, RelAddress addr) {
  return os << "RelAddress{" << addr.rid().index() << ", " << addr.offset()
            << "}";
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

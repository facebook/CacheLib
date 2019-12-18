#pragma once

#include <cassert>
#include <ostream>

#include <folly/logging/xlog.h>

#include "cachelib/navy/common/CompilerUtils.h"

namespace facebook {
namespace cachelib {
namespace navy {
class RegionId {
 public:
  RegionId() = default;
  explicit RegionId(uint32_t idx) : idx_{idx} {}

  bool valid() const noexcept { return idx_ != kInvalid; }

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
  explicit AbsAddress(uint64_t o) : offset_{o} {}

  uint64_t offset() const { return offset_; }

  AbsAddress add(uint64_t ofs) const { return AbsAddress{offset_ + ofs}; }

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
  explicit RelAddress(RegionId r, uint32_t o = 0) : rid_{r}, offset_{o} {}

  RegionId rid() const { return rid_; }

  uint32_t offset() const { return offset_; }

  RelAddress add(uint32_t ofs) const { return RelAddress{rid_, offset_ + ofs}; }

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

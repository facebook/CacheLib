#pragma once

#include <memory>
#include <system_error>

#include <folly/logging/xlog.h>

#include "cachelib/shm/PosixShmSegment.h"
#include "cachelib/shm/ShmCommon.h"
#include "cachelib/shm/SysVShmSegment.h"

namespace facebook {
namespace cachelib {

/**
 * This class supports using a shared memory api and map that to our address
 * space. The segments can be created by they corresponding key type. They can
 * be mapped into the address space. Segments once mapped cannot map to
 * another address space until the first mapping is detached.
 * getCurrentMapping tells the usable address space mapped to the segment. The
 * mappings are detached when the object is destroyed. However, the segments
 * are not removed when the object is destroyed to allow reattaching to them.
 * This class is not thread safe.
 *
 * Look into tests/test_shm.cpp for usage.
 */
class ShmSegment {
 public:
  // create a new segment with the given key
  // @param name   name of the segment
  // @param size   size of the segment.
  // @param opts   the options for the segment.
  ShmSegment(ShmNewT,
             std::string name,
             size_t size,
             bool usePosix,
             ShmSegmentOpts opts = {}) {
    if (usePosix) {
      segment_ = std::make_unique<PosixShmSegment>(ShmNew, std::move(name),
                                                   size, opts);
    } else {
      segment_ =
          std::make_unique<SysVShmSegment>(ShmNew, std::move(name), size, opts);
    }
  }

  // attach to an existing segment with the given key
  // @param name   name of the segment
  // @param opts   the options for the segment.
  ShmSegment(ShmAttachT,
             std::string name,
             bool usePosix,
             ShmSegmentOpts opts = {}) {
    if (usePosix) {
      segment_ =
          std::make_unique<PosixShmSegment>(ShmAttach, std::move(name), opts);
    } else {
      segment_ =
          std::make_unique<SysVShmSegment>(ShmAttach, std::move(name), opts);
    }
  }

  ~ShmSegment() {
    try {
      detachCurrentMapping();
    } catch (const std::system_error& e) {
      XDCHECK_EQ(e.code().value(), EINVAL); // Invalid errno
    }
  }

  // maps the segment into the address for the segment's current length. If the
  // segment is invalid or deleted, returns false and does nothing.
  //
  // @param  addr     the address to be attached at. If nullptr, the segment
  //                  will be mapped at a random address chosen by the kernel
  //
  // @return true if the mapping was successful, false otherwise. Upon
  // success, getCurrentMapping can be used to fetch the details of the
  // address mapping
  bool mapAddress(void* addr) {
    if (isMapped() || !segment_->isActive()) {
      return false;
    }

    void* retAddr = segment_->mapAddress(addr);
    XDCHECK(retAddr == addr || addr == nullptr);
    mapping_ = ShmAddr{retAddr, segment_->getSize()};
    return true;
  }

  // marks the shared memory resource to be detached once all the
  // mappings are unmapped.
  void markForRemoval() { segment_->markForRemoval(); }

  // current size of the segment
  size_t getSize() const { return segment_->getSize(); }

  std::string getKeyStr() { return segment_->getKeyStr(); }

  bool isActive() const noexcept { return segment_->isActive(); }

  bool isMarkedForRemoval() const noexcept {
    return segment_->isMarkedForRemoval();
  }

  // if its marked to be removed and there is nothing mapped, it is invalid.
  bool isInvalid() const noexcept { return !(isMapped() || isActive()); }

  // is the segment currently mapped to the address space
  bool isMapped() const noexcept { return mapping_.isMapped(); }

  // detaches the current mapping if it exists.
  void detachCurrentMapping() {
    if (!isMapped()) {
      return;
    }

    segment_->unMap(mapping_.addr);
    mapping_ = {};
  }

  // returns the current mapping and the usable length of the mapping.
  const ShmAddr& getCurrentMapping() const noexcept { return mapping_; }

 private:
  std::unique_ptr<ShmBase> segment_{};
  ShmAddr mapping_{};
};
} // namespace cachelib
} // namespace facebook

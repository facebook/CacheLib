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
#include <numa.h>
#include <numaif.h>
#include <sys/ipc.h>
#include <sys/mman.h>
#include <sys/shm.h>
#include <sys/stat.h>

#include <set>
#include <string>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#include <folly/Format.h>
#include <folly/Range.h>
#pragma GCC diagnostic pop
#include <folly/logging/xlog.h>

// On Linux, glibc's <bits/shm.h> provides SHM_HUGE_SHIFT only in >v2.36.  The
// canonical shift is in kernel UAPI <asm-generic/hugetlb_encode.h>.  Include
// only that header to avoid struct redefinition conflicts.
#ifdef __linux__
#include <asm-generic/hugetlb_encode.h>
#endif

#ifndef MAP_HUGETLB
#warning "MAP_HUGETLB not defined, disabling hugepage support"
#define MAP_HUGETLB 0
#define MAP_HUGE_SHIFT 0
#else
#ifndef MAP_HUGE_SHIFT
#ifdef HUGETLB_FLAG_ENCODE_SHIFT
#define MAP_HUGE_SHIFT HUGETLB_FLAG_ENCODE_SHIFT
#else
#warning "MAP_HUGE_SHIFT not defined, falling back to default huge page size"
#define MAP_HUGE_SHIFT 0
#endif
#endif
#endif

#ifndef SHM_HUGETLB
#warning "SHM_HUGETLB not defined, disabling hugepage support"
#define SHM_HUGETLB 0
#define SHM_HUGE_SHIFT 0
#else
#ifndef SHM_HUGE_SHIFT
#ifdef HUGETLB_FLAG_ENCODE_SHIFT
#define SHM_HUGE_SHIFT HUGETLB_FLAG_ENCODE_SHIFT
#else
#warning "SHM_HUGE_SHIFT not defined, falling back to default huge page size"
#define SHM_HUGE_SHIFT 0
#endif
#endif
#endif

namespace facebook {
namespace cachelib {

enum ShmAttachT { ShmAttach };
enum ShmNewT { ShmNew };

class PageSize {
 public:
  // Page size in bytes for a shm segment. kNormalPageSize selects the system
  // default page size; passing a supported huge-page size requests
  // HugeTLB-backed pages for the segment. The valid huge-page sizes depend on
  // the CPU's base page granule; callers may pass any kernel-supported size
  // These constants are just conveniences, it does NOT mean they're available.
  static constexpr size_t kNormalPageSize = 0;
  // 4 KiB base page granule (x86-64, aarch64-4k)
  static constexpr size_t kHugePageSize2MB = 2ULL * 1024 * 1024;
  static constexpr size_t kHugePageSize1GB = 1024ULL * 1024 * 1024;
  // 16 KiB base page granule (aarch64-16k)
  static constexpr size_t kHugePageSize32MB = 32ULL * 1024 * 1024;
  static constexpr size_t kHugePageSize64GB = 64ULL * 1024 * 1024 * 1024;
  // 64 KiB base page granule (aarch64-64k)
  static constexpr size_t kHugePageSize512MB = 512ULL * 1024 * 1024;

  explicit PageSize(size_t pageSize = kNormalPageSize) : pageSize_(pageSize) {
    XDCHECK(pageSize == kNormalPageSize || folly::isPowTwo(pageSize_))
        << "Invalid page size " << pageSize_ << ", must be power-of-two";
  }

  /* the system base page size in bytes */
  static size_t systemPageSize();

  /* Huge-page sizes (in bytes) the running kernel supports, read from
   * /sys/kernel/mm/hugepages. Empty if none are available / not readable. */
  static const std::set<size_t>& supportedHugePageSizes();

  /* effective page size in bytes */
  size_t getPageSize() const noexcept;

  /* true if pageSize denotes a huge page (larger than the system base page) */
  bool isHugePage() const noexcept;

  /* mmap(2) huge-page flags for pageSize (0 if not huge or unsupported) */
  int hugePageMmapFlags() const noexcept;

  /* shmget(2) huge-page flags for pageSize (0 if not huge or unsupported) */
  int hugePageShmgetFlags() const noexcept;

  /* round up to the closest page size */
  size_t getPageAlignedSize(size_t size) const noexcept;

  /* returns page aligned size for the input that is atleast as big as the input
   * size */
  size_t pageAligned(size_t size) const noexcept;

  /* true if the length is page aligned  */
  bool isPageAlignedSize(size_t length) const noexcept;

  /* true if the address is page aligned */
  bool isPageAlignedAddr(void* addr) const noexcept;

  // return the page size (in bytes) of the address mapping in this process.
  //
  // @throw  std::invalid_argument if the address mapping is not found.
  static size_t getPageSizeInSMap(void* addr);

 private:
  size_t pageSize_;

  /* log2(pageSize); used to encode the MAP_HUGE_* / SHM_HUGE_* flag bits */
  unsigned hugePageSizeToShift() const noexcept;
};

class NumaBitMask {
 public:
  using native_bitmask_type = struct bitmask*;

  NumaBitMask() { nodesMask = numa_allocate_nodemask(); }

  NumaBitMask(const NumaBitMask& other) {
    nodesMask = numa_allocate_nodemask();
    copy_bitmask_to_bitmask(other.nodesMask, nodesMask);
  }

  NumaBitMask(NumaBitMask&& other) noexcept {
    nodesMask = other.nodesMask;
    other.nodesMask = nullptr;
  }

  NumaBitMask(const std::string& str) {
    nodesMask = numa_parse_nodestring_all(str.c_str());
  }

  ~NumaBitMask() {
    if (nodesMask) {
      numa_bitmask_free(nodesMask);
    }
  }

  constexpr NumaBitMask& operator=(const NumaBitMask& other) {
    if (this != &other) {
      if (!nodesMask) {
        nodesMask = numa_allocate_nodemask();
      }
      copy_bitmask_to_bitmask(other.nodesMask, nodesMask);
    }
    return *this;
  }

  native_bitmask_type getNativeBitmask() const noexcept { return nodesMask; }

  NumaBitMask& setBit(unsigned int n) {
    numa_bitmask_setbit(nodesMask, n);
    return *this;
  }

  bool empty() const noexcept {
    return numa_bitmask_equal(numa_no_nodes_ptr, nodesMask) == 1;
  }

 protected:
  native_bitmask_type nodesMask = nullptr;
};

struct ShmSegmentOpts {
  PageSize pageSize{};
  bool readOnly{false};
  size_t alignment{1}; // alignment for mapping.
  NumaBitMask memBindNumaNodes;

  explicit ShmSegmentOpts(PageSize p) : pageSize(std::move(p)) {}
  explicit ShmSegmentOpts(PageSize p, bool ro)
      : pageSize(std::move(p)), readOnly(ro) {}
  ShmSegmentOpts() = default;
};

// Represents a mapping on shm with and address and size
struct ShmAddr {
  ShmAddr(void* a, size_t s) : addr(a), size(s) {}
  ShmAddr() {}

  bool isMapped() const noexcept { return addr != nullptr; }

  void* addr{nullptr}; // start of the memory
  size_t size{0};      // length from start that actually has a backing shm
};

/* common interface for both posix and sysv shared memory segments */
class ShmBase {
 public:
  ShmBase(ShmSegmentOpts opts, std::string name)
      : opts_(std::move(opts)), name_(std::move(name)) {}
  ShmBase(const ShmBase&) = delete;
  ShmBase& operator=(const ShmBase&) = delete;

  virtual ~ShmBase() {}

  bool isActive() const noexcept { return state_ == State::NORMAL; }
  bool isMarkedForRemoval() const noexcept {
    return state_ == State::MARKED_FOR_REMOVAL;
  }

  virtual size_t getSize() const = 0;
  virtual std::string getKeyStr() const = 0;
  virtual void* mapAddress(void* addr) const = 0;
  virtual void unMap(void* addr) const = 0;
  virtual void markForRemoval() = 0;

  const std::string& getName() const { return name_; }

 protected:
  void markActive() noexcept { state_ = State::NORMAL; }
  void markForRemove() noexcept { state_ = State::MARKED_FOR_REMOVAL; }

  // options for this segment
  ShmSegmentOpts opts_{};

  // address mapping that ensures that we own this segment for the lifeteime
  // of the object.
  void* referenceMapping_{nullptr};

 private:
  enum class State { NORMAL, MARKED_FOR_REMOVAL };
  State state_{State::NORMAL}; // current state of the segment.
  std::string name_{};         // name of the segment
};

} // namespace cachelib
} // namespace facebook

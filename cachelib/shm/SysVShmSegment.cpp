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

#include "cachelib/shm/SysVShmSegment.h"

#include <folly/ScopeGuard.h>
#include <folly/hash/Hash.h>
#include <folly/logging/xlog.h>
#include <numa.h>
#include <numaif.h>
#include <sys/mman.h>
#include <sys/shm.h>

#include <cstring>

#include "cachelib/common/Utils.h"

namespace facebook {
namespace cachelib {

constexpr static uint32_t kHugePageSize = 1 << 21; // 2MB
constexpr static uint32_t kModeRWFlags = 0666;     // octal rw for ugo
constexpr static uint32_t kModeRFlags = 0444;      // octal ro for ugo

namespace detail {

int shmGetImpl(key_t key, size_t size, int flags) {
  if (key == IPC_PRIVATE) {
    util::throwSystemError(EINVAL, "Cannot create private segment");
  }

  const int shmid = shmget(key, size, flags);

  if (shmid != kInvalidShmId) {
    return shmid;
  }

  switch (errno) {
  case EACCES:
  case EINVAL:
  case ENOSPC:
  case ENOMEM:
  case ENFILE:
    util::throwSystemError(
        errno,
        folly::sformat(
            "Failed to create segment with key {}, size {}, flags {}",
            key,
            size,
            flags));
    break;
  case ENOENT:
    if (!(flags & IPC_CREAT)) { // trying to attach
      util::throwSystemError(errno);
    } else {
      util::throwSystemError(errno, "Invalid errno");
    }
    break;
  case EEXIST:
    if (flags & (IPC_CREAT | IPC_EXCL)) { // trying to create
      util::throwSystemError(errno);
    } else {
      XDCHECK(false);
      util::throwSystemError(errno, "Invalid errno");
    }
    break;
  case EPERM:
#ifdef __linux__
    if (flags & SHM_HUGETLB) {
      util::throwSystemError(errno);
      break;
    }
#endif
    XDCHECK(false);
    util::throwSystemError(errno, "Invalid errno");
  default:
    XDCHECK(false);
    util::throwSystemError(errno, "Invalid errno");
  }
  return kInvalidShmId;
}

bool isAddrAligned(const void* const addr) {
  return reinterpret_cast<uintptr_t>(addr) % SHMLBA == 0;
}

void* shmAttachImpl(int shmid, const void* addr, int shmFlag) {
  if (!isAddrAligned(addr) && (shmFlag & SHM_RND)) {
    util::throwSystemError(EINVAL, "address to attach must be page aligned");
  }

  void* retAddr = shmat(shmid, addr, shmFlag);
  if (retAddr != reinterpret_cast<void*>(-1)) {
    return retAddr;
  }

  switch (errno) {
  case EACCES:
  case EIDRM:
  case EINVAL:
  case ENOMEM:
    util::throwSystemError(
        errno,
        folly::sformat(
            "Failed to attach segment with key {} at addr {}, flags {}",
            shmid,
            addr,
            shmFlag));
    break;
  default:
    XDCHECK(false);
    util::throwSystemError(errno, "Invalid errno");
  }
  return nullptr;
}

void shmDtImpl(const void* addr) {
  if (!isAddrAligned(addr)) {
    util::throwSystemError(EINVAL, "Address not aligned");
  }

  const int ret = shmdt(addr);

  if (ret == 0) {
    return;
  }

  if (errno == EINVAL) {
    util::throwSystemError(errno, "Address not attached");
  }
}

void shmCtlImpl(int shmid, int cmd, shmid_ds* buf) {
  if (cmd != IPC_STAT && cmd != IPC_RMID && cmd != IPC_SET && cmd != SHM_LOCK) {
    util::throwSystemError(EINVAL, "Unsupported shmctl operation");
  }

  const int ret = shmctl(shmid, cmd, buf);
  if (ret == 0) {
    return;
  }

  switch (errno) {
  // EOVERFLOW and EACCES make sense only for stat
  case EOVERFLOW:
  case EACCES:
    XDCHECK_EQ(cmd, IPC_STAT);
    if (cmd != IPC_STAT) {
      util::throwSystemError(errno, "Invalid errno");
    }
  case EFAULT:
    util::throwSystemError(errno);
    break;
  case EINVAL:
  case EIDRM:
    // EINVAL can only mean invalid shmid in our case.
    util::throwSystemError(EIDRM, "Segment does not exist");
    break;
  case EPERM:
    if (cmd != IPC_RMID && cmd != IPC_SET) {
      XDCHECK(false);
      util::throwSystemError(errno, "Invalid errno");
    }
    util::throwSystemError(errno);
    break;
  case ENOMEM:
    XDCHECK_EQ(cmd, SHM_LOCK);
    if (cmd != SHM_LOCK) {
      util::throwSystemError(errno, "Invalid errno");
    }
    util::throwSystemError(errno, "Could not lock memory");
    break;
  default:
    XDCHECK(false);
    util::throwSystemError(errno, "Invalid errno");
  }
}

void mbindImpl(void* addr,
               unsigned long len,
               int mode,

               const NumaBitMask& memBindNumaNodes,
               unsigned int flags) {
  auto nodesMask = memBindNumaNodes.getNativeBitmask();

  long ret = mbind(addr, len, mode, nodesMask->maskp, nodesMask->size, flags);
  if (ret != 0) {
    util::throwSystemError(
        errno, folly::sformat("mbind() failed: {}", std::strerror(errno)));
  }
}

} // namespace detail

void ensureSizeforHugePage(size_t size) {
  if (size % kHugePageSize) {
    util::throwSystemError(EINVAL, "Page not aligned to Huge page size");
  }
}

int SysVShmSegment::createNewSegment(key_t key,
                                     size_t size,
                                     const ShmSegmentOpts& opts) {
  size = detail::getPageAlignedSize(size, opts.pageSize);
  int extraFlags = 0;

  // size is required and expected to be non zero, page aligned
  XDCHECK(detail::isPageAlignedSize(size, opts.pageSize));

#ifndef SHM_HUGE_2MB
#define SHM_HUGE_2MB (21 << MAP_HUGE_SHIFT)
#endif

#ifndef SHM_HUGE_1GB
#define SHM_HUGE_1GB (30 << MAP_HUGE_SHIFT)
#endif

  if (opts.pageSize == PageSizeT::TWO_MB) {
    extraFlags |= SHM_HUGETLB | SHM_HUGE_2MB;
  } else if (opts.pageSize == PageSizeT::ONE_GB) {
    extraFlags |= SHM_HUGETLB | SHM_HUGE_1GB;
  }

  const int flags = IPC_CREAT | IPC_EXCL | kModeRWFlags | extraFlags;
  return detail::shmGetImpl(key, size, flags);
}

int SysVShmSegment::attachToExisting(key_t key, const ShmSegmentOpts& opts) {
  // size is optional for attach. using anything smaller than existing will
  // attach.
  const int flags = opts.readOnly ? kModeRFlags : kModeRWFlags;
  return detail::shmGetImpl(key, 0, flags);
}

SysVShmSegment::SysVShmSegment(ShmAttachT,
                               const std::string& name,
                               ShmSegmentOpts opts)
    : ShmBase(std::move(opts), name),
      key_(createKeyForName(name)),
      shmid_(attachToExisting(key_, opts_)) {
  markActive();
  createReferenceMapping();
  XDCHECK(isActive());
}

SysVShmSegment::SysVShmSegment(ShmNewT,
                               const std::string& name,
                               size_t size,
                               ShmSegmentOpts opts)
    : ShmBase(std::move(opts), name),
      key_(createKeyForName(name)),
      shmid_(createNewSegment(key_, size, opts_)) {
  markActive();
  createReferenceMapping();
  XDCHECK(isActive());
}

void* SysVShmSegment::mapAddress(void* addr) const {
  if (!isActive()) {
    util::throwSystemError(EINVAL, "Attaching to invalid segment");
  }

  if (!detail::isPageAlignedAddr(addr, opts_.pageSize)) {
    util::throwSystemError(EINVAL, "Unaligned address");
  }

  int shmFlags = 0; // 0 means RW in shmat
  // If users pass in an address, they must make sure that address is unused.
  if (addr != nullptr) {
    shmFlags |= SHM_REMAP;
  }

  if (opts_.readOnly) {
    shmFlags |= SHM_RDONLY;
  }

  void* retAddr = detail::shmAttachImpl(shmid_, addr, shmFlags);
  XDCHECK(retAddr == addr || addr == nullptr);
  memBind(retAddr);
  return retAddr;
}

void SysVShmSegment::unMap(void* addr) const { detail::shmDtImpl(addr); }

void SysVShmSegment::memBind(void* addr) const {
  if (opts_.memBindNumaNodes.empty()) {
    return;
  }
  detail::mbindImpl(addr, getSize(), MPOL_BIND, opts_.memBindNumaNodes, 0);
}

void SysVShmSegment::markForRemoval() {
  if (isMarkedForRemoval()) {
    return;
  }

  detail::shmCtlImpl(shmid_, IPC_RMID, nullptr);
  markForRemove();
}

size_t SysVShmSegment::getSize() const {
  shmid_ds buf = {};
  detail::shmCtlImpl(shmid_, IPC_STAT, &buf);
  return buf.shm_segsz;
}

bool SysVShmSegment::removeByName(const std::string& name) {
  try {
    auto key = createKeyForName(name);
    const int shmid = detail::shmGetImpl(key, 0, kModeRWFlags);
    detail::shmCtlImpl(shmid, IPC_RMID, nullptr);
    return true;
  } catch (const std::system_error& e) {
    if (e.code().value() != ENOENT) {
      throw;
    }
    return false;
  }
}

key_t SysVShmSegment::createKeyForName(const std::string& name) noexcept {
  // we dont need ftok.
  static_assert(std::is_same<KeyType, key_t>::value,
                "key type is incompatible");

  // key_t is an int
  return folly::hash::fnv32(name);
}

void SysVShmSegment::createReferenceMapping() {
  referenceMapping_ = detail::shmAttachImpl(shmid_, nullptr, SHM_RDONLY);
  const int rv = mprotect(referenceMapping_, getSize(), PROT_NONE);
  if (rv != 0) {
    util::throwSystemError(errno, "Failure to mprotect reference mapping");
  }
}

void SysVShmSegment::deleteReferenceMapping() const {
  if (referenceMapping_ != nullptr) {
    detail::shmDtImpl(referenceMapping_);
  }
}
} // namespace cachelib
} // namespace facebook

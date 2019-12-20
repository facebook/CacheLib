#include "cachelib/shm/PosixShmSegment.h"

#include <fcntl.h>

#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <folly/logging/xlog.h>

#include "cachelib/common/Utils.h"

namespace facebook {
namespace cachelib {

constexpr static mode_t kRWMode = 0666;
typedef struct stat stat_t;

namespace detail {

int shmOpenImpl(const char* name, int flags) {
  const int fd = shm_open(name, flags, kRWMode);

  if (fd != -1) {
    return fd;
  }

  switch (errno) {
  case EEXIST:
  case EMFILE:
  case ENFILE:
  case EACCES:
    util::throwSystemError(errno);
    break;
  case ENAMETOOLONG:
  case EINVAL:
    util::throwSystemError(errno, "Invalid segment name");
    break;
  case ENOENT:
    if (!(flags & O_CREAT)) {
      util::throwSystemError(errno);
    } else {
      XDCHECK(false);
      // FIXME: posix says that ENOENT is thrown only when O_CREAT
      // is not set. However, it seems to be set even when O_CREAT
      // was set and the parent of path name does not exist.
      util::throwSystemError(errno, "Invalid errno");
    }
    break;
  default:
    XDCHECK(false);
    util::throwSystemError(errno, "Invalid errno");
  }
  return kInvalidFD;
}

void unlinkImpl(const char* const name) {
  const int ret = shm_unlink(name);
  if (ret == 0) {
    return;
  }

  switch (errno) {
  case ENOENT:
  case EACCES:
    util::throwSystemError(errno);
    break;
  case ENAMETOOLONG:
  case EINVAL:
    util::throwSystemError(errno, "Invalid segment name");
    break;
  default:
    XDCHECK(false);
    util::throwSystemError(errno, "Invalid errno");
  }
}

void ftruncateImpl(int fd, size_t size) {
  const int ret = ftruncate(fd, size);
  if (ret == 0) {
    return;
  }
  switch (errno) {
  case EBADF:
  case EINVAL:
    util::throwSystemError(errno);
    break;
  default:
    XDCHECK(false);
    util::throwSystemError(errno, "Invalid errno");
  }
}

void fstatImpl(int fd, stat_t* buf) {
  const int ret = fstat(fd, buf);
  if (ret == 0) {
    return;
  }
  switch (errno) {
  case EBADF:
  case ENOMEM:
  case EOVERFLOW:
    util::throwSystemError(errno);
    break;
  default:
    XDCHECK(false);
    util::throwSystemError(errno, "Invalid errno");
  }
}

void* mmapImpl(
    void* addr, size_t length, int prot, int flags, int fd, off_t offset) {
  void* ret = mmap(addr, length, prot, flags, fd, offset);
  if (ret != MAP_FAILED) {
    return ret;
  }

  switch (errno) {
  case EACCES:
  case EAGAIN:
    if (flags & MAP_LOCKED) {
      util::throwSystemError(ENOMEM);
      break;
    }
  case EBADF:
  case EINVAL:
  case ENFILE:
  case ENODEV:
  case ENOMEM:
  case EPERM:
  case ETXTBSY:
  case EOVERFLOW:
    util::throwSystemError(errno);
    break;
  default:
    XDCHECK(false);
    util::throwSystemError(errno, "Invalid errno");
  }
  return nullptr;
}

void munmapImpl(void* addr, size_t length) {
  const int ret = munmap(addr, length);

  if (ret == 0) {
    return;
  } else if (errno == EINVAL) {
    util::throwSystemError(errno);
  } else {
    XDCHECK(false);
    util::throwSystemError(EINVAL, "Invalid errno");
  }
}

} // namespace detail

PosixShmSegment::PosixShmSegment(ShmAttachT,
                                 const std::string& name,
                                 ShmSegmentOpts opts)
    : ShmBase(std::move(opts), createKeyForName(name)),
      fd_(getExisting(getName(), opts_)) {
  XDCHECK_NE(fd_, kInvalidFD);
  markActive();
  createReferenceMapping();
}

PosixShmSegment::PosixShmSegment(ShmNewT,
                                 const std::string& name,
                                 size_t size,
                                 ShmSegmentOpts opts)
    : ShmBase(std::move(opts), createKeyForName(name)),
      fd_(createNewSegment(getName())) {
  markActive();
  resize(size);
  XDCHECK(isActive());
  XDCHECK_NE(fd_, kInvalidFD);
  // this ensures that the segment lives while the object lives.
  createReferenceMapping();
}

PosixShmSegment::~PosixShmSegment() {
  try {
    // delete the reference mapping so the segment can be deleted if its
    // marked to be.
    deleteReferenceMapping();
  } catch (const std::system_error& e) {
  }

  // need to close the fd without throwing any exceptions. so we call close
  // directly.
  if (fd_ != kInvalidFD) {
    const int ret = close(fd_);
    if (ret != 0) {
      XDCHECK_NE(errno, EIO);
      XDCHECK_NE(errno, EINTR);
      XDCHECK_EQ(errno, EBADF);
      XDCHECK(!errno);
    }
  }
}

int PosixShmSegment::createNewSegment(const std::string& name) {
  constexpr static int createFlags = O_RDWR | O_CREAT | O_EXCL;
  return detail::shmOpenImpl(name.c_str(), createFlags);
}

int PosixShmSegment::getExisting(const std::string& name,
                                 const ShmSegmentOpts& opts) {
  int flags = opts.readOnly ? O_RDONLY : O_RDWR;
  return detail::shmOpenImpl(name.c_str(), flags);
}

void PosixShmSegment::markForRemoval() {
  if (isActive()) {
    // we still have the fd open. so we can use it to perform ftruncate
    // even after marking for removal through unlink. The fd does not get
    // recycled until we actually destroy this object.
    removeByName(getName());
    markForRemove();
  } else {
    XDCHECK(false);
  }
}

bool PosixShmSegment::removeByName(const std::string& segmentName) {
  try {
    auto key = createKeyForName(segmentName);
    detail::unlinkImpl(key.c_str());
    return true;
  } catch (const std::system_error& e) {
    // unlink is opaque unlike sys-V api where its through the shmid. Hence
    // if someone has already unlinked it for us, we just let it pass.
    if (e.code().value() != ENOENT) {
      throw;
    }
    return false;
  }
}

size_t PosixShmSegment::getSize() const {
  if (isActive() || isMarkedForRemoval()) {
    stat_t buf = {};
    detail::fstatImpl(fd_, &buf);
    return buf.st_size;
  } else {
    throw std::runtime_error(folly::sformat(
        "Trying to get size of  segment with name {} in an invalid state",
        getName()));
  }
  return 0;
}

void PosixShmSegment::resize(size_t size) const {
  size = detail::getPageAlignedSize(size, opts_.pageSize);
  XDCHECK(isActive() || isMarkedForRemoval());
  if (isActive() || isMarkedForRemoval()) {
    XDCHECK_NE(fd_, kInvalidFD);
    detail::ftruncateImpl(fd_, size);
  } else {
    throw std::runtime_error(folly::sformat(
        "Trying to resize segment with name {} in an invalid state",
        getName()));
  }
}

void* PosixShmSegment::mapAddress(void* addr) const {
  size_t size = getSize();
  if (!detail::isPageAlignedSize(size, opts_.pageSize) ||
      !detail::isPageAlignedAddr(addr, opts_.pageSize)) {
    util::throwSystemError(EINVAL, "Address/size not aligned");
  }

#ifndef MAP_HUGE_2MB
#define MAP_HUGE_2MB (21 << MAP_HUGE_SHIFT)
#endif

#ifndef MAP_HUGE_1GB
#define MAP_HUGE_1GB (30 << MAP_HUGE_SHIFT)
#endif

  int flags = MAP_SHARED;
  if (opts_.pageSize == PageSizeT::TWO_MB) {
    flags |= MAP_HUGETLB | MAP_HUGE_2MB;
  } else if (opts_.pageSize == PageSizeT::ONE_GB) {
    flags |= MAP_HUGETLB | MAP_HUGE_1GB;
  }
  // If users pass in an address, they must make sure that address is unused.
  if (addr != nullptr) {
    flags |= MAP_FIXED;
  }

  const int prot = opts_.readOnly ? PROT_READ : PROT_WRITE | PROT_READ;

  void* retAddr = detail::mmapImpl(addr, size, prot, flags, fd_, 0);
  // if there was hint for mapping, then fail if we cannot respect this
  // because we want to be specific about mapping to exactly that address.
  if (retAddr != nullptr && addr != nullptr && retAddr != addr) {
    util::throwSystemError(EINVAL, "Address already mapped");
  }
  XDCHECK(retAddr == addr || addr == nullptr);
  return retAddr;
}

void PosixShmSegment::unMap(void* addr) const {
  detail::munmapImpl(addr, getSize());
}

std::string PosixShmSegment::createKeyForName(
    const std::string& name) noexcept {
  // ensure that the slash is always there in the head. repetitive
  // slash is fine.
  if (name.empty() || name[0] != '/') {
    return "/" + name;
  } else {
    return name;
  }
}

void PosixShmSegment::createReferenceMapping() {
  // create a mapping that lasts the life of this object. mprotect it to
  // ensure there are no actual accesses.
  referenceMapping_ = detail::mmapImpl(
      nullptr, detail::getPageSize(), PROT_NONE, MAP_SHARED, fd_, 0);
  XDCHECK(referenceMapping_ != nullptr);
}

void PosixShmSegment::deleteReferenceMapping() const {
  if (referenceMapping_ != nullptr) {
    detail::munmapImpl(referenceMapping_, detail::getPageSize());
  }
}
} // namespace cachelib
} // namespace facebook

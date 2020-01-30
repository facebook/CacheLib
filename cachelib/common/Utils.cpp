#include <fstream>
#include <stdexcept>

#include <dirent.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <sys/types.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#include <folly/Format.h>
#pragma GCC diagnostic pop
#include <folly/Random.h>

#include "cachelib/common/Utils.h"
namespace facebook {
namespace cachelib {
namespace util {

namespace {

constexpr size_t kPageSizeBytes = 4096;
const char* kProcShmMaxPath = "/proc/sys/kernel/shmmax";
const char* kProcShmAllPath = "/proc/sys/kernel/shmall";

// Get the value of kernel.shmmax in bytes.
// This function must be run as root.
//
// Use this to determine how much shm we can allocate on this system,
// if it's too low, the user may want to use `setShmMax` to set a higher
// limit.
uint64_t getShmMax() {
  std::ifstream shmMaxFile(kProcShmMaxPath);
  if (!shmMaxFile) {
    return 0;
  }

  uint64_t shmMax;
  if (shmMaxFile >> shmMax) {
    return shmMax;
  }

  return 0;
}

// Set the value of kernel.shmmax. This function must be run as root.
//
// @param shmMax  new shmmax value in bytes
//
// @return true on success, false otherwise
bool setShmMax(uint64_t shmMax) {
  std::ofstream shmMaxFile(kProcShmMaxPath, std::ios_base::trunc);
  if (!shmMaxFile) {
    return false;
  }

  shmMaxFile << shmMax;
  shmMaxFile.flush();
  return shmMaxFile.good();
}

// Get the value of kernel.shmall in number of pages.
// This function must be run as root.
//
// Use this to determine how much shm we can allocate *in total* on this system,
// if it's too low, the user may want to use `setShmAll` to set a higher
// limit.
uint64_t getShmAll() {
  std::ifstream shmAllFile(kProcShmAllPath);
  if (!shmAllFile) {
    return 0;
  }

  uint64_t shmAll;
  if (shmAllFile >> shmAll) {
    return shmAll;
  }

  return 0;
}

// Set the value of kernel.shmall. This function must be run as root.
//
// "shmall" is suggested to be set to "shmmax / PAGE_SIZE"
//
// @param shmAll  new shmmax value in bytes
//
// @return true on success, false otherwise
bool setShmAll(uint64_t shmAll) {
  std::ofstream shmAllFile(kProcShmAllPath, std::ios_base::trunc);
  if (!shmAllFile) {
    return false;
  }

  shmAllFile << shmAll;
  shmAllFile.flush();
  return shmAllFile.good();
}

} // namespace

void setShmIfNecessary(uint64_t bytes) {
  const auto curShmMax = getShmMax();
  if (curShmMax < bytes && !setShmMax(bytes)) {
    throw std::system_error(
        ENOMEM,
        std::system_category(),
        folly::sformat("Cannot set shmmax to {} from {}", bytes, curShmMax));
  }

  const auto curShmAll = getShmAll();
  if (curShmAll * kPageSizeBytes < bytes) {
    // always set this to be just bigger than shmmax
    const auto desiredNumPages = bytes / kPageSizeBytes + 1;
    if (!setShmAll(desiredNumPages)) {
      throw std::system_error(ENOMEM,
                              std::system_category(),
                              folly::sformat("Cannot set shmall to {} from {}",
                                             desiredNumPages, curShmAll));
    }
  }
}

void* align(size_t alignment, size_t size, void*& ptr, size_t& space) {
  assert((alignment & (alignment - 1)) == 0);
  const size_t alignmentMask = ~(alignment - 1);
  auto alignedPtr = reinterpret_cast<uint8_t*>(
      (reinterpret_cast<uintptr_t>(ptr) & alignmentMask));

  // not properly aligned so set it to the next aligned address.
  if (alignedPtr != ptr) {
    alignedPtr += alignment;
  }

  const ptrdiff_t diff = alignedPtr - reinterpret_cast<uint8_t*>(ptr);

  if ((diff + size) <= space) {
    ptr = reinterpret_cast<void*>(alignedPtr);
    space -= diff;
    return ptr;
  }
  return nullptr;
}

uint32_t getAlignedSize(uint32_t size, uint32_t alignment) {
  const auto rem = size % alignment;
  return rem == 0 ? size : size + alignment - rem;
}

void* mmapAlignedZeroedMemory(size_t alignment,
                              size_t numBytes,
                              bool noAccess) {
  // to enforce alignment, we try to make sure that the address we return is
  // aligned to slab size.
  size_t newBytes = numBytes + alignment;
  const auto protFlag = noAccess ? PROT_NONE : PROT_READ | PROT_WRITE;
  const auto mapFlag = MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE;
  void* memory = mmap(nullptr, newBytes, protFlag, mapFlag, -1, 0);
  if (memory != MAP_FAILED) {
    auto alignedMemory = align(alignment, numBytes, memory, newBytes);
    assert(alignedMemory != nullptr);
    return alignedMemory;
  }
  throw std::system_error(errno, std::system_category(), "Cannot mmap");
}

void setMaxLockMemory(uint64_t bytes) {
  struct rlimit rlim {
    bytes, bytes
  };
  const int rv = setrlimit(RLIMIT_MEMLOCK, &rlim);
  if (rv == 0) {
    return;
  }
  throw std::system_error(
      errno, std::system_category(),
      folly::sformat("Error setting rlimit to {} bytes. Errno = {}", bytes,
                     errno));
}

size_t getNumResidentPages(const void* memory, size_t len) {
  if (!isPageAlignedAddr(memory)) {
    throw std::invalid_argument(
        folly::sformat("addr {} is not page aligned", memory));
  }
  assert(isPageAlignedAddr(memory));
  const size_t numPages = getNumPages(len);

  // TODO this could be a large allocation. may be break it up if it matters.
  std::vector<unsigned char> vec(numPages, 0);
  const int rv = mincore(const_cast<void*>(memory), len, vec.data());
  if (rv != 0) {
    throw std::system_error(
        errno, std::system_category(),
        folly::sformat("Error in mincore addr = {}, len = {}. errno = {}",
                       memory, len, errno));
  }
  return std::count_if(vec.begin(), vec.end(),
                       [](unsigned char c) { return c != 0; });
}

size_t getPageSize() noexcept {
  static long pagesize = sysconf(_SC_PAGESIZE);
  assert(pagesize != -1);
  assert(pagesize > 0);
  return pagesize;
}

size_t getNumPages(size_t len) noexcept {
  return (len + getPageSize() - 1) / getPageSize();
}

bool isPageAlignedAddr(const void* addr) noexcept {
  return reinterpret_cast<uintptr_t>(addr) % getPageSize() == 0;
}

/* returns true with the file's mode. false if the file does not exist.
 * throws system_error for all other errors */
bool getStatIfExists(const std::string& name, mode_t* mode) {
  struct stat buf = {};
  const int ret = stat(name.c_str(), &buf);
  if (ret == 0) {
    if (mode != nullptr) {
      *mode = buf.st_mode;
    }
    return true;
  } else if (errno != ENOENT && errno != ENOTDIR) {
    // some system error, but it might exist
    throwSystemError(errno);
  }

  // does not exist;
  return false;
}

bool pathExists(const std::string& path) {
  return getStatIfExists(path, nullptr);
}

bool isDir(const std::string& name) {
  struct stat buf = {};
  auto err = stat(name.c_str(), &buf);
  if (err) {
    throwSystemError(errno);
  }
  return S_ISDIR(buf.st_mode) ? true : false;
}

/* throws error on any failure. */
void makeDir(const std::string& name) {
  auto mkdirs = [](const std::string& path, mode_t mode) {
    char tmp[256];
    char* p = nullptr;
    size_t len;

    snprintf(tmp, sizeof(tmp), "%s", path.c_str());
    len = strlen(tmp);
    if (len == 0) {
      throw std::invalid_argument(
          folly::sformat("Error forming path {}", path));
    }
    if (tmp[len - 1] == '/') {
      tmp[len - 1] = 0;
    }
    for (p = tmp + 1; *p; p++) {
      if (*p == '/') {
        *p = 0;
        SCOPE_EXIT { *p = '/'; };
        if (mkdir(tmp, mode) != 0 && errno != EEXIST) {
          throwSystemError(errno, folly::sformat("failed to create {}", tmp));
        }
        *p = '/';
      }
    }
    // error checked by caller
    mkdir(tmp, mode);
  };

  mkdirs(name.c_str(), 0777);
  if (!getStatIfExists(name, nullptr)) {
    throwSystemError(errno, folly::sformat("{} expected to be existing", name));
  }
}

/* throws error on any failure. */
void removePath(const std::string& name) {
  if (isDir(name)) {
    auto dir = opendir(name.c_str());
    if (!dir) {
      throwSystemError(errno);
    }
    SCOPE_EXIT { free(dir); };
    struct dirent* entry;
    while ((entry = readdir(dir))) {
      if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
        continue;
      }
      std::string path = name + "/" + std::string(entry->d_name);
      removePath(path);
    }
    auto err = rmdir(name.c_str());
    if (err) {
      throwSystemError(errno);
    }
  } else {
    auto err = unlink(name.c_str());
    if (err) {
      throwSystemError(errno);
    }
  }
}

std::string getUniqueTempDir(folly::StringPiece prefix) {
  const char* dir = getenv("TMPDIR");
  if (dir == nullptr) {
    dir = "/tmp";
  }
  return folly::sformat("{}/{}_{}", dir, prefix, folly::Random::rand32());
}

std::string toString(std::chrono::nanoseconds d) {
  // bunch of constants to help us round up and convert with appropriate
  // suffix. For example say 90ns or 5.6us or 5.5ms or 55s etc
  constexpr uint64_t micros =
      std::chrono::nanoseconds(std::chrono::microseconds(1)).count();
  constexpr uint64_t millis =
      std::chrono::nanoseconds(std::chrono::milliseconds(1)).count();
  constexpr uint64_t secs =
      std::chrono::nanoseconds(std::chrono::seconds(1)).count();

  uint64_t count = d.count();
  if (count < micros) {
    return folly::sformat("{}ns", count);
  } else if (count < millis) {
    return folly::sformat("{:.2f}us", static_cast<double>(count) / micros);
  } else if (count < secs) {
    return folly::sformat("{:.2f}ms", static_cast<double>(count) / millis);
  } else {
    return folly::sformat("{:.2f}s", static_cast<double>(count) / secs);
  }
}

} // namespace util
} // namespace cachelib
} // namespace facebook

#include <sys/mman.h>
#include <sys/stat.h>

#include <stdexcept>

#include <folly/File.h>
#include <folly/Format.h>
#include <folly/logging/xlog.h>

#include "cachelib/allocator/Util.h"

namespace facebook {
namespace cachelib {

// Simple wrapper that opens a file and memory maps it to the size specified.
// File is mmaped to slab aligned address to make it work with the
// MemoryAllocator.
class MemoryMappedFile {
  static constexpr int kInvalidFd = -1;

 public:
  MemoryMappedFile() {}

  // @param name   name of the file
  // @param size   size of the file and the memory mapping
  MemoryMappedFile(const std::string& name, size_t size)
      : file_(name, O_RDWR), size_(size) {
    const auto fileSize = getFileSize();
    if (size_ != fileSize) {
      throw std::invalid_argument(folly::sformat(
          "File size {} does not match expected size {}", fileSize, size_));
    }

    // We first mmap an area of memory that is our cache size and use that to
    // an aligned address that we will later remap to a file backed mapping.
    auto addr =
        util::mmapAlignedZeroedMemory(Slab::kSize, size_, true /* readOnly */);
    XDCHECK_EQ(0u, reinterpret_cast<uintptr_t>(addr) % Slab::kSize);
    const int flags = MAP_SHARED | MAP_FIXED;
    addr_ = ::mmap(addr, size_, PROT_READ | PROT_WRITE, flags, file_.fd(), 0);
    if (addr_ != addr) {
      throw std::runtime_error(
          folly::sformat("Failed to mmap file {} at {}", name, addr));
    }

    // we will have random access and should not dump any core for the large
    // memory map.
    if (::madvise(addr_, size_, MADV_RANDOM) ||
        ::madvise(addr_, size_, MADV_DONTDUMP)) {
      XLOG(ERR, "Failed to madvise cache memory");
    }
  }

  MemoryMappedFile(const MemoryMappedFile&) = delete;
  MemoryMappedFile& operator=(const MemoryMappedFile&) = delete;
  MemoryMappedFile(MemoryMappedFile&&) = delete;
  MemoryMappedFile& operator=(MemoryMappedFile&&) = delete;

  // return the slab aligned address and size
  void* addr() const { return addr_; }
  size_t size() const { return size_; }

  ~MemoryMappedFile() {
    if (addr_) {
      ::munmap(addr_, size_);
    }
  }

 private:
  // returns the file size
  size_t getFileSize() {
    struct stat buf;
    if (fstat(file_.fd(), &buf)) {
      throw std::system_error(errno, std::system_category());
    }
    return buf.st_size;
  }

  // open file
  folly::File file_;

  // address the file is mapped at
  void* addr_{nullptr};

  // size of the mapping
  const size_t size_{0};
};
} // namespace cachelib
} // namespace facebook

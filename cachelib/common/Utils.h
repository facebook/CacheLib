#pragma once

#include <folly/Format.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#include <folly/Range.h>
#pragma GCC diagnostic pop
#include <folly/FileUtil.h>
#include <folly/chrono/Hardware.h>

namespace facebook {
namespace cachelib {
namespace util {

// Provides an RAII wrapper around sysctl settings
class SysctlSetting {
 public:
  explicit SysctlSetting(const std::string& settingName,
                         const std::string& settingValue,
                         bool restoreOldValue = true)
      : settingName_(settingName), restoreOldValue_(restoreOldValue) {
    if (restoreOldValue_) {
      oldValue_ = get(settingName_);
    }
    set(settingName, settingValue);
  }

  ~SysctlSetting() {
    if (restoreOldValue_) {
      try {
        set(settingName_, oldValue_);
      } catch (const std::exception&) {
      }
    }
  }

  static std::string get(const std::string& settingName) {
    std::string value;
    if (!readSysctl(settingName, value)) {
      throw std::runtime_error(
          folly::sformat("Failed to read sysctl setting {}", settingName));
    }
    return value;
  }

  static void set(const std::string& settingName,
                  const std::string& settingValue) {
    if (!writeSysctl(settingName, settingValue)) {
      throw std::runtime_error(
          folly::sformat("Failed to write sysctl setting {}", settingName));
    }
  }

 private:
  static std::string sysctlPathName(const std::string& nodeName) {
    std::string ret("/proc/sys/");

    for (const auto c : nodeName) {
      if (c == '.') {
        ret += '/';
      } else {
        ret += c;
      }
    }
    return ret;
  }

  static bool readSysctl(const std::string& nodeName, std::string& content) {
    auto path = sysctlPathName(nodeName);
    return folly::readFile(path.c_str(), content);
  }

  static bool writeSysctl(const std::string& nodeName,
                          const std::string& content) {
    auto path = sysctlPathName(nodeName);
    return folly::writeFile(content, path.c_str());
  }

  std::string settingName_;
  std::string oldValue_;
  bool restoreOldValue_{true};
};

// This function will set the appropriate shm settings if necessary for
// the minimum required shared memory an user needs to allocate
//
// @param bytes  the minimum amount of shared memory needed in this process
//
// @throw std::system_error if unable to set shm
void setShmIfNecessary(uint64_t bytes);

// Set the system limit for max locked memory. This should be done as root to
// avoid failures in raising the limit.
//
// @param bytes  the new soft limit for max locked memory
//
// @throw std::system_error on failure
void setMaxLockMemory(uint64_t bytes);

// implementation of std::align since gcc 4.9 and clang don't have it supported
// yet.
//
// @param alignment   the desired alignment
// @param size        the size of the requested aligned memory
// @param ptr         pointer to the memory
// @param space       size of the memory pointed by ptr
//
// @return  pointer to aligned memory or nullptr on error.
//          on success, ptr and space are updated accordingly
void* align(size_t alignment, size_t size, void*& ptr, size_t& space);

// @return size aligned up to the next multiple of _alignment_
uint32_t getAlignedSize(uint32_t size, uint32_t alignment);

// creates a new mapping in the virtual address space of the calling process
// aligned by the size of Slab.
//
// @param alignment   the desired alignment
// @param numBytes    the length of the mapping
// @param noAccess    whether or not this mapping is going to be accessed
// @return    pointer to aligned memory or nullptr on error
//
// @throw std::system_error if unable to create mapping
void* mmapAlignedZeroedMemory(size_t alignment,
                              size_t numBytes,
                              bool noAccess = false);

// get the number of pages in the range which are resident in the process.
//
// @param mem   memory start which is page aligned
// @param len   length of the memory.
//
// @return number of pages that are resident
// @throw std::system_error on any error determining
size_t getNumResidentPages(const void* mem, size_t len);

// return the page size of the system
size_t getPageSize() noexcept;

// return the number of pages spanning len bytes of memory starting from a
// page aligned address
size_t getNumPages(size_t len) noexcept;

// return true if the memory is page aligned.
bool isPageAlignedAddr(const void* addr) noexcept;

/* returns true with the file's mode. false if the file does not exist.
 * throws system_error for all other errors */
bool getStatIfExists(const std::string& name, mode_t* mode);

// returns true if the path edxists and false if not.
bool pathExists(const std::string& path);

/* throws error on any failure. */
void makeDir(const std::string& name);

/* Removes the directory/file contents and the given directory recursively if
 * it is a directory.
 *
 * WARNING: Be extremely careful to avoid deleting more than you
 * expect. Check that the directory name is correct so that you don't
 * end up deleting root or home directory!
 *
 * throws error on failure deleting files inside the directory or
 * the directory itself. */
void removePath(const std::string& name);

// returns true if the path exists and is a directory. false if the path is a
// file. throws error if the path does not exist or any other error
bool isDir(const std::string& path);

// return a random path to temp directory  with the prefix
std::string getUniqueTempDir(folly::StringPiece prefix);

template <typename... Args>
void throwSystemError(int err, const Args&... args) {
  throw std::system_error(err, std::system_category(), args...);
}

// stringify the duration specified in nano seconds into an appropriate form.
// For example 5us or 5ns or 5s, or 5h
std::string toString(std::chrono::nanoseconds d);

// returns the current process's RSS size in bytes. Returns 0 upon any error.
// Caller is supposed to treat 0 values as errors.
size_t getRSSBytes();

// returns the current mem-available reported by the kernel. 0 means an error.
size_t getMemAvailable();

} // namespace util
} // namespace cachelib
} // namespace facebook

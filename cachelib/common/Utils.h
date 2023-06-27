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

#include <folly/Format.h>
#include <folly/Random.h>
#include <numa.h>
#include <numaif.h>

#include <unordered_map>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#include <folly/Range.h>
#pragma GCC diagnostic pop
#include <folly/FileUtil.h>
#include <folly/chrono/Hardware.h>
#include <folly/logging/xlog.h>

#include <numeric>

namespace facebook {
namespace cachelib {
namespace util {

class NumaBitMask {
 public:
  using native_bitmask_type = struct bitmask*;

  NumaBitMask() { nodesMask = numa_allocate_nodemask(); }

  NumaBitMask(const NumaBitMask& other) {
    nodesMask = numa_allocate_nodemask();
    copy_bitmask_to_bitmask(other.nodesMask, nodesMask);
  }

  NumaBitMask(NumaBitMask&& other) {
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

// A wrapper class for functions to collect counters.
// It can be initialized by either
// 1. folly::StringPiece, double -> void, or
// 2. folly::StringPiece, double, CounterType.
// This allows counters to be collected and aggregated differently.
class CounterVisitor {
 public:
  enum CounterType {
    COUNT /* couters whose value can be exported directly */,
    RATE /* counters whose value should be exported by delta */
  };

  CounterVisitor() { init(); }

  /* implicit */ CounterVisitor(
      std::function<void(folly::StringPiece, double)> biFn)
      : biFn_(std::move(biFn)) {
    init();
  }

  /* implicit */ CounterVisitor(
      std::function<void(folly::StringPiece, double, CounterType)> triFn)
      : triFn_(std::move(triFn)) {
    init();
  }

  void operator()(folly::StringPiece name,
                  double count,
                  CounterType type) const {
    XDCHECK_NE(nullptr, triFn_);
    triFn_(name, count, type);
  }

  void operator()(folly::StringPiece name, double count) const {
    XDCHECK_NE(nullptr, biFn_);
    biFn_(name, count);
  }

  void operator=(std::function<void(folly::StringPiece, double)> biFn) {
    biFn_ = biFn;
    triFn_ = nullptr;
    init();
  }

  void operator=(
      std::function<void(folly::StringPiece, double, CounterType)> triFn) {
    triFn_ = triFn;
    biFn_ = nullptr;
    init();
  }

 private:
  // Initialize so that at most one of the functions is initialized.
  void init() {
    if (biFn_ && triFn_) {
      throw std::invalid_argument(
          "CounterVisitor can have at most one single function initialized.");
    }
    if (biFn_) {
      triFn_ = [this](folly::StringPiece name, double count, CounterType) {
        biFn_(name, count);
      };
    } else if (triFn_) {
      biFn_ = [this](folly::StringPiece name, double count) {
        triFn_(name, count, CounterType::COUNT);
      };
    } else {
      // Create noop functions.
      triFn_ = [](folly::StringPiece, double, CounterType) {};
      biFn_ = [](folly::StringPiece, double) {};
    }
  }

  // Function to collect all counters by value (COUNT).
  std::function<void(folly::StringPiece name, double count)> biFn_;
  // Function to collect counters by type.
  std::function<void(folly::StringPiece name, double count, CounterType type)>
      triFn_;
};

// A class to collect stats into, consisting of a map for counts and a map for
// rates. Together with CounterVisitor, counters can be collected into the two
// maps according to their types.
class StatsMap {
 public:
  StatsMap() {}
  StatsMap(const StatsMap&) = delete;
  StatsMap(StatsMap&& o) noexcept {
    countMap = std::move(o.countMap);
    rateMap = std::move(o.rateMap);
  }

  void operator=(StatsMap&& o) noexcept {
    countMap = std::move(o.countMap);
    rateMap = std::move(o.rateMap);
  }

  // Insert a count stat
  void insertCount(std::string key, double val) { countMap[key] = val; }

  // Insert a rate stat
  void insertRate(std::string key, double val) { rateMap[key] = val; }

  const std::unordered_map<std::string, double>& getCounts() const {
    return countMap;
  }

  const std::unordered_map<std::string, double>& getRates() const {
    return rateMap;
  }

  // Return an unordered map.
  std::unordered_map<std::string, double> toMap() const {
    std::unordered_map<std::string, double> ret;
    ret.insert(countMap.begin(), countMap.end());
    ret.insert(rateMap.begin(), rateMap.end());
    return ret;
  }

  CounterVisitor createCountVisitor() {
    return {[this](folly::StringPiece key,
                   double val,
                   CounterVisitor::CounterType type) {
      if (type == CounterVisitor::CounterType::COUNT) {
        insertCount(key.str(), val);
      } else {
        insertRate(key.str(), val);
      }
    }};
  }

 private:
  std::unordered_map<std::string, double> countMap;
  std::unordered_map<std::string, double> rateMap;
};

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
template <typename T>
std::enable_if_t<std::is_arithmetic<T>::value, T> getAlignedSize(
    T size, uint32_t alignment) {
  const T rem = size % alignment;
  return rem == 0 ? size : size + alignment - rem;
}

// @return ceiling of the quotient
template <typename T>
std::enable_if_t<std::is_arithmetic<T>::value, T> getDivCeiling(
    T dividend, uint32_t divisor) {
  return (dividend + divisor - 1) / divisor;
}
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

// destroy the mapping created by mmapAlignedZeroedMemory
//
// @param addr  the pointer to the memory to unmap
// @param size  size of the memory region
void munmapMemory(void* addr, size_t size);

// binds memory to the NUMA nodes specified by nmask.
//
// @param addr  the pointer to the memory to bind.
// @param len   length of the memory.
// @param mode  mode supported by mmap call
// @param mask  mask specifies node ids
// @param flags flags supported by mmap call
void mbindMemory(void* addr,
                 unsigned long len,
                 int mode,
                 const NumaBitMask& mask,
                 unsigned int flags);

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

// returns true if the path exists and is a regular file
bool isBlk(const std::string& name);

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

// Print stack trace for the current exception thrown
void printExceptionStackTraces();

// Return max or min value if the double is outside of type's range
template <typename T>
T narrow_cast(double i) {
  if (i > static_cast<double>(std::numeric_limits<T>::max())) {
    return std::numeric_limits<T>::max();
  } else if (i < static_cast<double>(std::numeric_limits<T>::min())) {
    return std::numeric_limits<T>::min();
  }
  return static_cast<T>(i);
}

template <typename T>
std::pair<double, double> getMeanDeviation(std::vector<T> v) {
  double sum = std::accumulate(v.begin(), v.end(), 0.0);
  double mean = sum / v.size();

  double accum = 0.0;
  std::for_each(v.begin(), v.end(), [&](const T& d) {
    accum += ((double)d - mean) * ((double)d - mean);
  });

  return std::make_pair(mean, sqrt(accum / v.size()));
}

// To force the compiler to NOT optimize away the store/load
// when user supplies void* and we need to read it in 32bit chunks.
// The compiler should be able to optimize this into just a single load.
inline uint32_t strict_aliasing_safe_read32(const void* ptr) {
  uint32_t result;
  memcpy(&result, ptr, sizeof(result));
  return result;
}

// To force the compiler to NOT optimize away the store/load
// when user supplies void* and we need to read it in 64bit chunks.
// The compiler should be able to optimize this into just a single load.
inline uint64_t strict_aliasing_safe_read64(const void* ptr) {
  uint64_t result;
  memcpy(&result, ptr, sizeof(result));
  return result;
}
} // namespace util
} // namespace cachelib
} // namespace facebook

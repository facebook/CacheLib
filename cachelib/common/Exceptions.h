#pragma once

#include <stdexcept>
#include <string>

#ifndef CACHELIB_CHECK_THROW
#define CACHELIB_CHECK_THROW(cond, msg)          \
  do {                                           \
    if (UNLIKELY(!(cond))) {                     \
      XLOG(ERR, "CHECK FAILED: " #cond "," msg); \
      throw std::invalid_argument(msg);          \
    }                                            \
  } while (0)
#endif

#ifndef CACHELIB_CHECK_THROWF
#define CACHELIB_CHECK_THROWF(cond, fmt, arg1, ...)                          \
  do {                                                                       \
    if (UNLIKELY(!(cond))) {                                                 \
      XLOGF(ERR, "CHECK FAILED: " #cond "," fmt, arg1, ##__VA_ARGS__);       \
      throw std::invalid_argument(folly::sformat(fmt, arg1, ##__VA_ARGS__)); \
    }                                                                        \
  } while (0)
#endif

namespace facebook {
namespace cachelib {
namespace exception {
class OutOfMemory : public std::bad_alloc {
 public:
  OutOfMemory(std::string what) : what_{std::move(what)} {}

  const char* what() const noexcept override { return what_.c_str(); }

 private:
  const std::string what_;
};

class RefcountOverflow : public std::overflow_error {
 public:
  using std::overflow_error::overflow_error;
};

class RefcountUnderflow : public std::underflow_error {
 public:
  using std::underflow_error::underflow_error;
};

class SlabReleaseAborted : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

// An allocation error. This could be a genuine std::bad_alloc from
// the global allocator, or it can be an internal allocation error
// from the backing cachelib item.
class ObjectCacheAllocationError : public OutOfMemory {
 public:
  using OutOfMemory::OutOfMemory;
};

// Bad arguments were fed into deallocate(). This indicates the alloc
// argument was invalid, or the size was different from the originally
// requested size.
class ObjectCacheDeallocationBadArgs : public std::invalid_argument {
  using std::invalid_argument::invalid_argument;
};
} // namespace exception
} // namespace cachelib
} // namespace facebook

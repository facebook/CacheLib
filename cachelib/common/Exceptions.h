#pragma once

#include <stdexcept>
#include <string>

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

} // namespace exception
} // namespace cachelib
} // namespace facebook

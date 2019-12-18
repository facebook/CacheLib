#pragma once

#include <stdexcept>
#include <string>

namespace facebook {
namespace cachelib {
namespace cachebench {
class EndOfTrace : public std::out_of_range {
 public:
  using std::out_of_range::out_of_range;
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook

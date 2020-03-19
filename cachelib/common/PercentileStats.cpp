#include "cachelib/common/PercentileStats.h"

namespace facebook {
namespace cachelib {
namespace util {
const std::array<double, 14> PercentileStats::kQuantiles{
    0,    0.05, 0.1,   0.25,   0.5,     0.75,     0.9,
    0.95, 0.99, 0.999, 0.9999, 0.99999, 0.999999, 1.0};
constexpr int PercentileStats::kDefaultWindowSize;
} // namespace util
} // namespace cachelib
} // namespace facebook

#include "cachelib/allocator/PoolOptimizeStrategy.h"

namespace facebook {
namespace cachelib {

const PoolOptimizeContext PoolOptimizeStrategy::kNoOpContext = {
    Slab::kInvalidPoolId, Slab::kInvalidPoolId};

} // namespace cachelib
} // namespace facebook

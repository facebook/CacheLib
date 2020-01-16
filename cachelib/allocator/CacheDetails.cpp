#include "cachelib/allocator/CacheDetails.h"
namespace facebook {
namespace cachelib {
namespace detail {

const std::string kShmInfoName = "shm_info";
const std::string kShmCacheName = "shm_cache";
const std::string kShmHashTableName = "shm_hash_table";
const std::string kShmChainedItemHashTableName = "shm_chained_alloc_hash_table";

} // namespace detail
} // namespace cachelib
} // namespace facebook

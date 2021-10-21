// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

namespace facebook {
namespace rust {
namespace cachelib {

bool enable_container_memory_monitor(LruAllocatorConfig& config) {
  // We don't know how to do this outside Facebook, yet.
  return false;
}

} // namespace cachelib
} // namespace rust
} // namespace facebook

#include "NvmTestBase.h"

#include <folly/synchronization/Baton.h>

#include "cachelib/allocator/NvmCacheState.h"
#include "dipper/dipper_registry.h"

namespace facebook {
namespace cachelib {
namespace tests {

NavyDipper::NavyDipper() {
  path_ = boost::filesystem::unique_path(std::string("nvmcache_navydipper") +
                                         ".%%%%-%%%%-%%%%");

  const auto dipperFilePath = "/tmp" / path_;
  boost::filesystem::create_directories(dipperFilePath);
  const auto space = boost::filesystem::space(dipperFilePath);
  if (space.available < 400 * 1024 * 1024) {
    throw std::runtime_error("400MB is required for navy setup in test mode");
  }

  config_ = folly::dynamic::object;
  config_["dipper_backend"] = "navy_dipper";
  config_["dipper_navy_recovery_path"] = dipperFilePath.string();
  config_["dipper_fs_size"] = 100; /* megabytes */
  config_["dipper_navy_file_name"] = (dipperFilePath / "navy").string();
  config_["dipper_navy_direct_io"] = false;
  config_["dipper_navy_region_size"] = 4 * 1024 * 1024;   /* 4 MB */
  config_["dipper_navy_metadata_size"] = 4 * 1024 * 1024; /* 4 MB */
  config_["dipper_navy_lru"] = true;
  config_["dipper_navy_block_size"] = 1024;
  config_["dipper_test_mode"] = true;
  config_["dipper_request_ordering"] = false;
  config_["dipper_navy_req_order_shards_power"] = 10;
  config_["dipper_num_ushards"] = 1;
}

NavyDipper::~NavyDipper() { boost::filesystem::remove_all("/tmp" / path_); }

template <typename B>
NvmCacheTest<B>::NvmCacheTest() {
  facebook::dipper::registerBackend<facebook::dipper::NavyDipperFactory>();
  cacheDir_ =
      boost::filesystem::unique_path("/tmp/cachedir-%%%%-%%%%-%%%%").string();
  {
    allocConfig_.enableCachePersistence(cacheDir_);
    allocConfig_.setRemoveCallback(
        [this](const LruAllocator::RemoveCbData&) { nEvictions_++; });
    allocConfig_.setCacheSize(20 * 1024 * 1024);

    // Disable slab rebalancing
    allocConfig_.enablePoolRebalancing(nullptr, std::chrono::seconds{0});

    LruAllocator::NvmCacheConfig nvmConfig;
    nvmConfig.dipperOptions = backend_.getOptions();
    allocConfig_.enableNvmCache(nvmConfig);
  }
  makeCache();
}

template <typename B>
AllocatorT& NvmCacheTest<B>::makeCache() {
  cache_.reset();
  cache_ = std::make_unique<LruAllocator>(allocConfig_);
  id_ = cache_->addPool("default", poolSize_, poolAllocsizes_);
  return *cache_;
}

template <typename B>
NvmCacheTest<B>::~NvmCacheTest() {
  boost::filesystem::remove_all(cacheDir_);
}

template <typename B>
bool NvmCacheTest<B>::checkKeyExists(folly::StringPiece key, bool ramOnly) {
  return ramOnly ? cache_->peek(key) != nullptr : fetch(key, false) != nullptr;
}

template <typename B>
ItemHandle NvmCacheTest<B>::fetch(folly::StringPiece key, bool ramOnly) {
  auto hdl = ramOnly ? cache_->findFast(key, AccessMode::kRead)
                     : cache_->find(key, AccessMode::kRead);
  hdl.wait();
  return hdl;
}

template <typename B>
GlobalCacheStats NvmCacheTest<B>::getStats() const {
  return cache_->getGlobalCacheStats();
}

template <typename B>
void NvmCacheTest<B>::convertToShmCache() {
  cache_.reset();
  cache_ =
      std::make_unique<LruAllocator>(LruAllocator::SharedMemNew, allocConfig_);
  id_ = cache_->addPool("default", poolSize_, poolAllocsizes_);
}

template <typename B>
void NvmCacheTest<B>::warmRoll() {
  if (cache_->shutDown() != LruAllocator::ShutDownStatus::kSuccess) {
    throw std::runtime_error("Failed to warm roll");
  }
  cache_.reset();
  cache_ = std::make_unique<LruAllocator>(LruAllocator::SharedMemAttach,
                                          allocConfig_);
}

template <typename B>
void NvmCacheTest<B>::coldRoll() {
  // to simulate a cold roll, we shutdown safely and then explicitly create a
  // new one for the ram part
  if (cache_->shutDown() != LruAllocator::ShutDownStatus::kSuccess) {
    throw std::runtime_error("Failed to cold roll");
  }
  cache_ =
      std::make_unique<LruAllocator>(LruAllocator::SharedMemNew, allocConfig_);
  id_ = cache_->addPool("default", poolSize_, poolAllocsizes_);
}

template <typename B>
void NvmCacheTest<B>::iceRoll() {
  // shutdown with warm roll and indicatae that we want to drop dipper
  if (cache_->shutDown() != LruAllocator::ShutDownStatus::kSuccess) {
    throw std::runtime_error("Failed to ice roll");
  }
  cache_.reset();

  const auto fileName = NvmCacheState::getFileForNvmCacheDrop(cacheDir_);
  {
    std::ofstream dropFile(fileName, std::ios::trunc);
    dropFile.flush();
  }

  if (!util::getStatIfExists(fileName, nullptr)) {
    throw std::runtime_error(
        folly::sformat("Failed to create drop file {}", fileName));
  }

  cache_ = std::make_unique<LruAllocator>(LruAllocator::SharedMemAttach,
                                          allocConfig_);
}

template <typename B>
void NvmCacheTest<B>::iceColdRoll() {
  // shutdown with cold roll and indicate that we want to drop dipper
  cache_.reset();

  const auto fileName = NvmCacheState::getFileForNvmCacheDrop(cacheDir_);
  {
    std::ofstream dropFile(fileName, std::ios::trunc);
    dropFile.flush();
  }

  if (!util::getStatIfExists(fileName, nullptr)) {
    throw std::runtime_error(
        folly::sformat("Failed to create drop file {}", fileName));
  }

  cache_ =
      std::make_unique<LruAllocator>(LruAllocator::SharedMemNew, allocConfig_);
  id_ = cache_->addPool("default", poolSize_, poolAllocsizes_);
}

} // namespace tests
} // namespace cachelib
} // namespace facebook

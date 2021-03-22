#include <folly/File.h>
#include <gtest/gtest.h>

#include "cachelib/allocator/nvmcache/NavyConfig.h"
#include "cachelib/allocator/nvmcache/NavySetup.h"
#include "cachelib/common/Utils.h"

namespace facebook {
namespace cachelib {
TEST(NavySetupTest, RAID0DeviceSize) {
  // Verify size is reduced when we pass in a size that's not aligned to
  // stripeSize for RAID0Device

  auto filePath =
      folly::sformat("/tmp/navy_device_raid0io_test-{}", ::getpid());
  util::makeDir(filePath);
  SCOPE_EXIT { util::removePath(filePath); };

  std::vector<std::string> files = {filePath + "/CACHE0", filePath + "/CACHE1",
                                    filePath + "/CACHE2", filePath + "/CACHE3"};

  int size = 9 * 1024 * 1024;
  int ioAlignSize = 4096;
  int stripeSize = 8 * 1024 * 1024;

  folly::dynamic navyFileArray = folly::dynamic::array;
  for (const auto& file : files) {
    navyFileArray.push_back(file);
  }

  folly::dynamic cfg = folly::dynamic::object;
  cfg["dipper_navy_raid_paths"] = navyFileArray;
  cfg["dipper_navy_file_size"] = size;
  cfg["dipper_navy_region_size"] = stripeSize;
  cfg["dipper_navy_block_size"] = ioAlignSize;
  cfg["dipper_navy_truncate_file"] = true;

  auto device = createDevice(cfg, nullptr);
  EXPECT_GT(size * files.size(), device->getSize());
  EXPECT_EQ(files.size() * 8 * 1024 * 1024, device->getSize());
}

TEST(NavySetupTest, RAID0DeviceSize2) {
  // Verify size is reduced when we pass in a size that's not aligned to
  // stripeSize for RAID0Device

  auto filePath =
      folly::sformat("/tmp/navy_device_raid0io_test-{}", ::getpid());
  util::makeDir(filePath);
  SCOPE_EXIT { util::removePath(filePath); };

  std::vector<std::string> files = {filePath + "/CACHE0", filePath + "/CACHE1",
                                    filePath + "/CACHE2", filePath + "/CACHE3"};

  int size = 9 * 1024 * 1024;
  int ioAlignSize = 4096;
  int stripeSize = 8 * 1024 * 1024;

  std::vector<std::string> navyFileArray;
  for (const auto& file : files) {
    navyFileArray.push_back(file);
  }

  navy::NavyConfig cfg{};
  cfg.setRaidPaths(navyFileArray);
  cfg.setFileSize(size);
  cfg.setBlockCacheRegionSize(stripeSize);
  cfg.setBlockSize(ioAlignSize);
  cfg.setTruncateFile(true);

  auto device = createDevice(cfg, nullptr);
  EXPECT_GT(size * files.size(), device->getSize());
  EXPECT_EQ(files.size() * 8 * 1024 * 1024, device->getSize());
}
} // namespace cachelib
} // namespace facebook

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

// How to run the huge-page matrix/persistence tests.
//
// These tests need real 2MB huge pages. Without setup they degrade
// gracefully: combos that cannot allocate are skipped, and the no-pool /
// no-mount paths assert that creation throws for the right reason.
// To run them for real, follow Setup, Run, Cleanup exactly as below.
//
// Setup (needs root):
//   1. Reserve 2MB huge pages (22 needed; ask 256) and verify:
//        P=/sys/kernel/mm/hugepages/hugepages-2048kB
//        echo 256 | sudo tee $P/nr_hugepages && cat $P/nr_hugepages
//      If the number comes back short, the box is fragmented;
//      compact and repeat the verify line:
//        echo 1 | sudo tee /proc/sys/vm/compact_memory
//   2. Mount a 2MB hugetlbfs and make a user-owned subdir (ignore
//      "already mounted" errors on repeat runs):
//        sudo mkdir -p /mnt/huge
//        sudo mount -t hugetlbfs -o pagesize=2M none /mnt/huge
//        sudo mkdir -p /mnt/huge/$USER && sudo chown $USER /mnt/huge/$USER
//   3. Allow non-root SysV huge segments:
//        echo $(id -g) | sudo tee /proc/sys/vm/hugetlb_shm_group
//
// Run (no root needed):
//   CACHELIB_TEST_HUGETLBFS_MOUNT=/mnt/huge/$USER buck2 test \
//     fbcode//cachelib/allocator/tests:huge-page-matrix-test
// A full run passes everything with zero skips; any skip names the
// missing setup.
//
// Cleanup (only needed when a run aborts or crashes; green runs remove
// their own segments):
//   1. Remove your detached SysV segments (attached segments and
//      other users' segments are untouched):
//        ipcs -m | grep " $USER " | awk '$6==0{print $2}'| xargs -r ipcrm -m
//   2. Remove leaked POSIX files:
//        rm -f /mnt/huge/$USER/*
//   3. Release the pool and restore defaults (needs root):
//        P=/sys/kernel/mm/hugepages/hugepages-2048kB
//        echo 0 | sudo tee $P/nr_hugepages
//        echo 0 | sudo tee /proc/sys/vm/hugetlb_shm_group
//        sudo umount /mnt/huge

#include <folly/testing/TestUtil.h>

#include <functional>
#include <tuple>

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/allocator/CacheAllocatorConfig.h"
#include "cachelib/allocator/CacheDetails.h"
#include "cachelib/allocator/memory/Slab.h"
#include "cachelib/shm/HugePageTestUtils.h"
#include "cachelib/shm/ShmCommon.h"

namespace facebook {
namespace cachelib {
namespace tests {

using AllocatorT = LruAllocator;

namespace {
constexpr size_t kHugePageSize = PageSize::kHugePageSize2MB;
const size_t kCacheSize = 10 * Slab::kSize;
constexpr unsigned kMainBucketsPower = 12;
constexpr unsigned kChainedBucketsPower = 10;

enum class HugePageBacking { kHeap, kTmp, kSysV, kPosix };

bool kernelSupportsMatrixHugePages() {
  return PageSize::supportedHugePageSizes().contains(kHugePageSize);
}

MemoryMonitor::Config advisingMonitorConfig() {
  MemoryMonitor::Config memConfig;
  memConfig.mode = MemoryMonitor::ResidentMemory;
  memConfig.maxAdvisePercent = 10;
  memConfig.lowerLimitGB = 1;
  memConfig.upperLimitGB = 10;
  return memConfig;
}

size_t hugeBytesNeeded(bool slabHuge, bool accessHuge) {
  const PageSize huge{kHugePageSize};
  size_t need = 0;
  if (slabHuge) {
    need += huge.getPageAlignedSize(kCacheSize);
  }
  if (accessHuge) {
    need += huge.getPageAlignedSize(
        AllocatorT::AccessContainer::getRequiredSize(1u << kMainBucketsPower));
    need +=
        huge.getPageAlignedSize(AllocatorT::AccessContainer::getRequiredSize(
            1u << kChainedBucketsPower));
  }
  return need;
}

AllocatorT::Config makeConfig(HugePageBacking backing,
                              bool slabHuge,
                              bool accessHuge,
                              const std::string& cacheDir,
                              const std::string& mount) {
  AllocatorT::Config config;
  config.setCacheSize(kCacheSize);
  config.setAccessConfig({kMainBucketsPower, 6});
  config.configureChainedItems({kChainedBucketsPower, 5});
  const PageSize huge{kHugePageSize};
  if (slabHuge && accessHuge) {
    config.enableHugePages(huge, mount);
  } else if (slabHuge) {
    config.enableSlabHugePages(huge, mount);
  } else if (accessHuge) {
    config.enableAccessHugePages(huge, mount);
  }
  // Attach compares mount against persisted metadata even for normal pages.
  if (!mount.empty()) {
    config.hugePageMountDir = mount;
  }
  if (backing == HugePageBacking::kTmp) {
    config.enableMemoryMonitor(std::chrono::seconds{3600},
                               advisingMonitorConfig());
  } else if (backing == HugePageBacking::kSysV ||
             backing == HugePageBacking::kPosix) {
    config.enableCachePersistence(cacheDir);
    if (backing == HugePageBacking::kPosix) {
      config.usePosixForShm();
    }
  }
  return config;
}

bool canAllocateHugeCombo(HugePageBacking backing,
                          size_t needBytes,
                          const std::string& mount) {
  if (backing == HugePageBacking::kPosix && mount.empty()) {
    return false;
  }
  return canReserveHugePages(needBytes, PageSize{kHugePageSize});
}

// True if the combo creates a SysV HugeTLB segment (gated by the
// hugetlb_shm_group sysctl). SysV backs both slab and access tables with SysV
// segments; temp shm uses a SysV segment for slabs only (its access tables
// are heap mappings).
bool sysvHugeTlbNeeded(HugePageBacking backing,
                       bool slabHuge,
                       bool accessHuge) {
  return (backing == HugePageBacking::kSysV && (slabHuge || accessHuge)) ||
         (backing == HugePageBacking::kTmp && slabHuge);
}

void expectThrowsWithMsg(std::function<void()> fn, const std::string& substr) {
  try {
    fn();
    FAIL() << "expected throw containing: " << substr;
  } catch (const std::invalid_argument& e) {
    EXPECT_NE(std::string(e.what()).find(substr), std::string::npos);
  }
}

void expectAttachThrowsConfigChange(const AllocatorT::Config& config) {
  expectThrowsWithMsg(
      [&] { AllocatorT alloc(AllocatorT::SharedMemAttach, config); },
      "Huge page config changed");
}

void warmRollRoundTrip(bool posix, bool slabHuge, bool accessHuge) {
  folly::test::TemporaryDirectory tmpDir;
  const auto cacheDir = tmpDir.path().string();
  const std::string mount =
      posix ? detail::hugetlbfsMountFromEnv() : std::string{};
  const auto backing = posix ? HugePageBacking::kPosix : HugePageBacking::kSysV;
  const auto config =
      makeConfig(backing, slabHuge, accessHuge, cacheDir, mount);
  {
    AllocatorT alloc(AllocatorT::SharedMemNew, config);
    const auto poolId =
        alloc.addPool("default", alloc.getCacheMemoryStats().ramCacheSize);
    for (int i = 0; i < 10; ++i) {
      auto h = alloc.allocate(poolId, folly::to<std::string>(i), 100);
      EXPECT_TRUE(h);
      alloc.insertOrReplace(h);
    }
    alloc.shutDown();
  }
  {
    AllocatorT alloc(AllocatorT::SharedMemAttach, config);
    for (int i = 0; i < 10; ++i) {
      EXPECT_TRUE(
          alloc.find(typename AllocatorT::Key{folly::to<std::string>(i)}));
    }
  }
}
} // namespace

class HugePageMatrixTest
    : public ::testing::TestWithParam<std::tuple<HugePageBacking, bool, bool>> {
 protected:
  void runBackingPagesCombo();
};

void HugePageMatrixTest::runBackingPagesCombo() {
  const auto [backing, slabHuge, accessHuge] = GetParam();
  const bool wantHuge = slabHuge || accessHuge;
  if (wantHuge && !kernelSupportsMatrixHugePages()) {
    GTEST_SKIP() << "2MB huge pages not supported by kernel";
  }

  folly::test::TemporaryDirectory tmpDir;
  const auto cacheDir = tmpDir.path().string();
  const std::string mount = backing == HugePageBacking::kPosix
                                ? detail::hugetlbfsMountFromEnv()
                                : std::string{};
  auto config = makeConfig(backing, slabHuge, accessHuge, cacheDir, mount);
  const size_t needBytes = hugeBytesNeeded(slabHuge, accessHuge);

  if (wantHuge && !canAllocateHugeCombo(backing, needBytes, mount)) {
    if (backing == HugePageBacking::kSysV &&
        detail::totalHugePages(kHugePageSize) == 0) {
      expectThrowsWithMsg(
          [&] { AllocatorT alloc(AllocatorT::SharedMemNew, config); },
          "Unable to create shared memory segment");
      return;
    }
    if (backing == HugePageBacking::kPosix && mount.empty()) {
      expectThrowsWithMsg(
          [&] { AllocatorT alloc(AllocatorT::SharedMemNew, config); },
          "hugetlbfs mount");
      return;
    }
    GTEST_SKIP() << "insufficient free huge pages";
  }

  if (sysvHugeTlbNeeded(backing, slabHuge, accessHuge) &&
      !sysvHugeTlbPermitted(kHugePageSize)) {
    GTEST_SKIP() << "SysV HugeTLB creation not permitted for this user; set "
                    "the hugetlb_shm_group sysctl to include them";
  }

  const size_t slabExpected =
      slabHuge ? kHugePageSize : PageSize::systemPageSize();
  const size_t accessExpected =
      accessHuge ? kHugePageSize : PageSize::systemPageSize();
  auto checkPages = [&](AllocatorT& alloc) {
    EXPECT_EQ(backing != HugePageBacking::kHeap, alloc.isOnShm());
    const auto slabInfo = alloc.getSlabMemoryInfo();
    EXPECT_EQ(slabExpected,
              PageSize::getPageSizeInSMap(const_cast<void*>(slabInfo.first)));
    void* mainBuckets = nullptr;
    void* chainedBuckets = nullptr;
    if (backing == HugePageBacking::kSysV ||
        backing == HugePageBacking::kPosix) {
      mainBuckets =
          alloc.shmManager_
              ->getShmByName(facebook::cachelib::detail::kShmHashTableName)
              .getCurrentMapping()
              .addr;
      chainedBuckets =
          alloc.shmManager_
              ->getShmByName(
                  facebook::cachelib::detail::kShmChainedItemHashTableName)
              .getCurrentMapping()
              .addr;
    } else {
      mainBuckets =
          const_cast<void*>(alloc.accessContainer_->getBucketMemory());
      chainedBuckets = const_cast<void*>(
          alloc.chainedItemAccessContainer_->getBucketMemory());
    }
    EXPECT_EQ(accessExpected, PageSize::getPageSizeInSMap(mainBuckets));
    EXPECT_EQ(accessExpected, PageSize::getPageSizeInSMap(chainedBuckets));
  };

  if (backing == HugePageBacking::kSysV || backing == HugePageBacking::kPosix) {
    AllocatorT alloc(AllocatorT::SharedMemNew, config);
    checkPages(alloc);
  } else {
    AllocatorT alloc(config);
    checkPages(alloc);
  }
}

TEST_P(HugePageMatrixTest, BackingPages) { runBackingPagesCombo(); }

std::string hugePageMatrixName(
    const ::testing::TestParamInfo<HugePageMatrixTest::ParamType>& info) {
  const auto [backing, slabHuge, accessHuge] = info.param;
  std::string name;
  switch (backing) {
  case HugePageBacking::kHeap:
    name = "Heap";
    break;
  case HugePageBacking::kTmp:
    name = "Tmp";
    break;
  case HugePageBacking::kSysV:
    name = "SysV";
    break;
  case HugePageBacking::kPosix:
    name = "Posix";
    break;
  }
  name += slabHuge ? "_SlabHuge" : "_SlabNormal";
  name += accessHuge ? "_AccessHuge" : "_AccessNormal";
  return name;
}

INSTANTIATE_TEST_SUITE_P(
    All,
    HugePageMatrixTest,
    ::testing::Combine(::testing::Values(HugePageBacking::kHeap,
                                         HugePageBacking::kTmp,
                                         HugePageBacking::kSysV,
                                         HugePageBacking::kPosix),
                       ::testing::Bool(),
                       ::testing::Bool()),
    hugePageMatrixName);

class HugePagePersistenceTest : public testing::Test {};

TEST_F(HugePagePersistenceTest, WarmRollSameSettings) {
  for (bool posix : {false, true}) {
    for (bool slabHuge : {false, true}) {
      for (bool accessHuge : {false, true}) {
        const bool wantHuge = slabHuge || accessHuge;
        const std::string mount =
            posix ? detail::hugetlbfsMountFromEnv() : std::string{};
        const auto backing =
            posix ? HugePageBacking::kPosix : HugePageBacking::kSysV;
        if (wantHuge &&
            (!kernelSupportsMatrixHugePages() ||
             !canAllocateHugeCombo(
                 backing, hugeBytesNeeded(slabHuge, accessHuge), mount) ||
             (sysvHugeTlbNeeded(backing, slabHuge, accessHuge) &&
              !sysvHugeTlbPermitted(kHugePageSize)))) {
          continue;
        }
        warmRollRoundTrip(posix, slabHuge, accessHuge);
      }
    }
  }
}

TEST_F(HugePagePersistenceTest, ConfigChangeForcesColdRoll) {
  if (!kernelSupportsMatrixHugePages()) {
    GTEST_SKIP() << "2MB huge pages not supported by kernel";
  }
  for (bool posix : {false, true}) {
    const auto backing =
        posix ? HugePageBacking::kPosix : HugePageBacking::kSysV;
    for (bool slabHuge : {false, true}) {
      for (bool accessHuge : {false, true}) {
        if (!slabHuge && !accessHuge) {
          continue;
        }
        folly::test::TemporaryDirectory tmpDir;
        const auto cacheDir = tmpDir.path().string();
        {
          const auto createConfig =
              makeConfig(backing, false, false, cacheDir, "");
          AllocatorT alloc(AllocatorT::SharedMemNew, createConfig);
          alloc.shutDown();
        }
        const auto attachConfig =
            makeConfig(backing, slabHuge, accessHuge, cacheDir, "");
        expectAttachThrowsConfigChange(attachConfig);
      }
    }
  }
}

TEST_F(HugePagePersistenceTest, ReverseConfigChangeForcesColdRoll) {
  if (!kernelSupportsMatrixHugePages()) {
    GTEST_SKIP() << "2MB huge pages not supported by kernel";
  }
  for (bool posix : {false, true}) {
    const std::string mount =
        posix ? detail::hugetlbfsMountFromEnv() : std::string{};
    const auto backing =
        posix ? HugePageBacking::kPosix : HugePageBacking::kSysV;
    if (!canAllocateHugeCombo(backing, hugeBytesNeeded(true, true), mount) ||
        (sysvHugeTlbNeeded(backing, true, true) &&
         !sysvHugeTlbPermitted(kHugePageSize))) {
      continue;
    }
    folly::test::TemporaryDirectory tmpDir;
    const auto cacheDir = tmpDir.path().string();
    {
      const auto createConfig =
          makeConfig(backing, true, true, cacheDir, mount);
      AllocatorT alloc(AllocatorT::SharedMemNew, createConfig);
      alloc.shutDown();
    }
    const auto attachConfig =
        makeConfig(backing, false, false, cacheDir, mount);
    expectAttachThrowsConfigChange(attachConfig);
  }
}

} // namespace tests
} // namespace cachelib
} // namespace facebook

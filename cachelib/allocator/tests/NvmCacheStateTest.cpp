#include <gtest/gtest.h>

#include <fstream>
#include <thread>

#include <folly/io/RecordIO.h>
#include "cachelib/allocator/CacheVersion.h"
#include "cachelib/allocator/NvmCacheState.h"
#include "cachelib/allocator/serialize/gen-cpp2/objects_types.h"
#include "cachelib/common/Serialization.h"
#include "cachelib/common/Utils.h"

namespace facebook {
namespace cachelib {

class NvmCacheStateTest : public testing::Test {
 public:
  NvmCacheStateTest() : cacheDir_(util::getUniqueTempDir("NvmCacheTest")) {
    util::makeDir(cacheDir_);
  }

  ~NvmCacheStateTest() override {
    try {
      util::removePath(cacheDir_);
    } catch (...) {
    }
  }

  std::string getCacheDir() const { return cacheDir_; }

 private:
  std::string cacheDir_;
};

TEST_F(NvmCacheStateTest, FreshStart) {
  auto dir = getCacheDir();
  NvmCacheState s(dir, false /* encryption */, false /* truncateAllocSize */);

  // directory is empty at this point
  ASSERT_FALSE(s.shouldDropNvmCache());
  ASSERT_FALSE(s.wasCleanShutDown());
}

TEST_F(NvmCacheStateTest, ClearState) {
  auto dir = getCacheDir();

  {
    NvmCacheState s(dir, false /* encryption */, false /* truncateAllocSize */);

    // directory is empty at this point
    ASSERT_FALSE(s.wasCleanShutDown());
    s.markSafeShutDown();
    s.clearPrevState();
  }

  {
    NvmCacheState s(dir, false /* encryption */, false /* truncateAllocSize */);
    ASSERT_TRUE(s.shouldDropNvmCache());
    ASSERT_FALSE(s.wasCleanShutDown());
  }
}

TEST_F(NvmCacheStateTest, CreationTime) {
  auto dir = getCacheDir();

  time_t creationTime = 0;
  {
    NvmCacheState s(dir, false /* encryption */, false /* truncateAllocSize */);
    creationTime = s.getCreationTime();
    s.markSafeShutDown();
  }

  {
    NvmCacheState s(dir, false /* encryption */, false /* truncateAllocSize */);
    ASSERT_TRUE(s.wasCleanShutDown());
    ASSERT_EQ(creationTime, s.getCreationTime());
    s.clearPrevState();
  }

  {
    // Intentionally write a bad metadata file
    serialization::NvmCacheMetadata metadata;
    *metadata.nvmFormatVersion_ref() = kCacheNvmFormatVersion - 1;
    *metadata.creationTime_ref() = 12345;
    *metadata.safeShutDown_ref() = true;
    auto metadataIoBuf = Serializer::serializeToIOBuf(metadata);
    folly::File shutDownFile{folly::sformat("{}/{}", dir, "NvmCacheState"),
                             O_CREAT | O_TRUNC | O_RDWR};
    folly::RecordIOWriter rw{std::move(shutDownFile)};
    rw.write(std::move(metadataIoBuf));
  }

  {
    NvmCacheState s(dir, false /* encryption */, false /* truncateAllocSize */);
    ASSERT_TRUE(s.wasCleanShutDown());

    // Creation time is reset because of version mismatch
    ASSERT_NE(12345, s.getCreationTime());
  }
}

TEST_F(NvmCacheStateTest, Encryption) {
  auto dir = getCacheDir();

  {
    NvmCacheState s(dir, true /* encryption */, false /* truncateAllocSize */);
    s.markSafeShutDown();
  }

  {
    NvmCacheState s(dir, false /* encryption */, false /* truncateAllocSize */);
    ASSERT_TRUE(s.wasCleanShutDown());
    ASSERT_TRUE(s.shouldDropNvmCache());
  }

  {
    // Intentionally write a bad metadata file
    serialization::NvmCacheMetadata metadata;
    *metadata.nvmFormatVersion_ref() = kCacheNvmFormatVersion;
    *metadata.creationTime_ref() = 12345;
    *metadata.safeShutDown_ref() = true;
    *metadata.encryptionEnabled_ref() = true;
    auto metadataIoBuf = Serializer::serializeToIOBuf(metadata);
    folly::File shutDownFile{folly::sformat("{}/{}", dir, "NvmCacheState"),
                             O_CREAT | O_TRUNC | O_RDWR};
    folly::RecordIOWriter rw{std::move(shutDownFile)};
    rw.write(std::move(metadataIoBuf));
  }

  {
    NvmCacheState s(dir, false /* encryption */, false /* truncateAllocSize */);
    ASSERT_TRUE(s.wasCleanShutDown());
    ASSERT_TRUE(s.shouldDropNvmCache());
  }
}

TEST_F(NvmCacheStateTest, TruncateAllocSize) {
  auto dir = getCacheDir();

  {
    NvmCacheState s(dir, false /* encryption */, true /* truncateAllocSize */);
    s.markSafeShutDown();
  }

  {
    NvmCacheState s(dir, false /* encryption */, false /* truncateAllocSize */);
    ASSERT_TRUE(s.wasCleanShutDown());
    ASSERT_TRUE(s.shouldDropNvmCache());
  }

  {
    // Intentionally write a bad metadata file
    serialization::NvmCacheMetadata metadata;
    *metadata.nvmFormatVersion_ref() = kCacheNvmFormatVersion;
    *metadata.creationTime_ref() = 12345;
    *metadata.safeShutDown_ref() = true;
    *metadata.truncateAllocSize_ref() = true;
    auto metadataIoBuf = Serializer::serializeToIOBuf(metadata);
    folly::File shutDownFile{folly::sformat("{}/{}", dir, "NvmCacheState"),
                             O_CREAT | O_TRUNC | O_RDWR};
    folly::RecordIOWriter rw{std::move(shutDownFile)};
    rw.write(std::move(metadataIoBuf));
  }

  {
    NvmCacheState s(dir, false /* encryption */, false /* truncateAllocSize */);
    ASSERT_TRUE(s.wasCleanShutDown());
    ASSERT_TRUE(s.shouldDropNvmCache());
  }
}

TEST_F(NvmCacheStateTest, SafeShutDown) {
  auto dir = getCacheDir();

  time_t creationTime = 0;
  {
    NvmCacheState s(dir, false /* encryption */, false /* truncateAllocSize */);
    creationTime = s.getCreationTime();
    s.markSafeShutDown();
  }

  {
    NvmCacheState s(dir, false /* encryption */, false /* truncateAllocSize */);
    ASSERT_TRUE(s.wasCleanShutDown());
    ASSERT_EQ(creationTime, s.getCreationTime());
    s.clearPrevState();
  }

  {
    NvmCacheState s(dir, false /* encryption */, false /* truncateAllocSize */);
    ASSERT_FALSE(s.wasCleanShutDown());
  }
}

TEST_F(NvmCacheStateTest, SafeShutDownLegacy) {
  auto dir = getCacheDir();

  time_t creationTime = 0;
  {
    NvmCacheState s(dir, false /* encryption */, false /* truncateAllocSize */);
    creationTime = s.getCreationTime();
    s.markSafeShutDown();
    ::unlink("NvmCacheState");
  }

  {
    NvmCacheState s(dir, false /* encryption */, false /* truncateAllocSize */);
    ASSERT_TRUE(s.wasCleanShutDown());
    ASSERT_EQ(creationTime, s.getCreationTime());
    s.clearPrevState();
  }

  {
    NvmCacheState s(dir, false /* encryption */, false /* truncateAllocSize */);
    ASSERT_FALSE(s.wasCleanShutDown());
  }
}

TEST_F(NvmCacheStateTest, Drop) {
  auto dir = getCacheDir();

  time_t creationTime = 0;
  {
    NvmCacheState s(dir, false /* encryption */, false /* truncateAllocSize */);
    creationTime = s.getCreationTime();
    s.markSafeShutDown();
  }
  {
    auto dropFile = NvmCacheState::getFileForNvmCacheDrop(dir);
    std::ofstream f(dropFile, std::ios::trunc);
    f.flush();
    ASSERT_TRUE(util::getStatIfExists(dropFile, nullptr));
  }

  {
    std::this_thread::sleep_for(std::chrono::seconds{1});
    NvmCacheState s(dir, false /* encryption */, false /* truncateAllocSize */);
    ASSERT_TRUE(s.shouldDropNvmCache());
    // we explicitly marked that shutdown was fine.
    ASSERT_TRUE(s.wasCleanShutDown());
    ASSERT_NE(creationTime, s.getCreationTime());
    s.clearPrevState();
  }

  {
    NvmCacheState s(dir, false /* encryption */, false /* truncateAllocSize */);
    ASSERT_TRUE(s.shouldDropNvmCache());
    ASSERT_FALSE(s.wasCleanShutDown());
  }
}

TEST_F(NvmCacheStateTest, Truncated) {
  auto dir = getCacheDir();

  time_t creationTime = 0;
  {
    NvmCacheState s(dir, false /* encryption */, false /* truncateAllocSize */);
    creationTime = s.getCreationTime();
    s.markSafeShutDown();
  }

  {
    std::this_thread::sleep_for(std::chrono::seconds{1});
    NvmCacheState s(dir, false /* encryption */, false /* truncateAllocSize */);
    // we explicitly marked that shutdown was fine.
    ASSERT_TRUE(s.wasCleanShutDown());
    ASSERT_EQ(creationTime, s.getCreationTime());
    s.markTruncated();
    ASSERT_NE(creationTime, s.getCreationTime());
    s.markSafeShutDown();
  }

  {
    NvmCacheState s(dir, false /* encryption */, false /* truncateAllocSize */);
    ASSERT_FALSE(s.shouldDropNvmCache());
    ASSERT_TRUE(s.wasCleanShutDown());
  }
}

} // namespace cachelib
} // namespace facebook

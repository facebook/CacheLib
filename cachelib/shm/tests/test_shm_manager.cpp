#include <sys/time.h>
#include <fstream>

#include <folly/FileUtil.h>

#include "cachelib/common/Utils.h"
#include "cachelib/shm/PosixShmSegment.h"
#include "cachelib/shm/ShmManager.h"
#include "cachelib/shm/SysVShmSegment.h"
#include "cachelib/shm/tests/common.h"

static const std::string namePrefix = "shm-test";
using namespace facebook::cachelib::tests;

using facebook::cachelib::ShmManager;

using ShutDownRes = typename facebook::cachelib::ShmManager::ShutDownRes;

class ShmManagerTest : public ShmTestBase {
 public:
  ShmManagerTest() : cacheDir(dirPrefix + std::to_string(::getpid())) {}

  const std::string cacheDir{};
  std::vector<std::string> segmentsToDestroy{};

 protected:
  void SetUp() final {
    // make sure nothing exists at the start
    facebook::cachelib::util::removePath(cacheDir);
  }

  void TearDown() final {
    try {
      clearAllSegments();
    } catch (const std::exception& e) {
      // ignore
    }

    try {
      facebook::cachelib::util::removePath(cacheDir);
      // make sure nothing exists at the end
    } catch (const std::exception& e) {
      // ignore
    }
  }

  virtual void clearAllSegments() = 0;

  /*
   * We define the generic test here that can be run by the appropriate
   * specification of the test fixture by their shm type
   */
  void testInvalidCachedDir(bool posix);
  void testInvalidMetaFile(bool posix);
  void testEmptyMetaFile(bool posix);
  void testSegments(bool posix);
  void testStaticCleanup(bool posix);
  void testDropFile(bool posix);
  void testInvalidType(bool posix);
  void testRemove(bool posix);
  void testShutDown(bool posix);
  void testCleanup(bool posix);
  void testAttachReadOnly(bool posix);
  void testMetaFileDeletion(bool posix);

 private:
  const static std::string dirPrefix;
};

class ShmManagerTestSysV : public ShmManagerTest {
 public:
  void clearAllSegments() override {
    for (const auto& seg : segmentsToDestroy) {
      ShmManager::removeByName(cacheDir, seg, false);
    }
  }
};

class ShmManagerTestPosix : public ShmManagerTest {
 public:
  void clearAllSegments() override {
    for (const auto& seg : segmentsToDestroy) {
      ShmManager::removeByName(cacheDir, seg, true);
    }
  }
};

const std::string ShmManagerTest::dirPrefix = "/tmp/shm-test";

void ShmManagerTest::testMetaFileDeletion(bool posix) {
  const std::string segmentName = std::to_string(::getpid());
  const std::string segmentName2 = segmentName + "-2";
  segmentsToDestroy.push_back(segmentName);
  segmentsToDestroy.push_back(segmentName2);
  const size_t size = getRandomSize();
  const unsigned char magicVal = 'g';
  // start the session with the first type and create some segments.
  auto addr = getNewUnmappedAddr();
  {
    ShmManager s(cacheDir, posix);
    auto m = s.createShm(segmentName, size, addr);

    writeToMemory(m.addr, m.size, magicVal);
    checkMemory(m.addr, m.size, magicVal);

    // delete the file before shutdown
    ASSERT_TRUE(facebook::cachelib::util::pathExists(cacheDir + "/metadata"));
    facebook::cachelib::util::removePath(cacheDir + "/metadata");
    ASSERT_FALSE(facebook::cachelib::util::pathExists(cacheDir + "/metadata"));
    // trying to shutdown with the file deleted should fail.
    ASSERT_TRUE(s.shutDown() == ShutDownRes::kFileDeleted);
  }

  ASSERT_TRUE(facebook::cachelib::util::pathExists(cacheDir));
  ASSERT_FALSE(facebook::cachelib::util::pathExists(cacheDir + "/metadata"));

  // now try to attach and that should fail.
  {
    ShmManager s(cacheDir, posix);
    ASSERT_THROW(s.attachShm(segmentName), std::invalid_argument);
    auto m = s.createShm(segmentName, size, addr);
    checkMemory(m.addr, m.size, 0);
    writeToMemory(m.addr, m.size, magicVal);
    checkMemory(m.addr, m.size, magicVal);
    ASSERT_TRUE(s.shutDown() == ShutDownRes::kSuccess);
  }

  // delete files after shutdown and this should result in same behavior
  ASSERT_TRUE(facebook::cachelib::util::pathExists(cacheDir));
  ASSERT_TRUE(facebook::cachelib::util::pathExists(cacheDir + "/metadata"));
  facebook::cachelib::util::removePath(cacheDir + "/metadata");
  ASSERT_FALSE(facebook::cachelib::util::pathExists(cacheDir + "/metadata"));

  // now try to attach and that should fail.
  {
    ShmManager s(cacheDir, posix);
    ASSERT_THROW(s.attachShm(segmentName), std::invalid_argument);
    auto m = s.createShm(segmentName, size, addr);
    checkMemory(m.addr, m.size, 0);
    writeToMemory(m.addr, m.size, magicVal);
    checkMemory(m.addr, m.size, magicVal);
    ASSERT_TRUE(s.shutDown() == ShutDownRes::kSuccess);
  }

  // The segment managed by us could get deleted outside from us. This should
  // not cause us trouble when we delete the meta file
  {
    ShmManager s(cacheDir, posix);
    ASSERT_NO_THROW({
      const auto m = s.attachShm(segmentName, addr);
      writeToMemory(m.addr, m.size, magicVal);
      checkMemory(m.addr, m.size, magicVal);
    });

    ASSERT_NO_THROW({
      const auto m2 = s.createShm(segmentName2, size, nullptr);
      writeToMemory(m2.addr, m2.size, magicVal);
      checkMemory(m2.addr, m2.size, magicVal);
    });

    // simulate this being destroyed outside of shm manager.
    ShmManager::removeByName(cacheDir, segmentName, posix);

    // now detach. This will cause us to have a segment that we managed
    // disappear beneath us.
    s.getShmByName(segmentName).detachCurrentMapping();

    // delete the meta file
    ASSERT_TRUE(facebook::cachelib::util::pathExists(cacheDir + "/metadata"));
    facebook::cachelib::util::removePath(cacheDir + "/metadata");
    ASSERT_FALSE(facebook::cachelib::util::pathExists(cacheDir + "/metadata"));

    // shutdown should work as expected.
    ASSERT_NO_THROW(ASSERT_TRUE(s.shutDown() == ShutDownRes::kFileDeleted));
  }

  // The segment managed by us could get deleted outside from us. This should
  // not cause us trouble trying to shutdown. This is same as the above but
  // the shutdown is expected to succeed.
  {
    ShmManager s(cacheDir, posix);
    ASSERT_NO_THROW({
      const auto m = s.createShm(segmentName, size, addr);
      writeToMemory(m.addr, m.size, magicVal);
      checkMemory(m.addr, m.size, magicVal);
    });

    ASSERT_NO_THROW({
      const auto m2 = s.createShm(segmentName2, size, nullptr);
      writeToMemory(m2.addr, m2.size, magicVal);
      checkMemory(m2.addr, m2.size, magicVal);
    });

    // simulate this being destroyed outside of shm manager.
    ShmManager::removeByName(cacheDir, segmentName, posix);

    // now detach. This will cause us to have a segment that we managed
    // disappear beneath us.
    s.getShmByName(segmentName).detachCurrentMapping();

    // shutdown should work as expected.
    ASSERT_NO_THROW(ASSERT_TRUE(s.shutDown() == ShutDownRes::kSuccess));
  }
}

TEST_F(ShmManagerTestPosix, MetaFileDeletion) { testMetaFileDeletion(true); }

TEST_F(ShmManagerTestSysV, MetaFileDeletion) { testMetaFileDeletion(false); }

void ShmManagerTest::testDropFile(bool posix) {
  const std::string segmentName = std::to_string(::getpid());
  const std::string segmentName2 = segmentName + "-2";
  segmentsToDestroy.push_back(segmentName);
  segmentsToDestroy.push_back(segmentName2);
  const size_t size = getRandomSize();
  const unsigned char magicVal = 'g';
  // start the session with the first type and create some segments.
  auto addr = getNewUnmappedAddr();
  {
    ShmManager s(cacheDir, posix);
    auto m = s.createShm(segmentName, size, addr);

    writeToMemory(m.addr, m.size, magicVal);
    checkMemory(m.addr, m.size, magicVal);

    // trying to shutdown with the file deleted should fail.
    ASSERT_TRUE(s.shutDown() == ShutDownRes::kSuccess);
  }

  ASSERT_TRUE(facebook::cachelib::util::pathExists(cacheDir));
  std::ofstream(cacheDir + "/ColdRoll");
  ASSERT_TRUE(facebook::cachelib::util::pathExists(cacheDir + "/ColdRoll"));

  // now try to attach and that should fail.
  {
    ShmManager s(cacheDir, posix);
    ASSERT_FALSE(facebook::cachelib::util::pathExists(cacheDir + "/ColdRoll"));
    ASSERT_THROW(s.attachShm(segmentName), std::invalid_argument);
    auto m = s.createShm(segmentName, size, addr);
    checkMemory(m.addr, m.size, 0);
    writeToMemory(m.addr, m.size, magicVal);
    checkMemory(m.addr, m.size, magicVal);
    ASSERT_TRUE(s.shutDown() == ShutDownRes::kSuccess);
  }

  // now try to attach and that should succeed.
  {
    ShmManager s(cacheDir, posix);
    auto m = s.attachShm(segmentName, addr);
    checkMemory(m.addr, m.size, magicVal);
    ASSERT_TRUE(s.shutDown() == ShutDownRes::kSuccess);
  }

  ASSERT_TRUE(facebook::cachelib::util::pathExists(cacheDir));
  std::ofstream(cacheDir + "/ColdRoll");
  ASSERT_TRUE(facebook::cachelib::util::pathExists(cacheDir + "/ColdRoll"));

  // a new start should also delete the cold roll file
  {
    facebook::cachelib::util::removePath(cacheDir + "/metadata");
    ASSERT_FALSE(facebook::cachelib::util::pathExists(cacheDir + "/metadata"));

    ShmManager s(cacheDir, posix);
    // Cold roll file should no longer exist
    ASSERT_FALSE(facebook::cachelib::util::pathExists(cacheDir + "/ColdRoll"));
  }

  // now try to attach and that should fail due to previous cold roll
  {
    ShmManager s(cacheDir, posix);
    ASSERT_THROW(s.attachShm(segmentName), std::invalid_argument);
  }
}

TEST_F(ShmManagerTestPosix, DropFile) { testDropFile(true); }

TEST_F(ShmManagerTestSysV, DropFile) { testDropFile(false); }

// Tests to ensure that when we shutdown with posix and restart with shm, we
// dont mess things up and coming up with the wrong type fails.
void ShmManagerTest::testInvalidType(bool posix) {
  // we ll create the instance with this type and try with the other type

  const std::string segmentName = std::to_string(::getpid());
  segmentsToDestroy.push_back(segmentName);
  const size_t size = getRandomSize();
  const unsigned char magicVal = 'g';
  // start the sesion with the first type and create some segments.
  auto addr = getNewUnmappedAddr();
  {
    ShmManager s(cacheDir, posix);
    auto m = s.createShm(segmentName, size, addr);

    writeToMemory(m.addr, m.size, magicVal);
    checkMemory(m.addr, m.size, magicVal);
    ASSERT_TRUE(s.shutDown() == ShutDownRes::kSuccess);
  }

  ASSERT_TRUE(facebook::cachelib::util::pathExists(cacheDir));
  ASSERT_TRUE(facebook::cachelib::util::pathExists(cacheDir + "/metadata"));
  // now try to connect with the second type.

  ASSERT_THROW(ShmManager s(cacheDir, !posix), std::invalid_argument);

  {
    ShmManager s(cacheDir, posix);
    auto m = s.attachShm(segmentName, addr);

    checkMemory(m.addr, m.size, magicVal);
    ASSERT_TRUE(s.shutDown() == ShutDownRes::kSuccess);
  }
}

TEST_F(ShmManagerTestPosix, InvalidType) { testInvalidType(true); }

TEST_F(ShmManagerTestSysV, InvalidType) { testInvalidType(false); }

void ShmManagerTest::testRemove(bool posix) {
  const std::string seg1 = std::to_string(::getpid()) + "-0";
  const std::string seg2 = std::to_string(::getpid()) + "-1";
  const size_t size = getRandomSize();
  const unsigned char magicVal = 'x';
  segmentsToDestroy.push_back(seg1);
  segmentsToDestroy.push_back(seg2);
  auto addr = getNewUnmappedAddr();
  {
    ShmManager s(cacheDir, posix);
    ASSERT_FALSE(s.removeShm(seg1));
    auto m1 = s.createShm(seg1, size, nullptr);
    auto m2 = s.createShm(seg2, size, getNewUnmappedAddr());

    writeToMemory(m1.addr, m1.size, magicVal);
    writeToMemory(m2.addr, m2.size, magicVal);
    checkMemory(m1.addr, m1.size, magicVal);
    checkMemory(m2.addr, m2.size, magicVal);
    ASSERT_TRUE(s.shutDown() == ShutDownRes::kSuccess);
  }

  {
    ShmManager s(cacheDir, posix);
    auto m1 = s.attachShm(seg1, addr);
    auto& shm1 = s.getShmByName(seg1);
    checkMemory(m1.addr, m1.size, magicVal);

    auto m2 = s.attachShm(seg2, getNewUnmappedAddr());
    checkMemory(m2.addr, m2.size, magicVal);

    ASSERT_TRUE(shm1.isMapped());
    ASSERT_TRUE(s.removeShm(seg1));
    ASSERT_THROW(s.getShmByName(seg1), std::invalid_argument);

    // trying to remove now should indicate that the segment does not exist
    ASSERT_FALSE(s.removeShm(seg1));
    s.shutDown();
  }

  // attaching after shutdown should reflect the remove
  {
    ShmManager s(cacheDir, posix);
    auto m1 = s.createShm(seg1, size, addr);
    checkMemory(m1.addr, m1.size, 0);

    auto m2 = s.attachShm(seg2, getNewUnmappedAddr());
    checkMemory(m2.addr, m2.size, magicVal);
    s.shutDown();
  }

  // test detachAndRemove
  {
    ShmManager s(cacheDir, posix);
    auto m1 = s.attachShm(seg1, addr);
    checkMemory(m1.addr, m1.size, 0);

    auto m2 = s.attachShm(seg2, getNewUnmappedAddr());
    auto& shm2 = s.getShmByName(seg2);
    checkMemory(m2.addr, m2.size, magicVal);

    // call detach and remove with an attached segment
    ASSERT_TRUE(s.removeShm(seg1));
    ASSERT_THROW(s.getShmByName(seg1), std::invalid_argument);

    // call detach and remove with a detached segment
    shm2.detachCurrentMapping();
    ASSERT_TRUE(s.removeShm(seg2));
    ASSERT_THROW(s.getShmByName(seg2), std::invalid_argument);
    s.shutDown();
  }

  {
    ShmManager s(cacheDir, posix);
    ASSERT_THROW(s.attachShm(seg1), std::invalid_argument);
    ASSERT_THROW(s.attachShm(seg2), std::invalid_argument);
  }
}

TEST_F(ShmManagerTestPosix, Remove) { testRemove(true); }

TEST_F(ShmManagerTestSysV, Remove) { testRemove(false); }

void ShmManagerTest::testStaticCleanup(bool posix) {
  // pid-X to keep it unique so we dont collude with other tests
  int num = 0;
  const std::string segmentPrefix = std::to_string(::getpid());
  const std::string seg1 = segmentPrefix + "-" + std::to_string(num++);
  const std::string seg2 = segmentPrefix + "-" + std::to_string(num++);

  // open an instance and create some segments, write to the memory and
  // shutdown.
  ASSERT_NO_THROW({
    ShmManager s(cacheDir, posix);

    segmentsToDestroy.push_back(seg1);
    s.createShm(seg1, getRandomSize());

    segmentsToDestroy.push_back(seg2);
    s.createShm(seg2, getRandomSize());

    ASSERT_TRUE(s.shutDown() == ShutDownRes::kSuccess);
  });

  ASSERT_NO_THROW({
    ShmManager::removeByName(cacheDir, seg1, posix);
    ShmManager s(cacheDir, posix);
    ASSERT_THROW(s.attachShm(seg1), std::invalid_argument);

    ASSERT_TRUE(s.shutDown() == ShutDownRes::kSuccess);
  });

  ASSERT_NO_THROW({
    ShmManager::cleanup(cacheDir, posix);
    ShmManager s(cacheDir, posix);
    ASSERT_THROW(s.attachShm(seg2), std::invalid_argument);
  });
}

TEST_F(ShmManagerTestPosix, StaticCleanup) { testStaticCleanup(true); }

TEST_F(ShmManagerTestSysV, StaticCleanup) { testStaticCleanup(false); }

// test to ensure that if the directory is invalid, things fail
void ShmManagerTest::testInvalidCachedDir(bool posix) {
  std::ofstream f(cacheDir);
  f.close();
  ASSERT_TRUE(facebook::cachelib::util::pathExists(cacheDir));
  ASSERT_FALSE(facebook::cachelib::util::isDir(cacheDir));
  // expect error since the cache dir is a file.
  ASSERT_THROW(ShmManager s(cacheDir, posix), std::system_error);

  facebook::cachelib::util::removePath(cacheDir);
  // this should have created the directory and an empty meta file
  ASSERT_NO_THROW(ShmManager s(cacheDir, posix));
  ASSERT_TRUE(facebook::cachelib::util::pathExists(cacheDir));
  ASSERT_TRUE(facebook::cachelib::util::isDir(cacheDir));
  auto metaPath = cacheDir + "/metadata";
  ASSERT_TRUE(facebook::cachelib::util::pathExists(metaPath));
  std::string content;
  ASSERT_TRUE(folly::readFile(metaPath.c_str(), content));
  ASSERT_TRUE(content.empty());
}

TEST_F(ShmManagerTestPosix, InvalidCacheDir) { testInvalidCachedDir(true); }

TEST_F(ShmManagerTestSysV, InvalidCacheDir) { testInvalidCachedDir(false); }

// test to ensure that random contents in the file cause it to fail
void ShmManagerTest::testInvalidMetaFile(bool posix) {
  facebook::cachelib::util::makeDir(cacheDir);
  std::ofstream f(cacheDir + "/metadata");
  f << "helloworld";
  f.flush();
  f.close();
  ASSERT_THROW(ShmManager s(cacheDir, posix), std::invalid_argument);
}

TEST_F(ShmManagerTestPosix, InvalidMetaFile) { testInvalidMetaFile(true); }

TEST_F(ShmManagerTestSysV, InvalidMetaFile) { testInvalidMetaFile(false); }

// test to ensure that random contents in the file cause it to fail
void ShmManagerTest::testEmptyMetaFile(bool posix) {
  facebook::cachelib::util::makeDir(cacheDir);
  {
    std::ofstream f(cacheDir + "/metadata", std::ios::trunc);
    f.flush();
    f.close();
  }
  ASSERT_NO_THROW(ShmManager s(cacheDir, posix));
}

TEST_F(ShmManagerTestPosix, EmptyMetaFile) { testEmptyMetaFile(true); }

TEST_F(ShmManagerTestSysV, EmptyMetaFile) { testEmptyMetaFile(false); }

// test to ensure that segments can be created with a new cache dir, attached
// from existing cache dir, segments can be deleted and recreated using the
// same cache dir if they have not been attached to already.
void ShmManagerTest::testSegments(bool posix) {
  const char magicVal1 = 'f';
  const char magicVal2 = 'e';
  // pid-X to keep it unique so we dont collude with other tests
  int num = 0;
  const std::string segmentPrefix = std::to_string(::getpid());
  const std::string seg1 = segmentPrefix + "-" + std::to_string(num++);
  const std::string seg2 = segmentPrefix + "-" + std::to_string(num++);
  auto addr = getNewUnmappedAddr();

  // open an instance and create some segments, write to the memory and
  // shutdown.
  ASSERT_NO_THROW({
    ShmManager s(cacheDir, posix);

    segmentsToDestroy.push_back(seg1);
    auto m1 = s.createShm(seg1, getRandomSize(), addr);
    writeToMemory(m1.addr, m1.size, magicVal1);
    checkMemory(m1.addr, m1.size, magicVal1);

    segmentsToDestroy.push_back(seg2);
    auto m2 = s.createShm(seg2, getRandomSize(), getNewUnmappedAddr());
    writeToMemory(m2.addr, m2.size, magicVal2);
    checkMemory(m2.addr, m2.size, magicVal2);
    ASSERT_TRUE(s.shutDown() == ShutDownRes::kSuccess);
  });

  // try to attach
  ASSERT_NO_THROW({
    ShmManager s(cacheDir, posix);

    // attach
    auto m1 = s.attachShm(seg1, addr);
    writeToMemory(m1.addr, m1.size, magicVal1);
    checkMemory(m1.addr, m1.size, magicVal1);

    // attach
    auto m2 = s.attachShm(seg2, getNewUnmappedAddr());
    writeToMemory(m2.addr, m2.size, magicVal2);
    checkMemory(m2.addr, m2.size, magicVal2);
    // no clean shutdown this time.
  });

  // try to create new segments. This should destroy the previous segments
  {
    ShmManager s(cacheDir, posix);
    // try attach, but it should fail.
    ASSERT_THROW(s.attachShm(seg1), std::invalid_argument);

    // try attach
    ASSERT_THROW(s.attachShm(seg2), std::invalid_argument);

    // now create new segments with same name. This should remove the
    // previous version of the segments with same name.
    ASSERT_NO_THROW({
      auto m1 = s.createShm(seg1, getRandomSize(), addr);
      checkMemory(m1.addr, m1.size, 0);
      writeToMemory(m1.addr, m1.size, magicVal1);
      checkMemory(m1.addr, m1.size, magicVal1);

      segmentsToDestroy.push_back(seg2);
      auto m2 = s.createShm(seg2, getRandomSize(), getNewUnmappedAddr());
      checkMemory(m2.addr, m2.size, 0);
      writeToMemory(m2.addr, m2.size, magicVal2);
      checkMemory(m2.addr, m2.size, magicVal2);
    });
    // do a clean shutdown.
    ASSERT_TRUE(s.shutDown() == ShutDownRes::kSuccess);
  };

  // create exisiting segments again after safe shutdown and ensure that
  // previous versions are removed.
  ASSERT_NO_THROW({
    ShmManager s(cacheDir, posix);
    auto m1 = s.createShm(seg1, getRandomSize(), addr);
    // ensure its the new one.
    checkMemory(m1.addr, m1.size, 0);
    writeToMemory(m1.addr, m1.size, magicVal2);

    auto m2 = s.attachShm(seg2, getNewUnmappedAddr());
    // ensure that we attached to the previous segment.
    checkMemory(m2.addr, m2.size, magicVal2);
    writeToMemory(m2.addr, m2.size, magicVal1);
    ASSERT_TRUE(s.shutDown() == ShutDownRes::kSuccess);
  });

  // with one segment being created new in the last attempt and one from the
  // past, ensure that the shutdown was proper.  we expect magicVal2 from seg1
  // and magicVal1 from seg2 as per the previous run above.
  ASSERT_NO_THROW({
    ShmManager s(cacheDir, posix);

    // attach
    auto m1 = s.attachShm(seg1, addr);
    checkMemory(m1.addr, m1.size, magicVal2);

    // attach
    auto m2 = s.attachShm(seg2, getNewUnmappedAddr());
    checkMemory(m2.addr, m2.size, magicVal1);
    // no clean shutdown this time.
  });
}

TEST_F(ShmManagerTestPosix, Segments) { testSegments(true); }

TEST_F(ShmManagerTestSysV, Segments) { testSegments(false); }

void ShmManagerTest::testShutDown(bool posix) {
  // pid-X to keep it unique so we dont collude with other tests
  int num = 0;
  const std::string segmentPrefix = std::to_string(::getpid());
  const std::string seg1 = segmentPrefix + "-" + std::to_string(num++);
  const std::string seg2 = segmentPrefix + "-" + std::to_string(num++);
  const std::string seg3 = segmentPrefix + "-" + std::to_string(num++);
  size_t seg1Size = 0;
  size_t seg2Size = 0;
  size_t seg3Size = 0;

  // open an instance and create some segments
  ASSERT_NO_THROW({
    ShmManager s(cacheDir, posix);

    segmentsToDestroy.push_back(seg1);
    seg1Size = getRandomSize();
    s.createShm(seg1, seg1Size);
    auto& shm1 = s.getShmByName(seg1);
    ASSERT_EQ(shm1.getSize(), seg1Size);

    segmentsToDestroy.push_back(seg2);
    seg2Size = getRandomSize();
    s.createShm(seg2, seg2Size);
    auto& shm2 = s.getShmByName(seg2);
    ASSERT_EQ(shm2.getSize(), seg2Size);

    segmentsToDestroy.push_back(seg3);
    seg3Size = getRandomSize();
    s.createShm(seg3, seg3Size);
    auto& shm3 = s.getShmByName(seg3);
    ASSERT_EQ(shm3.getSize(), seg3Size);

    ASSERT_TRUE(s.shutDown() == ShutDownRes::kSuccess);
  });

  // should be able to attach to all of them.
  ASSERT_NO_THROW({
    ShmManager s(cacheDir, posix);

    s.attachShm(seg1);
    auto& shm1 = s.getShmByName(seg1);
    ASSERT_EQ(shm1.getSize(), seg1Size);

    s.attachShm(seg2);
    auto& shm2 = s.getShmByName(seg2);
    ASSERT_EQ(shm2.getSize(), seg2Size);

    s.attachShm(seg3);
    auto& shm3 = s.getShmByName(seg3);
    ASSERT_EQ(shm3.getSize(), seg3Size);

    ASSERT_TRUE(s.shutDown() == ShutDownRes::kSuccess);
  });

  // should be able to attach to all of them. attach only seg1 and seg3. seg2
  // should be destroyed.
  ASSERT_NO_THROW({
    ShmManager s(cacheDir, posix);

    s.attachShm(seg1);
    auto& shm1 = s.getShmByName(seg1);
    ASSERT_EQ(shm1.getSize(), seg1Size);

    s.attachShm(seg3);
    auto& shm3 = s.getShmByName(seg3);
    ASSERT_EQ(shm3.getSize(), seg3Size);

    ASSERT_TRUE(s.shutDown() == ShutDownRes::kSuccess);
  });

  // we should only find seg1 and seg3. seg2 should be missing for attach and
  // we should be able to create a new one.
  {
    ShmManager s(cacheDir, posix);

    ASSERT_NO_THROW({
      s.attachShm(seg1);
      auto& shm1 = s.getShmByName(seg1);
      ASSERT_EQ(shm1.getSize(), seg1Size);

      s.attachShm(seg3);
      auto& shm3 = s.getShmByName(seg3);
      ASSERT_EQ(shm3.getSize(), seg3Size);
    });

    ASSERT_THROW(s.attachShm(seg2), std::invalid_argument);

    // create a new one. this is possible only because the previous one was
    // destroyed.
    ASSERT_NO_THROW(s.createShm(seg2, seg2Size));
    ASSERT_EQ(s.getShmByName(seg2).getSize(), seg2Size);

    ASSERT_TRUE(s.shutDown() == ShutDownRes::kSuccess);
  };

  ASSERT_NO_THROW({
    ShmManager s(cacheDir, posix);
    // shutdown without attaching to any of the segments. This would delete
    // all the segments.
    ASSERT_TRUE(s.shutDown() == ShutDownRes::kSuccess);
  });

  {
    ShmManager s(cacheDir, posix);

    ASSERT_THROW(s.attachShm(seg1), std::invalid_argument);

    ASSERT_THROW(s.attachShm(seg2), std::invalid_argument);

    ASSERT_THROW(s.attachShm(seg3), std::invalid_argument);

    ASSERT_NO_THROW(s.createShm(seg1, seg1Size));
    ASSERT_EQ(s.getShmByName(seg1).getSize(), seg1Size);

    ASSERT_NO_THROW(s.createShm(seg2, seg2Size));
    ASSERT_EQ(s.getShmByName(seg2).getSize(), seg2Size);

    ASSERT_NO_THROW(s.createShm(seg3, seg3Size));
    ASSERT_EQ(s.getShmByName(seg3).getSize(), seg3Size);

    // dont call shutdown
  };

  // all segments should be destroyed now.
  {
    ShmManager s(cacheDir, posix);
    ASSERT_THROW(s.attachShm(seg1), std::invalid_argument);
    ASSERT_THROW(s.attachShm(seg2), std::invalid_argument);
    ASSERT_THROW(s.attachShm(seg3), std::invalid_argument);
  };
}

TEST_F(ShmManagerTestPosix, ShutDown) { testShutDown(true); }

TEST_F(ShmManagerTestSysV, ShutDown) { testShutDown(false); }

void ShmManagerTest::testCleanup(bool posix) {
  // pid-X to keep it unique so we dont collude with other tests
  int num = 0;
  const std::string segmentPrefix = std::to_string(::getpid());
  const std::string seg1 = segmentPrefix + "-" + std::to_string(num++);
  const std::string seg2 = segmentPrefix + "-" + std::to_string(num++);
  const std::string seg3 = segmentPrefix + "-" + std::to_string(num++);
  size_t seg1Size = 0;
  size_t seg2Size = 0;
  size_t seg3Size = 0;

  // open an instance and create some segments
  ASSERT_NO_THROW({
    ShmManager s(cacheDir, posix);

    segmentsToDestroy.push_back(seg1);
    seg1Size = getRandomSize();
    s.createShm(seg1, seg1Size);
    auto& shm1 = s.getShmByName(seg1);
    ASSERT_EQ(shm1.getSize(), seg1Size);

    segmentsToDestroy.push_back(seg2);
    seg2Size = getRandomSize();
    s.createShm(seg2, seg2Size);
    auto& shm2 = s.getShmByName(seg2);
    ASSERT_EQ(shm2.getSize(), seg2Size);

    segmentsToDestroy.push_back(seg3);
    seg3Size = getRandomSize();
    s.createShm(seg3, seg3Size);
    auto& shm3 = s.getShmByName(seg3);
    ASSERT_EQ(shm3.getSize(), seg3Size);

    // shutdown safely
    ASSERT_TRUE(s.shutDown() == ShutDownRes::kSuccess);
  });

  // this should free up all the segments and also reset the metadata file.
  // attaching now should not work.
  ShmManager::cleanup(cacheDir, posix);

  // should not be able to attach to any of the previous segments, but we
  // should be able to create new ones.
  {
    ShmManager s(cacheDir, posix);

    ASSERT_THROW(s.attachShm(seg1), std::invalid_argument);

    ASSERT_THROW(s.attachShm(seg2), std::invalid_argument);

    ASSERT_THROW(s.attachShm(seg3), std::invalid_argument);

    ASSERT_NO_THROW({
      s.createShm(seg1, seg1Size);
      auto& shm1 = s.getShmByName(seg1);
      ASSERT_EQ(shm1.getSize(), seg1Size);

      s.createShm(seg2, seg2Size);
      auto& shm2 = s.getShmByName(seg2);
      ASSERT_EQ(shm2.getSize(), seg2Size);

      s.createShm(seg3, seg3Size);
      auto& shm3 = s.getShmByName(seg3);
      ASSERT_EQ(shm3.getSize(), seg3Size);
    });
    // dont call shutdown
  }
}

TEST_F(ShmManagerTestPosix, Cleanup) { testCleanup(true); }

TEST_F(ShmManagerTestSysV, Cleanup) { testCleanup(false); }

void ShmManagerTest::testAttachReadOnly(bool posix) {
  // pid-X to keep it unique so we dont collude with other tests
  int num = 0;
  const std::string segmentPrefix = std::to_string(::getpid());
  const std::string seg = segmentPrefix + "-" + std::to_string(num++);
  size_t segSize = 0;

  // open an instance and create segment
  ShmManager s(cacheDir, posix);

  segmentsToDestroy.push_back(seg);
  segSize = getRandomSize();
  s.createShm(seg, segSize);
  auto& shm = s.getShmByName(seg);
  ASSERT_EQ(shm.getSize(), segSize);
  const unsigned char magicVal = 'd';
  writeToMemory(shm.getCurrentMapping().addr, segSize, magicVal);

  auto roShm = ShmManager::attachShmReadOnly(cacheDir, seg, posix);
  ASSERT_NE(roShm.get(), nullptr);
  ASSERT_TRUE(roShm->isMapped());
  checkMemory(roShm->getCurrentMapping().addr, segSize, magicVal);
}

TEST_F(ShmManagerTestPosix, AttachReadOnly) { testAttachReadOnly(true); }

TEST_F(ShmManagerTestSysV, AttachReadOnly) { testAttachReadOnly(false); }

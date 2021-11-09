/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

#include <folly/FileUtil.h>
#include <folly/Random.h>
#include <sys/time.h>

#include <fstream>

#include "cachelib/common/Utils.h"
#include "cachelib/shm/PosixShmSegment.h"
#include "cachelib/shm/ShmCommon.h"
#include "cachelib/shm/ShmManager.h"
#include "cachelib/shm/SysVShmSegment.h"
#include "cachelib/shm/tests/common.h"

static const std::string namePrefix = "shm-test";
using namespace facebook::cachelib::tests;

using facebook::cachelib::ShmManager;
using facebook::cachelib::ShmSegmentOpts;
using facebook::cachelib::ShmTypeOpts;
using facebook::cachelib::PosixSysVSegmentOpts;
using facebook::cachelib::FileShmSegmentOpts;

using ShutDownRes = typename facebook::cachelib::ShmManager::ShutDownRes;

class ShmManagerTest : public ShmTestBase {
 public:
  ShmManagerTest() : cacheDir(dirPrefix + std::to_string(::getpid())) {}

  const std::string cacheDir{};

 protected:
  std::vector<std::pair<std::string, ShmSegmentOpts>> segmentsToDestroy{};

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

  virtual std::pair<std::string, ShmSegmentOpts> makeSegmentImpl(
    std::string name) = 0;
  virtual void clearAllSegments() = 0;

  std::pair<std::string, ShmSegmentOpts> makeSegment(std::string name,
    bool addToDestroy = true) {
    auto val = makeSegmentImpl(name);
    if (addToDestroy)
      segmentsToDestroy.push_back(val);
    return val;
  }

  /*
   * We define the generic test here that can be run by the appropriate
   * specification of the test fixture by their shm type
   */
  void testInvalidCachedDir(bool posix);
  void testInvalidMetaFile(bool posix);
  void testEmptyMetaFile(bool posix);
  void testSegments(bool posix);
  void testMappingAlignment(bool posix);
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
  virtual std::pair<std::string, ShmSegmentOpts> makeSegmentImpl(std::string name)
    override {
      ShmSegmentOpts opts;
      opts.typeOpts = PosixSysVSegmentOpts{false};
      return std::pair<std::string, ShmSegmentOpts>{name, opts};
  }

  void clearAllSegments() override {
    for (const auto& seg : segmentsToDestroy) {
      ShmManager::removeByName(cacheDir, seg.first, seg.second.typeOpts);
    }
  }
};

class ShmManagerTestPosix : public ShmManagerTest {
 public:
  virtual std::pair<std::string, ShmSegmentOpts> makeSegmentImpl(std::string name)
    override {
      ShmSegmentOpts opts;
      opts.typeOpts = PosixSysVSegmentOpts{true};
      return std::pair<std::string, ShmSegmentOpts>{name, opts};
  }

  void clearAllSegments() override {
    for (const auto& seg : segmentsToDestroy) {
      ShmManager::removeByName(cacheDir, seg.first, seg.second.typeOpts);
    }
  }
};

class ShmManagerTestFile : public ShmManagerTest {
 public:
  virtual std::pair<std::string, ShmSegmentOpts> makeSegmentImpl(std::string name)
    override {
      ShmSegmentOpts opts;
      opts.typeOpts = FileShmSegmentOpts{"/tmp/" + name};
      return std::pair<std::string, ShmSegmentOpts>{name, opts};
  }

  void clearAllSegments() override {
    for (const auto& seg : segmentsToDestroy) {
      ShmManager::removeByName(cacheDir, seg.first, seg.second.typeOpts);
    }
  }
};

const std::string ShmManagerTest::dirPrefix = "/tmp/shm-test";

void ShmManagerTest::testMetaFileDeletion(bool posix) {
  int num = 0;
  auto segmentPrefix = std::to_string(::getpid());
  auto segment1 = makeSegment(segmentPrefix + "-" + std::to_string(num++));
  auto segment2 = makeSegment(segmentPrefix + "-" + std::to_string(num++));
  const auto seg1 = segment1.first;
  const auto seg2 = segment2.first;
  const auto seg1Opt = segment1.second;
  const auto seg2Opt = segment2.second;

  const size_t size = getRandomSize();
  const unsigned char magicVal = 'g';
  // start the session with the first type and create some segments.
  auto addr = getNewUnmappedAddr();
  {
    ShmManager s(cacheDir, posix);
    auto m = s.createShm(seg1, size, addr, seg1Opt);

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
    ASSERT_THROW(s.attachShm(seg1, nullptr, seg1Opt),
      std::invalid_argument);
    auto m = s.createShm(seg1, size, addr, seg1Opt);
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
    ASSERT_THROW(s.attachShm(seg1, nullptr, seg1Opt),
      std::invalid_argument);
    auto m = s.createShm(seg1, size, addr, seg1Opt);
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
      const auto m = s.attachShm(seg1, addr, seg1Opt);
      writeToMemory(m.addr, m.size, magicVal);
      checkMemory(m.addr, m.size, magicVal);
    });

    ASSERT_NO_THROW({
      const auto m2 = s.createShm(seg2, size, nullptr,
        seg2Opt);
      writeToMemory(m2.addr, m2.size, magicVal);
      checkMemory(m2.addr, m2.size, magicVal);
    });

    // simulate this being destroyed outside of shm manager.
    ShmManager::removeByName(cacheDir, seg1, seg1Opt.typeOpts);

    // now detach. This will cause us to have a segment that we managed
    // disappear beneath us.
    s.getShmByName(seg1).detachCurrentMapping();

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
      const auto m = s.createShm(seg1, size, addr, seg1Opt);
      writeToMemory(m.addr, m.size, magicVal);
      checkMemory(m.addr, m.size, magicVal);
    });

    ASSERT_NO_THROW({
      const auto m2 = s.createShm(seg2, size, nullptr, seg2Opt);
      writeToMemory(m2.addr, m2.size, magicVal);
      checkMemory(m2.addr, m2.size, magicVal);
    });

    // simulate this being destroyed outside of shm manager.
    ShmManager::removeByName(cacheDir, seg1, seg1Opt.typeOpts);

    // now detach. This will cause us to have a segment that we managed
    // disappear beneath us.
    s.getShmByName(seg1).detachCurrentMapping();

    // shutdown should work as expected.
    ASSERT_NO_THROW(ASSERT_TRUE(s.shutDown() == ShutDownRes::kSuccess));
  }
}

TEST_F(ShmManagerTestPosix, MetaFileDeletion) { testMetaFileDeletion(true); }

TEST_F(ShmManagerTestSysV, MetaFileDeletion) { testMetaFileDeletion(false); }

TEST_F(ShmManagerTestFile, MetaFileDeletion) { testMetaFileDeletion(false); }

void ShmManagerTest::testDropFile(bool posix) {
  int num = 0;
  auto segmentPrefix = std::to_string(::getpid());
  auto segment1 = makeSegment(segmentPrefix + "-" + std::to_string(num++));
  const auto seg1 = segment1.first;
  const auto seg1Opt = segment1.second;
  const size_t size = getRandomSize();
  const unsigned char magicVal = 'g';
  // start the session with the first type and create some segments.
  auto addr = getNewUnmappedAddr();
  {
    ShmManager s(cacheDir, posix);
    auto m = s.createShm(seg1, size, addr, seg1Opt);

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
    ASSERT_THROW(s.attachShm(seg1, nullptr, seg1Opt),
      std::invalid_argument);
    auto m = s.createShm(seg1, size, addr, seg1Opt);
    checkMemory(m.addr, m.size, 0);
    writeToMemory(m.addr, m.size, magicVal);
    checkMemory(m.addr, m.size, magicVal);
    ASSERT_TRUE(s.shutDown() == ShutDownRes::kSuccess);
  }

  // now try to attach and that should succeed.
  {
    ShmManager s(cacheDir, posix);
    auto m = s.attachShm(seg1, addr, seg1Opt);
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
    ASSERT_THROW(s.attachShm(seg1, nullptr, seg1Opt),
      std::invalid_argument);
  }
}

TEST_F(ShmManagerTestPosix, DropFile) { testDropFile(true); }

TEST_F(ShmManagerTestSysV, DropFile) { testDropFile(false); }

TEST_F(ShmManagerTestFile, DropFile) { testDropFile(false); }

// Tests to ensure that when we shutdown with posix and restart with shm, we
// dont mess things up and coming up with the wrong type fails.
void ShmManagerTest::testInvalidType(bool posix) {
  // we ll create the instance with this type and try with the other type
  int num = 0;
  auto segmentPrefix = std::to_string(::getpid());
  auto segment1 = makeSegment(segmentPrefix + "-" + std::to_string(num++));
  const auto seg1 = segment1.first;
  const auto seg1Opt = segment1.second;

  const size_t size = getRandomSize();
  const unsigned char magicVal = 'g';
  // start the sesion with the first type and create some segments.
  auto addr = getNewUnmappedAddr();
  {
    ShmManager s(cacheDir, posix);
    auto m = s.createShm(seg1, size, addr, seg1Opt);

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
    auto m = s.attachShm(seg1, addr, seg1Opt);

    checkMemory(m.addr, m.size, magicVal);
    ASSERT_TRUE(s.shutDown() == ShutDownRes::kSuccess);
  }
}

TEST_F(ShmManagerTestPosix, InvalidType) { testInvalidType(true); }

TEST_F(ShmManagerTestSysV, InvalidType) { testInvalidType(false); }

TEST_F(ShmManagerTestFile, InvalidType) { testInvalidType(false); }

void ShmManagerTest::testRemove(bool posix) {
  int num = 0;
  auto segmentPrefix = std::to_string(::getpid());
  auto segment1 = makeSegment(segmentPrefix + "-" + std::to_string(num++));
  auto segment2 = makeSegment(segmentPrefix + "-" + std::to_string(num++));
  const auto seg1 = segment1.first;
  const auto seg2 = segment2.first;
  const auto seg1Opt = segment1.second;
  const auto seg2Opt = segment2.second;
  const size_t size = getRandomSize();
  const unsigned char magicVal = 'x';
  auto addr = getNewUnmappedAddr();
  {
    ShmManager s(cacheDir, posix);
    ASSERT_FALSE(s.removeShm(seg1, seg1Opt.typeOpts));
    auto m1 = s.createShm(seg1, size, nullptr, seg1Opt);
    auto m2 = s.createShm(seg2, size, getNewUnmappedAddr(), seg2Opt);

    writeToMemory(m1.addr, m1.size, magicVal);
    writeToMemory(m2.addr, m2.size, magicVal);
    checkMemory(m1.addr, m1.size, magicVal);
    checkMemory(m2.addr, m2.size, magicVal);
    ASSERT_TRUE(s.shutDown() == ShutDownRes::kSuccess);
  }

  {
    ShmManager s(cacheDir, posix);
    auto m1 = s.attachShm(seg1, addr, seg1Opt);
    auto& shm1 = s.getShmByName(seg1);
    checkMemory(m1.addr, m1.size, magicVal);

    auto m2 = s.attachShm(seg2, getNewUnmappedAddr(), seg2Opt);
    checkMemory(m2.addr, m2.size, magicVal);

    ASSERT_TRUE(shm1.isMapped());
    ASSERT_TRUE(s.removeShm(seg1, seg1Opt.typeOpts));
    ASSERT_THROW(s.getShmByName(seg1), std::invalid_argument);

    // trying to remove now should indicate that the segment does not exist
    ASSERT_FALSE(s.removeShm(seg1, seg1Opt.typeOpts));
    s.shutDown();
  }

  // attaching after shutdown should reflect the remove
  {
    ShmManager s(cacheDir, posix);
    auto m1 = s.createShm(seg1, size, addr, seg1Opt);
    checkMemory(m1.addr, m1.size, 0);

    auto m2 = s.attachShm(seg2, getNewUnmappedAddr(), seg2Opt);
    checkMemory(m2.addr, m2.size, magicVal);
    s.shutDown();
  }

  // test detachAndRemove
  {
    ShmManager s(cacheDir, posix);
    auto m1 = s.attachShm(seg1, addr, seg1Opt);
    checkMemory(m1.addr, m1.size, 0);

    auto m2 = s.attachShm(seg2, getNewUnmappedAddr(), seg2Opt);
    auto& shm2 = s.getShmByName(seg2);
    checkMemory(m2.addr, m2.size, magicVal);

    // call detach and remove with an attached segment
    ASSERT_TRUE(s.removeShm(seg1, seg1Opt.typeOpts));
    ASSERT_THROW(s.getShmByName(seg1), std::invalid_argument);

    // call detach and remove with a detached segment
    shm2.detachCurrentMapping();
    ASSERT_TRUE(s.removeShm(seg2, seg2Opt.typeOpts));
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

TEST_F(ShmManagerTestFile, Remove) { testRemove(false); }

void ShmManagerTest::testStaticCleanup(bool posix) {
  // pid-X to keep it unique so we dont collude with other tests
  int num = 0;
  auto segmentPrefix = std::to_string(::getpid());
  auto segment1 = makeSegment(segmentPrefix + "-" + std::to_string(num++));
  auto segment2 = makeSegment(segmentPrefix + "-" + std::to_string(num++));
  const auto seg1 = segment1.first;
  const auto seg2 = segment2.first;
  const auto seg1Opt = segment1.second;
  const auto seg2Opt = segment2.second;

  // open an instance and create some segments, write to the memory and
  // shutdown.
  ASSERT_NO_THROW({
    ShmManager s(cacheDir, posix);

    s.createShm(seg1, getRandomSize(), nullptr, seg1Opt);
    s.createShm(seg2, getRandomSize(), nullptr, seg2Opt);

    ASSERT_TRUE(s.shutDown() == ShutDownRes::kSuccess);
  });

  ASSERT_NO_THROW({
    ShmManager::removeByName(cacheDir, seg1, seg1Opt.typeOpts);
    ShmManager s(cacheDir, posix);
    ASSERT_THROW(s.attachShm(seg1, nullptr, seg1Opt), std::invalid_argument);

    ASSERT_TRUE(s.shutDown() == ShutDownRes::kSuccess);
  });

  ASSERT_NO_THROW({
    ShmManager::cleanup(cacheDir, posix);
    ShmManager s(cacheDir, posix);
    ASSERT_THROW(s.attachShm(seg2, nullptr, seg1Opt), std::invalid_argument);
  });
}

TEST_F(ShmManagerTestPosix, StaticCleanup) { testStaticCleanup(true); }

TEST_F(ShmManagerTestSysV, StaticCleanup) { testStaticCleanup(false); }

TEST_F(ShmManagerTestFile, StaticCleanup) { testStaticCleanup(false); }

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

TEST_F(ShmManagerTestFile, InvalidCacheDir) { testInvalidCachedDir(false); }

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

TEST_F(ShmManagerTestFile, EmptyMetaFile) { testEmptyMetaFile(false); }

// test to ensure that segments can be created with a new cache dir, attached
// from existing cache dir, segments can be deleted and recreated using the
// same cache dir if they have not been attached to already.
void ShmManagerTest::testSegments(bool posix) {
  const char magicVal1 = 'f';
  const char magicVal2 = 'e';
  // pid-X to keep it unique so we dont collude with other tests
  int num = 0;
  auto segmentPrefix = std::to_string(::getpid());
  auto segment1 = makeSegment(segmentPrefix + "-" + std::to_string(num++));
  auto segment2 = makeSegment(segmentPrefix + "-" + std::to_string(num++));
  const auto seg1 = segment1.first;
  const auto seg2 = segment2.first;
  const auto seg1Opt = segment1.second;
  const auto seg2Opt = segment2.second;
  auto addr = getNewUnmappedAddr();

  // open an instance and create some segments, write to the memory and
  // shutdown.
  ASSERT_NO_THROW({
    ShmManager s(cacheDir, posix);

    auto m1 = s.createShm(seg1, getRandomSize(), addr, seg1Opt);
    writeToMemory(m1.addr, m1.size, magicVal1);
    checkMemory(m1.addr, m1.size, magicVal1);

    auto m2 = s.createShm(seg2, getRandomSize(), getNewUnmappedAddr(), seg2Opt);
    writeToMemory(m2.addr, m2.size, magicVal2);
    checkMemory(m2.addr, m2.size, magicVal2);
    ASSERT_TRUE(s.shutDown() == ShutDownRes::kSuccess);
  });

  // try to attach
  ASSERT_NO_THROW({
    ShmManager s(cacheDir, posix);

    // attach
    auto m1 = s.attachShm(seg1, addr, seg1Opt);
    writeToMemory(m1.addr, m1.size, magicVal1);
    checkMemory(m1.addr, m1.size, magicVal1);

    // attach
    auto m2 = s.attachShm(seg2, getNewUnmappedAddr(), seg2Opt);
    writeToMemory(m2.addr, m2.size, magicVal2);
    checkMemory(m2.addr, m2.size, magicVal2);
    // no clean shutdown this time.
  });

  // try to create new segments. This should destroy the previous segments
  {
    ShmManager s(cacheDir, posix);
    // try attach, but it should fail.
    ASSERT_THROW(s.attachShm(seg1, nullptr, seg1Opt), std::invalid_argument);

    // try attach
    ASSERT_THROW(s.attachShm(seg2, nullptr, seg2Opt), std::invalid_argument);

    // now create new segments with same name. This should remove the
    // previous version of the segments with same name.
    ASSERT_NO_THROW({
      auto m1 = s.createShm(seg1, getRandomSize(), addr, seg1Opt);
      checkMemory(m1.addr, m1.size, 0);
      writeToMemory(m1.addr, m1.size, magicVal1);
      checkMemory(m1.addr, m1.size, magicVal1);

      auto m2 = s.createShm(seg2, getRandomSize(), getNewUnmappedAddr(), seg2Opt);
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
    auto m1 = s.createShm(seg1, getRandomSize(), addr, seg1Opt);
    // ensure its the new one.
    checkMemory(m1.addr, m1.size, 0);
    writeToMemory(m1.addr, m1.size, magicVal2);

    auto m2 = s.attachShm(seg2, getNewUnmappedAddr(), seg2Opt);
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
    auto m1 = s.attachShm(seg1, addr, seg1Opt);
    checkMemory(m1.addr, m1.size, magicVal2);

    // attach
    auto m2 = s.attachShm(seg2, getNewUnmappedAddr(), seg2Opt);
    checkMemory(m2.addr, m2.size, magicVal1);
    // no clean shutdown this time.
  });
}

TEST_F(ShmManagerTestPosix, Segments) { testSegments(true); }

TEST_F(ShmManagerTestSysV, Segments) { testSegments(false); }

TEST_F(ShmManagerTestFile, Segments) { testSegments(false); }

void ShmManagerTest::testShutDown(bool posix) {
  // pid-X to keep it unique so we dont collude with other tests
  int num = 0;
  const std::string segmentPrefix = std::to_string(::getpid());
  auto segment1 = makeSegment(segmentPrefix + "-" + std::to_string(num++));
  auto segment2 = makeSegment(segmentPrefix + "-" + std::to_string(num++));
  auto segment3 = makeSegment(segmentPrefix + "-" + std::to_string(num++));
  const auto seg1 = segment1.first;
  const auto seg2 = segment2.first;
  const auto seg3 = segment3.first;
  const auto seg1Opt = segment1.second;
  const auto seg2Opt = segment2.second;
  const auto seg3Opt = segment3.second;
  size_t seg1Size = 0;
  size_t seg2Size = 0;
  size_t seg3Size = 0;

  // open an instance and create some segments
  ASSERT_NO_THROW({
    ShmManager s(cacheDir, posix);

    seg1Size = getRandomSize();
    s.createShm(seg1, seg1Size, nullptr, seg1Opt);
    auto& shm1 = s.getShmByName(seg1);
    ASSERT_EQ(shm1.getSize(), seg1Size);

    seg2Size = getRandomSize();
    s.createShm(seg2, seg2Size, nullptr, seg2Opt);
    auto& shm2 = s.getShmByName(seg2);
    ASSERT_EQ(shm2.getSize(), seg2Size);

    seg3Size = getRandomSize();
    s.createShm(seg3, seg3Size, nullptr, seg3Opt);
    auto& shm3 = s.getShmByName(seg3);
    ASSERT_EQ(shm3.getSize(), seg3Size);

    ASSERT_TRUE(s.shutDown() == ShutDownRes::kSuccess);
  });

  // should be able to attach to all of them.
  ASSERT_NO_THROW({
    ShmManager s(cacheDir, posix);

    s.attachShm(seg1, nullptr, seg1Opt);
    auto& shm1 = s.getShmByName(seg1);
    ASSERT_EQ(shm1.getSize(), seg1Size);

    s.attachShm(seg2, nullptr, seg2Opt);
    auto& shm2 = s.getShmByName(seg2);
    ASSERT_EQ(shm2.getSize(), seg2Size);

    s.attachShm(seg3, nullptr, seg3Opt);
    auto& shm3 = s.getShmByName(seg3);
    ASSERT_EQ(shm3.getSize(), seg3Size);

    ASSERT_TRUE(s.shutDown() == ShutDownRes::kSuccess);
  });

  // should be able to attach to all of them. attach only seg1 and seg3. seg2
  // should be destroyed.
  ASSERT_NO_THROW({
    ShmManager s(cacheDir, posix);

    s.attachShm(seg1, nullptr, seg1Opt);
    auto& shm1 = s.getShmByName(seg1);
    ASSERT_EQ(shm1.getSize(), seg1Size);

    s.attachShm(seg3, nullptr, seg3Opt);
    auto& shm3 = s.getShmByName(seg3);
    ASSERT_EQ(shm3.getSize(), seg3Size);

    ASSERT_TRUE(s.shutDown() == ShutDownRes::kSuccess);
  });

  // we should only find seg1 and seg3. seg2 should be missing for attach and
  // we should be able to create a new one.
  {
    ShmManager s(cacheDir, posix);

    ASSERT_NO_THROW({
      s.attachShm(seg1, nullptr, seg1Opt);
      auto& shm1 = s.getShmByName(seg1);
      ASSERT_EQ(shm1.getSize(), seg1Size);

      s.attachShm(seg3, nullptr, seg3Opt);
      auto& shm3 = s.getShmByName(seg3);
      ASSERT_EQ(shm3.getSize(), seg3Size);
    });

    ASSERT_THROW(s.attachShm(seg2, nullptr, seg2Opt), std::invalid_argument);

    // create a new one. this is possible only because the previous one was
    // destroyed.
    ASSERT_NO_THROW(s.createShm(seg2, seg2Size, nullptr, seg2Opt));
    ASSERT_EQ(s.getShmByName(seg2).getSize(), seg2Size);
    auto *v = std::get_if<PosixSysVSegmentOpts>(&s.getShmTypeByName(seg2));
    if (v)
      ASSERT_EQ(v->usePosix, posix);

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

    ASSERT_THROW(s.attachShm(seg1, nullptr, seg1Opt), std::invalid_argument);

    ASSERT_THROW(s.attachShm(seg2, nullptr, seg2Opt), std::invalid_argument);

    ASSERT_THROW(s.attachShm(seg3, nullptr, seg3Opt), std::invalid_argument);

    ASSERT_NO_THROW(s.createShm(seg1, seg1Size, nullptr, seg1Opt));
    ASSERT_EQ(s.getShmByName(seg1).getSize(), seg1Size);

    ASSERT_NO_THROW(s.createShm(seg2, seg2Size, nullptr, seg3Opt));
    ASSERT_EQ(s.getShmByName(seg2).getSize(), seg2Size);

    ASSERT_NO_THROW(s.createShm(seg3, seg3Size, nullptr, seg3Opt));
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

TEST_F(ShmManagerTestFile, ShutDown) { testShutDown(false); }

void ShmManagerTest::testCleanup(bool posix) {
  // pid-X to keep it unique so we dont collude with other tests
  int num = 0;
  const std::string segmentPrefix = std::to_string(::getpid());
  auto segment1 = makeSegment(segmentPrefix + "-" + std::to_string(num++));
  auto segment2 = makeSegment(segmentPrefix + "-" + std::to_string(num++));
  auto segment3 = makeSegment(segmentPrefix + "-" + std::to_string(num++));
  const auto seg1 = segment1.first;
  const auto seg2 = segment2.first;
  const auto seg3 = segment3.first;
  const auto seg1Opt = segment1.second;
  const auto seg2Opt = segment2.second;
  const auto seg3Opt = segment3.second;
  size_t seg1Size = 0;
  size_t seg2Size = 0;
  size_t seg3Size = 0;

  // open an instance and create some segments
  ASSERT_NO_THROW({
    ShmManager s(cacheDir, posix);

    seg1Size = getRandomSize();
    s.createShm(seg1, seg1Size, nullptr, seg1Opt);
    auto& shm1 = s.getShmByName(seg1);
    ASSERT_EQ(shm1.getSize(), seg1Size);

    seg2Size = getRandomSize();
    s.createShm(seg2, seg2Size, nullptr, seg3Opt);
    auto& shm2 = s.getShmByName(seg2);
    ASSERT_EQ(shm2.getSize(), seg2Size);

    seg3Size = getRandomSize();
    s.createShm(seg3, seg3Size, nullptr, seg3Opt);
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

    ASSERT_THROW(s.attachShm(seg1, nullptr, seg1Opt), std::invalid_argument);

    ASSERT_THROW(s.attachShm(seg2, nullptr, seg2Opt), std::invalid_argument);

    ASSERT_THROW(s.attachShm(seg3, nullptr, seg3Opt), std::invalid_argument);

    ASSERT_NO_THROW({
      s.createShm(seg1, seg1Size, nullptr, seg1Opt);
      auto& shm1 = s.getShmByName(seg1);
      ASSERT_EQ(shm1.getSize(), seg1Size);

      s.createShm(seg2, seg2Size, nullptr, seg2Opt);
      auto& shm2 = s.getShmByName(seg2);
      ASSERT_EQ(shm2.getSize(), seg2Size);

      s.createShm(seg3, seg3Size, nullptr, seg3Opt);
      auto& shm3 = s.getShmByName(seg3);
      ASSERT_EQ(shm3.getSize(), seg3Size);
    });
    // dont call shutdown
  }
}

TEST_F(ShmManagerTestPosix, Cleanup) { testCleanup(true); }

TEST_F(ShmManagerTestSysV, Cleanup) { testCleanup(false); }

TEST_F(ShmManagerTestFile, Cleanup) { testCleanup(false); }

void ShmManagerTest::testAttachReadOnly(bool posix) {
  // pid-X to keep it unique so we dont collude with other tests
  int num = 0;
  const std::string segmentPrefix = std::to_string(::getpid());
  auto segment1 = makeSegment(segmentPrefix + "-" + std::to_string(num++));
  const auto seg = segment1.first;
  const auto segOpt = segment1.second;
  size_t segSize = 0;

  // open an instance and create segment
  ShmManager s(cacheDir, posix);

  segSize = getRandomSize();
  s.createShm(seg, segSize, nullptr, segOpt);
  auto& shm = s.getShmByName(seg);
  ASSERT_EQ(shm.getSize(), segSize);
  const unsigned char magicVal = 'd';
  writeToMemory(shm.getCurrentMapping().addr, segSize, magicVal);

  auto roShm = ShmManager::attachShmReadOnly(cacheDir, seg, segOpt.typeOpts);
  ASSERT_NE(roShm.get(), nullptr);
  ASSERT_TRUE(roShm->isMapped());
  checkMemory(roShm->getCurrentMapping().addr, segSize, magicVal);

  auto addr = getNewUnmappedAddr();
  roShm = ShmManager::attachShmReadOnly(cacheDir, seg, segOpt.typeOpts, addr);
  ASSERT_NE(roShm.get(), nullptr);
  ASSERT_TRUE(roShm->isMapped());
  ASSERT_EQ(roShm->getCurrentMapping().addr, addr);
  checkMemory(roShm->getCurrentMapping().addr, segSize, magicVal);
}

TEST_F(ShmManagerTestPosix, AttachReadOnly) { testAttachReadOnly(true); }

TEST_F(ShmManagerTestSysV, AttachReadOnly) { testAttachReadOnly(false); }

TEST_F(ShmManagerTestFile, AttachReadOnly) { testAttachReadOnly(false); }

// test to ensure that segments can be created with a new cache dir, attached
// from existing cache dir, segments can be deleted and recreated using the
// same cache dir if they have not been attached to already.
void ShmManagerTest::testMappingAlignment(bool posix) {
  // pid-X to keep it unique so we dont collude with other tests
  int num = 0;
  const std::string segmentPrefix = std::to_string(::getpid());
  auto segment1 = makeSegment(segmentPrefix + "-" + std::to_string(num++));
  auto segment2 = makeSegment(segmentPrefix + "-" + std::to_string(num++));
  const auto seg1 = segment1.first;
  const auto seg2 = segment2.first;
  auto seg1Opt = segment1.second;
  auto seg2Opt = segment2.second;
  const char magicVal1 = 'f';
  const char magicVal2 = 'n';

  {
    ShmManager s(cacheDir, posix);
    seg1Opt.alignment = 1ULL << folly::Random::rand32(0, 18);
    auto m1 = s.createShm(seg1, getRandomSize(), nullptr, seg1Opt);
    ASSERT_EQ(reinterpret_cast<uint64_t>(m1.addr) & (seg1Opt.alignment - 1), 0);
    writeToMemory(m1.addr, m1.size, magicVal1);
    checkMemory(m1.addr, m1.size, magicVal1);
    // invalid alignment should throw
    seg2Opt.alignment = folly::Random::rand32(1 << 23, 1 << 24);
    ASSERT_THROW(s.createShm(seg2, getRandomSize(), nullptr, seg2Opt),
                 std::invalid_argument);
    ASSERT_THROW(s.getShmByName(seg2), std::invalid_argument);

    auto addr = getNewUnmappedAddr();
    // alignment option is ignored when using explicit address
    seg2Opt.alignment = folly::Random::rand32(1 << 23, 1 << 24);
    auto m2 = s.createShm(seg2, getRandomSize(), addr, seg2Opt);
    ASSERT_EQ(m2.addr, addr);
    writeToMemory(m2.addr, m2.size, magicVal2);
    checkMemory(m2.addr, m2.size, magicVal2);
    s.shutDown();
  }

  // try to attach
  {
    ShmManager s(cacheDir, posix);

    // can choose a different alignemnt
    facebook::cachelib::ShmSegmentOpts opts;
    seg1Opt.alignment = 1ULL << folly::Random::rand32(18, 22);
    // attach
    auto m1 = s.attachShm(seg1, nullptr, seg1Opt);
    ASSERT_EQ(reinterpret_cast<uint64_t>(m1.addr) & (seg1Opt.alignment - 1), 0);
    checkMemory(m1.addr, m1.size, magicVal1);

    // alignment can be enabled on previously explicitly mapped segments
    seg2Opt.alignment = 1ULL << folly::Random::rand32(1, 22);
    auto m2 = s.attachShm(seg2, nullptr, seg2Opt);
    ASSERT_EQ(reinterpret_cast<uint64_t>(m2.addr) & (seg2Opt.alignment - 1), 0);
    checkMemory(m2.addr, m2.size, magicVal2);
  };
}
TEST_F(ShmManagerTestPosix, TestMappingAlignment) {
  testMappingAlignment(true);
}

TEST_F(ShmManagerTestSysV, TestMappingAlignment) {
  testMappingAlignment(false);
}

TEST_F(ShmManagerTestFile, TestMappingAlignment) {
  testMappingAlignment(false);
}

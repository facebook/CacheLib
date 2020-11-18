#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "cachelib/navy/kangaroo/KangarooLog.h"
#include "cachelib/navy/common/Types.h"

#include "cachelib/navy/driver/Driver.h"
#include "cachelib/navy/testing/BufferGen.h"
#include "cachelib/navy/testing/MockDevice.h"

using testing::_;
using testing::NiceMock;

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
namespace {
void setLog(KangarooLog::Config& config,
    uint32_t readSize,
    uint32_t threshold, 
    SetNumberCallback setNumCb,
    SetMultiInsertCallback insertCb) {
  config.readSize = readSize;
  config.segmentSize = 2 * config.readSize;
  config.logSize = 6 * config.segmentSize;
  config.threshold = threshold;
  config.setNumberCallback = setNumCb;
  config.setMultiInsertCallback = insertCb;
}
} // namespace

TEST(KangarooLog, BasicOps) {
  std::cout << "Basic ops" << std::endl;
  SetMultiInsertCallback testInsertCb = [](HashedKey hk, NextSetItemInLogCallback cb, ReadmitCallback readmit) {return;};
  auto testSetNumCb = [](uint64_t id) { return KangarooBucketId(id % 7); };
  KangarooLog::Config config;
  setLog(config, 64, 2, testSetNumCb, testInsertCb);
  config.logIndexPartitions = 4;
  config.logPhysicalPartitions = 2;
  config.numIndexPartitionSlots = 5;
  auto device = std::make_unique<NiceMock<MockDevice>>(config.logSize, 64);
  config.device = device.get();
  
  KangarooLog kl(std::move(config));

  const auto hk1 = makeHK("key 1");
  const auto hk2 = makeHK("key 2");
  const auto hk3 = makeHK("key 3");

  Status ret = kl.insert(hk1, makeView("value 1"));
  EXPECT_EQ(ret, Status::Ok);
  Buffer value;
  EXPECT_EQ(kl.lookup(hk1, value), Status::Ok);
  EXPECT_EQ(makeView("value 1"), value.view());
  
  ret = kl.insert(hk2, makeView("value 2"));
  EXPECT_EQ(ret, Status::Ok);
  EXPECT_EQ(kl.lookup(hk1, value), Status::Ok);
  EXPECT_EQ(makeView("value 1"), value.view());
  EXPECT_EQ(kl.lookup(hk2, value), Status::Ok);
  EXPECT_EQ(makeView("value 2"), value.view());
  
  // Check for lookup after written to flash
  ret = kl.insert(hk3, makeView("value 3"));
  EXPECT_EQ(ret, Status::Ok);
  EXPECT_EQ(kl.lookup(hk1, value), Status::Ok);
  EXPECT_EQ(makeView("value 1"), value.view());
  EXPECT_EQ(kl.lookup(hk2, value), Status::Ok);
  EXPECT_EQ(makeView("value 2"), value.view());
  EXPECT_EQ(kl.lookup(hk3, value), Status::Ok);
  EXPECT_EQ(makeView("value 3"), value.view());

  EXPECT_EQ(kl.remove(hk1), Status::Ok); 
  EXPECT_EQ(kl.lookup(hk1, value), Status::NotFound);

  // trigger merging
  EXPECT_EQ(kl.insert(makeHK("key 4"), makeView("value 4")), Status::Ok);
  EXPECT_EQ(kl.insert(makeHK("key 5"), makeView("value 5")), Status::Ok);
  EXPECT_EQ(kl.lookup(makeHK("key 5"), value), Status::Ok);
  EXPECT_EQ(makeView("value 5"), value.view());

  // overwrite 1st segment
  EXPECT_EQ(kl.insert(makeHK("key 6"), makeView("value 6")), Status::Ok);
  EXPECT_EQ(kl.insert(makeHK("key 7"), makeView("value 7")), Status::Ok);
  EXPECT_EQ(kl.lookup(makeHK("key 7"), value), Status::Ok);
  EXPECT_EQ(makeView("value 7"), value.view());
  EXPECT_EQ(kl.lookup(hk1, value), Status::NotFound);
  EXPECT_EQ(kl.lookup(hk2, value), Status::Ok); /* only flushed one partition */
}

}
} // namespace navy
} // namespace cachelib
} // namespace facebook

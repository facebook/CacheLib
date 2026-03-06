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

#include <gtest/gtest.h>

#include <fstream>
#include <string>
#include <vector>

#include "cachelib/cachebench/workload/PieceWiseCache.h"
#include "folly/String.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

class PieceWiseCacheTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::string testKey = "test";
    std::vector<std::string> extraFieldV1, extraFieldV2, extraFieldV3;
    std::unordered_map<uint32_t, std::vector<std::string>> statsPerAggField;
    std::unordered_map<std::string, std::string> admFeatureMap1, admFeatureMap2,
        admFeatureMap3;
    std::string itemValue;

    piecewiseCache =
        std::make_unique<PieceWiseCacheAdapter>(/*maxCachePiecesv=*/32000,
                                                /*numAggregationFields=*/0,
                                                statsPerAggField);

    // pieceReq1 is stored as multiple pieces, and the range contains two
    // pieces
    pieceReq1 = std::make_unique<PieceWiseReqWrapper>(
        /*cachePieceSize=*/65536,
        /*timestamp=*/1000,
        /*reqId=*/1,
        /*opType=*/OpType::kGet,
        /*key=*/testKey,
        /*fullContentSize=*/1000000,
        /*responseHeaderSize=*/1000,
        /*rangeStart=*/100,
        /*rangeEnd=*/100000,
        /*ttl=*/3600,
        /*extraField=*/extraFieldV1,
        /*admFeatureMap=*/admFeatureMap1,
        /*isHit=*/folly::none,
        /*itemValue=*/itemValue);

    // pieceReq2 is stored as multiple pieces, and the range contains a single
    // piece
    pieceReq2 = std::make_unique<PieceWiseReqWrapper>(
        /*cachePieceSize=*/65536,
        /*timestamp=*/1000,
        /*reqId=*/1,
        /*opType=*/OpType::kGet,
        /*key=*/testKey,
        /*fullContentSize=*/1000000,
        /*responseHeaderSize=*/1000,
        /*rangeStart=*/100,
        /*rangeEnd=*/65530,
        /*ttl=*/3600,
        /*extraField=*/extraFieldV2,
        /*admFeatureMap=*/admFeatureMap2,
        /*isHit=*/folly::none,
        /*itemValue=*/itemValue);

    // nonPieceReq is stored as whole object
    nonPieceReq = std::make_unique<PieceWiseReqWrapper>(
        /*cachePieceSize=*/65536,
        /*timestamp=*/1000,
        /*reqId=*/1,
        /*opType=*/OpType::kGet,
        /*key=*/testKey,
        /*fullContentSize=*/10000,
        /*responseHeaderSize=*/1000,
        /*rangeStart=*/100,
        /*rangeEnd=*/6000,
        /*ttl=*/3600,
        /*extraField=*/extraFieldV3,
        /*admFeatureMap=*/admFeatureMap3,
        /*isHit=*/folly::none,
        /*itemValue=*/itemValue);
  }

  std::unique_ptr<PieceWiseCacheAdapter> piecewiseCache;
  std::unique_ptr<PieceWiseReqWrapper> pieceReq1, pieceReq2, nonPieceReq;
};

TEST_F(PieceWiseCacheTest, PieceWiseReqWrapperTest) {
  // piece cache request
  PieceWiseReqWrapper pieceReqCopy(*pieceReq1);
  EXPECT_NE(&pieceReq1->baseKey, &pieceReqCopy.baseKey);
  EXPECT_NE(&pieceReq1->pieceKey, &pieceReqCopy.pieceKey);
  EXPECT_NE(&pieceReq1->req.key, &pieceReqCopy.req.key);
  EXPECT_EQ(pieceReq1->req.key, "test");
  EXPECT_EQ(pieceReqCopy.req.key, "test");
  EXPECT_EQ(*(pieceReq1->req.sizeBegin), 0);
  EXPECT_EQ(*(pieceReqCopy.req.sizeBegin), 0);
  EXPECT_EQ(pieceReq1->pieceType, PieceType::Metadata);
  EXPECT_EQ(pieceReqCopy.pieceType, PieceType::Metadata);
  EXPECT_EQ(pieceReqCopy.requestRange.getRequestRange()->first, 100);
  EXPECT_EQ(pieceReqCopy.requestRange.getRequestRange()->second.value(),
            100000);

  // update pieceReqCopy, but not pieceReq1
  auto nextPieceIndex = pieceReqCopy.cachePieces->getCurFetchingPieceIndex();
  EXPECT_EQ(nextPieceIndex, 0);
  pieceReqCopy.updatePieceKey(GenericPiecesBase::createPieceKey(
      pieceReqCopy.baseKey,
      nextPieceIndex,
      pieceReqCopy.cachePieces->getPiecesPerGroup()));
  pieceReqCopy.sizes[0] =
      pieceReqCopy.cachePieces->getSizeOfAPiece(nextPieceIndex);
  pieceReqCopy.pieceType = PieceType::Body;
  pieceReqCopy.cachePieces->updateFetchIndex();
  EXPECT_EQ(pieceReq1->req.key, "test");
  EXPECT_EQ(pieceReqCopy.req.key, "test|#|body-0-0");
  EXPECT_EQ(*(pieceReq1->req.sizeBegin), 0);
  EXPECT_EQ(*(pieceReqCopy.req.sizeBegin), 65536);
  EXPECT_EQ(pieceReq1->pieceType, PieceType::Metadata);
  EXPECT_EQ(pieceReqCopy.pieceType, PieceType::Body);
  EXPECT_EQ(pieceReqCopy.cachePieces->getCurFetchingPieceIndex(), 1);

  // check piece index is correct in the copy
  PieceWiseReqWrapper pieceReqCopy1(pieceReqCopy);
  EXPECT_EQ(pieceReqCopy1.pieceType, PieceType::Body);
  EXPECT_EQ(pieceReqCopy1.cachePieces->getCurFetchingPieceIndex(), 1);

  // non-piece cache request
  PieceWiseReqWrapper nonPieceReqCopy(*nonPieceReq);
  EXPECT_NE(&nonPieceReq->baseKey, &nonPieceReqCopy.baseKey);
  EXPECT_NE(&nonPieceReq->pieceKey, &nonPieceReqCopy.pieceKey);
  EXPECT_NE(&nonPieceReq->req.key, &nonPieceReqCopy.req.key);
  EXPECT_EQ(nonPieceReq->req.key, "test");
  EXPECT_EQ(nonPieceReqCopy.req.key, "test");
  EXPECT_EQ(*(nonPieceReq->req.sizeBegin), 11000);
  EXPECT_EQ(*(nonPieceReqCopy.req.sizeBegin), 11000);
  EXPECT_EQ(nonPieceReq->pieceType, PieceType::Metadata);
  EXPECT_EQ(nonPieceReqCopy.pieceType, PieceType::Metadata);
  EXPECT_EQ(nonPieceReqCopy.requestRange.getRequestRange()->first, 100);
  EXPECT_EQ(nonPieceReqCopy.requestRange.getRequestRange()->second.value(),
            6000);

  // update nonPieceReqCopy, but not nonPieceReq
  nonPieceReqCopy.req.setOp(OpType::kSet);
  EXPECT_EQ(nonPieceReq->req.getOp(), OpType::kGet);
  EXPECT_EQ(nonPieceReqCopy.req.getOp(), OpType::kSet);
}

TEST_F(PieceWiseCacheTest, CacheHitTest) {
  const auto& stat = piecewiseCache->getStats().getInternalStats();

  // process pieceReq1
  piecewiseCache->recordNewReq(*pieceReq1);
  // metadata piece hits
  EXPECT_FALSE(piecewiseCache->processReq(*pieceReq1, OpResultType::kGetHit));
  // header piece hits
  EXPECT_FALSE(piecewiseCache->processReq(*pieceReq1, OpResultType::kGetHit));
  // first body piece hits, more body pieces are left
  EXPECT_FALSE(piecewiseCache->processReq(*pieceReq1, OpResultType::kGetHit));
  // second body piece hits, this is the last piece: done
  EXPECT_TRUE(piecewiseCache->processReq(*pieceReq1, OpResultType::kGetHit));
  EXPECT_EQ(stat.getBytes.get(), 132072);
  EXPECT_EQ(stat.getHitBytes.get(), 132072);
  EXPECT_EQ(stat.getFullHitBytes.get(), 132072);
  EXPECT_EQ(stat.getBodyBytes.get(), 131072);
  EXPECT_EQ(stat.getHitBodyBytes.get(), 131072);
  EXPECT_EQ(stat.getFullHitBodyBytes.get(), 131072);
  EXPECT_EQ(stat.totalIngressBytes.get(), 0);
  EXPECT_EQ(stat.totalEgressBytes.get(), 100901);
  EXPECT_EQ(stat.objGets.get(), 1);
  EXPECT_EQ(stat.objGetHits.get(), 1);
  EXPECT_EQ(stat.objGetFullHits.get(), 1);

  // process pieceReq2
  piecewiseCache->recordNewReq(*pieceReq2);
  // metadata piece hits
  EXPECT_FALSE(piecewiseCache->processReq(*pieceReq2, OpResultType::kGetHit));
  // header piece hits
  EXPECT_FALSE(piecewiseCache->processReq(*pieceReq2, OpResultType::kGetHit));
  // first body piece hits: done
  EXPECT_TRUE(piecewiseCache->processReq(*pieceReq2, OpResultType::kGetHit));
  EXPECT_EQ(stat.getBytes.get(), 198608);
  EXPECT_EQ(stat.getHitBytes.get(), 198608);
  EXPECT_EQ(stat.getFullHitBytes.get(), 198608);
  EXPECT_EQ(stat.getBodyBytes.get(), 196608);
  EXPECT_EQ(stat.getHitBodyBytes.get(), 196608);
  EXPECT_EQ(stat.getFullHitBodyBytes.get(), 196608);
  EXPECT_EQ(stat.totalIngressBytes.get(), 0);
  EXPECT_EQ(stat.totalEgressBytes.get(), 167332);
  EXPECT_EQ(stat.objGets.get(), 2);
  EXPECT_EQ(stat.objGetHits.get(), 2);
  EXPECT_EQ(stat.objGetFullHits.get(), 2);

  // process nonPieceReq
  piecewiseCache->recordNewReq(*nonPieceReq);
  // object hits: done
  EXPECT_TRUE(piecewiseCache->processReq(*nonPieceReq, OpResultType::kGetHit));
  EXPECT_EQ(stat.getBytes.get(), 209608);
  EXPECT_EQ(stat.getHitBytes.get(), 209608);
  EXPECT_EQ(stat.getFullHitBytes.get(), 209608);
  EXPECT_EQ(stat.getBodyBytes.get(), 206608);
  EXPECT_EQ(stat.getHitBodyBytes.get(), 206608);
  EXPECT_EQ(stat.getFullHitBodyBytes.get(), 206608);
  EXPECT_EQ(stat.totalIngressBytes.get(), 0);
  EXPECT_EQ(stat.totalEgressBytes.get(), 174233);
  EXPECT_EQ(stat.objGets.get(), 3);
  EXPECT_EQ(stat.objGetHits.get(), 3);
  EXPECT_EQ(stat.objGetFullHits.get(), 3);
}

TEST_F(PieceWiseCacheTest, CacheMissTest) {
  const auto& stat = piecewiseCache->getStats().getInternalStats();

  // process pieceReq1
  piecewiseCache->recordNewReq(*pieceReq1);
  // metadata piece misses
  EXPECT_FALSE(piecewiseCache->processReq(*pieceReq1, OpResultType::kGetMiss));
  EXPECT_EQ(pieceReq1->req.getOp(), OpType::kSet);
  // set metadata piece, more pieces needed
  EXPECT_FALSE(
      piecewiseCache->processReq(*pieceReq1, OpResultType::kSetSuccess));
  // set header piece, more pieces needed
  EXPECT_FALSE(
      piecewiseCache->processReq(*pieceReq1, OpResultType::kSetSuccess));
  // set first body piece, more body pieces are left
  EXPECT_FALSE(
      piecewiseCache->processReq(*pieceReq1, OpResultType::kSetSuccess));
  // set second body piece, this is the last piece: done
  EXPECT_TRUE(
      piecewiseCache->processReq(*pieceReq1, OpResultType::kSetSuccess));
  EXPECT_EQ(stat.getBytes.get(), 132072);
  EXPECT_EQ(stat.getHitBytes.get(), 0);
  EXPECT_EQ(stat.getFullHitBytes.get(), 0);
  EXPECT_EQ(stat.getBodyBytes.get(), 131072);
  EXPECT_EQ(stat.getHitBodyBytes.get(), 0);
  EXPECT_EQ(stat.getFullHitBodyBytes.get(), 0);
  EXPECT_EQ(stat.totalIngressBytes.get(), 132072);
  EXPECT_EQ(stat.totalEgressBytes.get(), 100901);
  EXPECT_EQ(stat.objGets.get(), 1);
  EXPECT_EQ(stat.objGetHits.get(), 0);
  EXPECT_EQ(stat.objGetFullHits.get(), 0);

  // process pieceReq2
  piecewiseCache->recordNewReq(*pieceReq2);
  EXPECT_FALSE(piecewiseCache->processReq(*pieceReq2, OpResultType::kGetMiss));
  EXPECT_EQ(pieceReq2->req.getOp(), OpType::kSet);
  // set metadata piece, more pieces needed
  EXPECT_FALSE(
      piecewiseCache->processReq(*pieceReq2, OpResultType::kSetSuccess));
  // set header piece, more pieces needed
  EXPECT_FALSE(
      piecewiseCache->processReq(*pieceReq2, OpResultType::kSetSuccess));
  EXPECT_EQ(pieceReq2->req.getOp(), OpType::kSet);
  // set first body piece: done
  EXPECT_TRUE(
      piecewiseCache->processReq(*pieceReq2, OpResultType::kSetSuccess));
  EXPECT_EQ(stat.getBytes.get(), 198608);
  EXPECT_EQ(stat.getHitBytes.get(), 0);
  EXPECT_EQ(stat.getFullHitBytes.get(), 0);
  EXPECT_EQ(stat.getBodyBytes.get(), 196608);
  EXPECT_EQ(stat.getHitBodyBytes.get(), 0);
  EXPECT_EQ(stat.getFullHitBodyBytes.get(), 0);
  EXPECT_EQ(stat.totalIngressBytes.get(), 198608);
  EXPECT_EQ(stat.totalEgressBytes.get(), 167332);
  EXPECT_EQ(stat.objGets.get(), 2);
  EXPECT_EQ(stat.objGetHits.get(), 0);
  EXPECT_EQ(stat.objGetFullHits.get(), 0);

  // process nonPieceReq
  piecewiseCache->recordNewReq(*nonPieceReq);
  EXPECT_FALSE(
      piecewiseCache->processReq(*nonPieceReq, OpResultType::kGetMiss));
  EXPECT_EQ(nonPieceReq->req.getOp(), OpType::kSet);
  EXPECT_TRUE(
      piecewiseCache->processReq(*nonPieceReq, OpResultType::kSetSuccess));
  EXPECT_EQ(stat.getBytes.get(), 209608);
  EXPECT_EQ(stat.getHitBytes.get(), 0);
  EXPECT_EQ(stat.getFullHitBytes.get(), 0);
  EXPECT_EQ(stat.getBodyBytes.get(), 206608);
  EXPECT_EQ(stat.getHitBodyBytes.get(), 0);
  EXPECT_EQ(stat.getFullHitBodyBytes.get(), 0);
  EXPECT_EQ(stat.totalIngressBytes.get(), 209608);
  EXPECT_EQ(stat.totalEgressBytes.get(), 174233);
  EXPECT_EQ(stat.objGets.get(), 3);
  EXPECT_EQ(stat.objGetHits.get(), 0);
  EXPECT_EQ(stat.objGetFullHits.get(), 0);
}

TEST_F(PieceWiseCacheTest, CacheHitMissTest) {
  const auto& stat = piecewiseCache->getStats().getInternalStats();

  // process pieceReq1
  piecewiseCache->recordNewReq(*pieceReq1);
  // metadata piece hits
  EXPECT_FALSE(piecewiseCache->processReq(*pieceReq1, OpResultType::kGetHit));
  // header piece hits
  EXPECT_FALSE(piecewiseCache->processReq(*pieceReq1, OpResultType::kGetHit));
  // first body piece hits, more body pieces are left
  EXPECT_FALSE(piecewiseCache->processReq(*pieceReq1, OpResultType::kGetHit));
  // second body piece misses
  EXPECT_FALSE(piecewiseCache->processReq(*pieceReq1, OpResultType::kGetMiss));
  EXPECT_EQ(pieceReq1->req.getOp(), OpType::kSet);
  // set second body piece, this is the last piece: done
  EXPECT_TRUE(
      piecewiseCache->processReq(*pieceReq1, OpResultType::kSetSuccess));
  EXPECT_EQ(stat.getBytes.get(), 132072);
  EXPECT_EQ(stat.getHitBytes.get(), 66536);
  EXPECT_EQ(stat.getFullHitBytes.get(), 0);
  EXPECT_EQ(stat.getBodyBytes.get(), 131072);
  EXPECT_EQ(stat.getHitBodyBytes.get(), 65536);
  EXPECT_EQ(stat.getFullHitBodyBytes.get(), 0);
  EXPECT_EQ(stat.totalIngressBytes.get(), 66536);
  EXPECT_EQ(stat.totalEgressBytes.get(), 100901);
  EXPECT_EQ(stat.objGets.get(), 1);
  EXPECT_EQ(stat.objGetHits.get(), 1);
  EXPECT_EQ(stat.objGetFullHits.get(), 0);

  // process pieceReq2
  piecewiseCache->recordNewReq(*pieceReq2);
  // metadata piece hits
  EXPECT_FALSE(piecewiseCache->processReq(*pieceReq2, OpResultType::kGetHit));
  // header piece hits
  EXPECT_FALSE(piecewiseCache->processReq(*pieceReq2, OpResultType::kGetHit));
  // first body piece misses
  EXPECT_FALSE(piecewiseCache->processReq(*pieceReq2, OpResultType::kGetMiss));
  EXPECT_EQ(pieceReq2->req.getOp(), OpType::kSet);
  // set first body piece: done
  EXPECT_TRUE(
      piecewiseCache->processReq(*pieceReq2, OpResultType::kSetSuccess));
  EXPECT_EQ(stat.getBytes.get(), 198608);
  EXPECT_EQ(stat.getHitBytes.get(), 67536);
  EXPECT_EQ(stat.getFullHitBytes.get(), 0);
  EXPECT_EQ(stat.getBodyBytes.get(), 196608);
  EXPECT_EQ(stat.getHitBodyBytes.get(), 65536);
  EXPECT_EQ(stat.getFullHitBodyBytes.get(), 0);
  EXPECT_EQ(stat.totalIngressBytes.get(), 133072);
  EXPECT_EQ(stat.totalEgressBytes.get(), 167332);
  EXPECT_EQ(stat.objGets.get(), 2);
  EXPECT_EQ(stat.objGetHits.get(), 2);
  EXPECT_EQ(stat.objGetFullHits.get(), 0);
}

TEST_F(PieceWiseCacheTest, MissTraceOutputSmallTest) {
  // Create a temporary file for miss trace output
  const std::string missTraceFile = "/tmp/test_miss_trace.csv";

  std::string testKey = "test_key";
  std::vector<std::string> extraFieldV{"handler1", "content_type1", "vip1"};
  std::unordered_map<uint32_t, std::vector<std::string>> statsPerAggField;
  std::unordered_map<std::string, std::string> admFeatureMap;
  std::string itemValue = "item123";

  // Create adapter with miss trace file
  auto adapterWithTrace = std::make_unique<PieceWiseCacheAdapter>(
      /*maxCachePieces=*/32000,
      /*numAggregationFields=*/3,
      statsPerAggField,
      missTraceFile);

  // Create a request with custom sampling rate
  auto pieceReq = std::make_unique<PieceWiseReqWrapper>(
      /*cachePieceSize=*/65536,
      /*timestamp=*/1000000,
      /*reqId=*/1,
      /*opType=*/OpType::kGet,
      /*key=*/testKey,
      /*fullContentSize=*/20000,
      /*responseHeaderSize=*/1000,
      /*rangeStart=*/0,
      /*rangeEnd=*/19999,
      /*ttl=*/3600,
      /*extraField=*/extraFieldV,
      /*admFeatureMap=*/admFeatureMap,
      /*isHit=*/folly::none,
      /*itemValue=*/itemValue,
      /*metadataSize=*/0);

  // Process a miss to trigger miss trace output
  adapterWithTrace->recordNewReq(*pieceReq);
  adapterWithTrace->processReq(*pieceReq, OpResultType::kGetMiss);

  // Read and verify miss trace file
  std::ifstream traceFile(missTraceFile);
  ASSERT_TRUE(traceFile.is_open());

  std::string header;
  std::getline(traceFile, header);
  EXPECT_TRUE(header.find("SamplingRate") != std::string::npos);

  std::string line;
  std::getline(traceFile, line);

  // Parse CSV line
  std::vector<std::string> fields;
  folly::split(',', line, fields);

  // Verify fields (timestamp, key, opType, objectSize, responseSize,
  // headerSize, rangeStart, rangeEnd, TTL, samplingRate, cache_hit, item_value,
  // ...)
  ASSERT_GE(fields.size(), 12);
  EXPECT_EQ(fields[1], testKey);    // cacheKey
  EXPECT_EQ(fields[2], "1");        // OpType (GET)
  EXPECT_EQ(fields[3], "20000");    // objectSize
  EXPECT_EQ(fields[4], "21000");    // responseSize
  EXPECT_EQ(fields[5], "1000");     // headerSize
  EXPECT_EQ(fields[6], "0");        // rangeStart
  EXPECT_EQ(fields[7], "19999");    // rangeEnd
  EXPECT_EQ(fields[8], "3600");     // TTL
  EXPECT_EQ(fields[10], "0");       // cache_hit
  EXPECT_EQ(fields[11], itemValue); // item_value

  // Clean up
  traceFile.close();
  std::remove(missTraceFile.c_str());
}
TEST_F(PieceWiseCacheTest, MissTraceSmallNonRangeFullSizeZeroTest) {
  // Add a test for a small non-range request with fullContentSize = 0
  // Expectation: treat as whole-object (non-piece) with unknown size;
  // responseSize = headerSize
  const std::string missTraceFile =
      "/tmp/test_small_non_range_fullsize_zero.csv";

  std::string testKey = "test_key";
  std::vector<std::string> extraFieldV{"facebook_live_streaming_cache_entry",
                                       "28", "xx-fbcdn"};
  std::unordered_map<uint32_t, std::vector<std::string>> statsPerAggField;
  std::unordered_map<std::string, std::string> admFeatureMap;
  std::string itemValue = "fb_live_streaming_cache_entry";

  auto adapterWithTrace = std::make_unique<PieceWiseCacheAdapter>(
      /*maxCachePieces=*/32000,
      /*numAggregationFields=*/3,
      statsPerAggField,
      missTraceFile);

  // Create a non-range request with fullContentSize = 0 and small header
  // Using timestamp from the provided line: 1764355647959
  // OpType GET, responseHeaderSize 2003, TTL 10800, no range
  auto req = std::make_unique<PieceWiseReqWrapper>(
      /*cachePieceSize=*/65536,
      /*timestamp=*/1764355647959ULL,
      /*reqId=*/1,
      /*opType=*/OpType::kGet,
      /*key=*/testKey,
      /*fullContentSize=*/0,       // full object size unknown/zero
      /*responseHeaderSize=*/2003, // small header
      /*rangeStart=*/-1,           // no range
      /*rangeEnd=*/-1,             // no range
      /*ttl=*/10800,               // TTL from sample
      /*extraField=*/extraFieldV,
      /*admFeatureMap=*/admFeatureMap,
      /*isHit=*/folly::none,
      /*itemValue=*/itemValue,
      /*metadataSize=*/0);

  adapterWithTrace->recordNewReq(*req);
  // Force a miss to trigger trace output
  EXPECT_FALSE(adapterWithTrace->processReq(*req, OpResultType::kGetMiss));

  // Read and verify miss trace file
  std::ifstream traceFile(missTraceFile);
  ASSERT_TRUE(traceFile.is_open());

  std::string header;
  std::getline(traceFile, header);
  EXPECT_TRUE(header.find("SamplingRate") != std::string::npos);

  std::string line;
  std::getline(traceFile, line);

  std::vector<std::string> fields;
  folly::split(',', line, fields);

  // Fields: 0=timestamp, 1=key, 2=opType, 3=objectSize, 4=responseSize,
  // 5=headerSize, 6=rangeStart, 7=rangeEnd, 8=TTL, 9=samplingRate,
  // 10=cache_hit, 11=item_value, ...
  ASSERT_GE(fields.size(), 12);
  EXPECT_EQ(fields[1], testKey);    // key
  EXPECT_EQ(fields[2], "1");        // GET
  EXPECT_EQ(fields[3], "0");        // objectSize is 0
  EXPECT_EQ(fields[5], "2003");     // headerSize
  EXPECT_EQ(fields[6], "-1");       // rangeStart (no range)
  EXPECT_EQ(fields[7], "-1");       // rangeEnd (no range)
  EXPECT_EQ(fields[8], "10800");    // TTL
  EXPECT_EQ(fields[10], "0");       // cache_hit
  EXPECT_EQ(fields[11], itemValue); // item_value

  // responseSize should equal headerSize when body is unknown/absent
  EXPECT_EQ(fields[4], "2003");

  traceFile.close();
  std::remove(missTraceFile.c_str());
}

TEST_F(PieceWiseCacheTest, MissTracePartialHitRangeAdjustmentTest) {
  // Test that miss trace outputs adjusted range for partial hits
  const std::string missTraceFile = "/tmp/test_partial_hit_trace.csv";

  std::string testKey = "partial_hit_key";
  std::vector<std::string> extraFieldV{"handler1", "content_type1", "vip1"};
  std::unordered_map<uint32_t, std::vector<std::string>> statsPerAggField;
  std::unordered_map<std::string, std::string> admFeatureMap;
  std::string itemValue = "item456";

  auto adapterWithTrace = std::make_unique<PieceWiseCacheAdapter>(
      /*maxCachePieces=*/32000,
      /*numAggregationFields=*/3,
      statsPerAggField,
      missTraceFile);

  // Create a piecewise cached request (fullContentSize > cachePieceSize)
  // Range: 0-199999 (200KB), with 64KB pieces
  auto pieceReq = std::make_unique<PieceWiseReqWrapper>(
      /*cachePieceSize=*/65536,
      /*timestamp=*/2000000,
      /*reqId=*/2,
      /*opType=*/OpType::kGet,
      /*key=*/testKey,
      /*fullContentSize=*/200000,
      /*responseHeaderSize=*/1000,
      /*rangeStart=*/0,
      /*rangeEnd=*/199999,
      /*ttl=*/7200,
      /*extraField=*/extraFieldV,
      /*admFeatureMap=*/admFeatureMap,
      /*isHit=*/folly::none,
      /*itemValue=*/itemValue,
      /*metadataSize=*/0);

  adapterWithTrace->recordNewReq(*pieceReq);

  // Simulate partial hit: metadata hits, header hits, first body piece hits,
  // second body piece misses
  EXPECT_FALSE(adapterWithTrace->processReq(*pieceReq,
                                            OpResultType::kGetHit)); // metadata
  EXPECT_FALSE(
      adapterWithTrace->processReq(*pieceReq, OpResultType::kGetHit)); // header
  EXPECT_FALSE(adapterWithTrace->processReq(
      *pieceReq, OpResultType::kGetHit)); // body piece 0
  EXPECT_FALSE(adapterWithTrace->processReq(
      *pieceReq, OpResultType::kGetMiss)); // body piece 1 miss

  // At this point, miss trace should be written with adjusted range
  // rangeStart should be at the start of piece 1 (65536)
  // rangeEnd should be the original request end (199999)

  // Read and verify miss trace
  std::ifstream traceFile(missTraceFile);
  ASSERT_TRUE(traceFile.is_open());

  std::string header;
  std::getline(traceFile, header);

  std::string line;
  std::getline(traceFile, line);

  std::vector<std::string> fields;
  folly::split(',', line, fields);

  // Debug output if test fails
  if (fields.size() < 12) {
    std::cerr << "DEBUG: Line content: '" << line << "'" << std::endl;
    std::cerr << "DEBUG: Fields count: " << fields.size() << std::endl;
    for (size_t i = 0; i < fields.size(); ++i) {
      std::cerr << "DEBUG: Field[" << i << "]: '" << fields[i] << "'"
                << std::endl;
    }
  }

  ASSERT_GE(fields.size(), 12);

  // Field indices: 0=timestamp, 1=key, 2=opType, 3=objectSize, 4=responseSize,
  // 5=headerSize, 6=rangeStart, 7=rangeEnd, 8=TTL, 9=samplingRate, 10=cache_hit

  // Verify adjusted range for partial hit
  // Since first piece (65536 bytes) was a hit, rangeStart should be 65536
  EXPECT_EQ(fields[6], "65536");  // rangeStart (start of piece 1)
  EXPECT_EQ(fields[7], "199999"); // rangeEnd (original request end)

  uint64_t objectSize = folly::to<uint64_t>(fields[3]);
  EXPECT_EQ(objectSize, 200000); // Full object size

  uint64_t rangeStart = folly::to<uint64_t>(fields[6]);
  uint64_t rangeEnd = folly::to<uint64_t>(fields[7]);
  auto bodySize = rangeEnd - rangeStart + 1;

  // Verify responseSize includes header
  uint64_t responseSize = folly::to<uint64_t>(fields[4]);
  uint64_t headerSize = folly::to<uint64_t>(fields[5]);
  EXPECT_EQ(responseSize, bodySize + headerSize); // Should be 135464

  EXPECT_EQ(fields[10], "0"); // cache_hit

  traceFile.close();
  std::remove(missTraceFile.c_str());
}

TEST_F(PieceWiseCacheTest, MissTraceRangeRequestFirstBodyMissTest) {
  // Test that range requests respect the original range boundary in miss traces
  const std::string missTraceFile = "/tmp/test_range_request_trace.csv";

  std::string testKey = "range_request_key";
  std::vector<std::string> extraFieldV{"handler1", "content_type1", "vip1"};
  std::unordered_map<uint32_t, std::vector<std::string>> statsPerAggField;
  std::unordered_map<std::string, std::string> admFeatureMap;
  std::string itemValue = "item789";

  auto adapterWithTrace = std::make_unique<PieceWiseCacheAdapter>(
      /*maxCachePieces=*/32000,
      /*numAggregationFields=*/3,
      statsPerAggField,
      missTraceFile);

  // Create a range request: request only bytes 50000-150000 from a 1MB file
  auto rangeReq = std::make_unique<PieceWiseReqWrapper>(
      /*cachePieceSize=*/65536,
      /*timestamp=*/3000000,
      /*reqId=*/3,
      /*opType=*/OpType::kGet,
      /*key=*/testKey,
      /*fullContentSize=*/1000000,
      /*responseHeaderSize=*/1000,
      /*rangeStart=*/50000,
      /*rangeEnd=*/150000,
      /*ttl=*/3600,
      /*extraField=*/extraFieldV,
      /*admFeatureMap=*/admFeatureMap,
      /*isHit=*/folly::none,
      /*itemValue=*/itemValue,
      /*metadataSize=*/0);

  adapterWithTrace->recordNewReq(*rangeReq);

  // Simulate partial hit: metadata hits, header hits, first body piece misses
  EXPECT_FALSE(adapterWithTrace->processReq(*rangeReq,
                                            OpResultType::kGetHit)); // metadata
  EXPECT_FALSE(
      adapterWithTrace->processReq(*rangeReq, OpResultType::kGetHit)); // header
  EXPECT_FALSE(adapterWithTrace->processReq(
      *rangeReq, OpResultType::kGetMiss)); // First body piece miss

  // Read and verify miss trace
  std::ifstream traceFile(missTraceFile);
  ASSERT_TRUE(traceFile.is_open());

  std::string header;
  std::getline(traceFile, header);

  std::string line;
  std::getline(traceFile, line);

  std::vector<std::string> fields;
  folly::split(',', line, fields);

  ASSERT_GE(fields.size(), 12);

  // For a partial hit on a range request, the range should be aligned with
  // piece boundary
  uint64_t rangeStart = folly::to<uint64_t>(fields[6]);
  uint64_t rangeEnd = folly::to<uint64_t>(fields[7]);

  auto expectedRangeStart = 65536 * (50000 / 65536);
  EXPECT_EQ(rangeStart, expectedRangeStart);
  auto expectedRangeEnd = 65536 * ((150000 + 65536 - 1) / 65536) - 1;
  EXPECT_EQ(rangeEnd, expectedRangeEnd);

  uint64_t objectSize = folly::to<uint64_t>(fields[3]);
  EXPECT_EQ(objectSize, 1000000);
  traceFile.close();
  std::remove(missTraceFile.c_str());
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook

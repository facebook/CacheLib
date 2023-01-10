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

#include <string>
#include <vector>

#include "cachelib/cachebench/workload/PieceWiseCache.h"

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
    std::string itemValue = "";

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
        /*extraField=*/std::move(extraFieldV1),
        /*admFeatureMap=*/std::move(admFeatureMap1),
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
        /*extraField=*/std::move(extraFieldV2),
        /*admFeatureMap=*/std::move(admFeatureMap2),
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
        /*extraField=*/std::move(extraFieldV3),
        /*admFeatureMap=*/std::move(admFeatureMap3),
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
  EXPECT_EQ(pieceReq1->req.key, "test|#|header-0");
  EXPECT_EQ(pieceReqCopy.req.key, "test|#|header-0");
  EXPECT_EQ(*(pieceReq1->req.sizeBegin), 1000);
  EXPECT_EQ(*(pieceReqCopy.req.sizeBegin), 1000);
  EXPECT_TRUE(pieceReq1->isHeaderPiece);
  EXPECT_TRUE(pieceReqCopy.isHeaderPiece);
  EXPECT_EQ(pieceReqCopy.requestRange.getRequestRange()->first, 100);
  EXPECT_EQ(pieceReqCopy.requestRange.getRequestRange()->second.value(),
            100000);

  // update pieceReqCopy, but not pieceReq1
  auto nextPieceIndex = pieceReqCopy.cachePieces->getCurFetchingPieceIndex();
  EXPECT_EQ(nextPieceIndex, 0);
  pieceReqCopy.pieceKey = GenericPieces::createPieceKey(
      pieceReqCopy.baseKey,
      nextPieceIndex,
      pieceReqCopy.cachePieces->getPiecesPerGroup());
  pieceReqCopy.sizes[0] =
      pieceReqCopy.cachePieces->getSizeOfAPiece(nextPieceIndex);
  pieceReqCopy.isHeaderPiece = false;
  pieceReqCopy.cachePieces->updateFetchIndex();
  EXPECT_EQ(pieceReq1->req.key, "test|#|header-0");
  EXPECT_EQ(pieceReqCopy.req.key, "test|#|body-0-0");
  EXPECT_EQ(*(pieceReq1->req.sizeBegin), 1000);
  EXPECT_EQ(*(pieceReqCopy.req.sizeBegin), 65536);
  EXPECT_TRUE(pieceReq1->isHeaderPiece);
  EXPECT_FALSE(pieceReqCopy.isHeaderPiece);
  EXPECT_EQ(pieceReqCopy.cachePieces->getCurFetchingPieceIndex(), 1);

  // check piece index is correct in the copy
  PieceWiseReqWrapper pieceReqCopy1(pieceReqCopy);
  EXPECT_FALSE(pieceReqCopy1.isHeaderPiece);
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
  EXPECT_FALSE(nonPieceReq->isHeaderPiece);
  EXPECT_FALSE(nonPieceReqCopy.isHeaderPiece);
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
  // header piece hits
  piecewiseCache->recordNewReq(*pieceReq2);
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
  // header piece misses
  EXPECT_FALSE(piecewiseCache->processReq(*pieceReq1, OpResultType::kGetMiss));
  EXPECT_EQ(pieceReq1->req.getOp(), OpType::kSet);
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
  // header piece hits
  piecewiseCache->recordNewReq(*pieceReq2);
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

} // namespace cachebench
} // namespace cachelib
} // namespace facebook

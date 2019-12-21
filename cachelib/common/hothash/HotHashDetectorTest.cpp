#include <cstdlib>
#include <random>

#include <folly/Random.h>
#include <gtest/gtest.h>

#include "cachelib/common/hothash/HotHashDetector.h"

using facebook::cachelib::HotHashDetector;

static void testHotHashes(uint32_t seed,
                          size_t numBuckets,
                          size_t numOfWarmItems,
                          size_t hotnessMultiplier,
                          size_t numHashes,
                          size_t numHotHashes,
                          uint32_t hotHashProbabilityMultiplier,
                          size_t batchSize,
                          size_t maxIterations) {
  std::mt19937 prng(seed);
  unsigned attempts = 0;
  static constexpr unsigned MAX_ATTEMPTS = 1; // Can relax if needed
  while (attempts < MAX_ATTEMPTS) {
    HotHashDetector detector(numBuckets, numOfWarmItems, hotnessMultiplier);
    std::vector<uint64_t> hashes(
        numHashes + numHotHashes * (hotHashProbabilityMultiplier - 1));
    for (size_t i = 0; i < numHashes; ++i) {
      hashes[i] = folly::Random::rand64(prng);
    }
    for (size_t i = 0; i < numHotHashes; ++i) {
      for (size_t j = 0; j < hotHashProbabilityMultiplier - 1; ++j) {
        hashes[numHashes + j * numHotHashes + i] = hashes[i];
      }
    }

    size_t iteration = 0;
    size_t hotCount = 0;
    while (hotCount != numHotHashes && iteration < maxIterations) {
      for (size_t i = 0; i + numHashes < batchSize; ++i) {
        uint32_t chosenIdx = folly::Random::rand32(0, hashes.size(), prng);
        detector.bumpHash(hashes[chosenIdx]);
      }
      hotCount = 0;
      for (size_t i = 0; i < numHashes; ++i) {
        hotCount += detector.bumpHash(hashes[i]) ? 1 : 0;
      }
      ++iteration;
    }
    ASSERT_NE(maxIterations, iteration);
    size_t errorCount = 0;
    uint32_t hotnessSum = 0;
    for (size_t i = 0; i < numHotHashes; ++i) {
      uint8_t hotness = detector.bumpHash(hashes[i]);
      errorCount += hotness ? 0 : 1;
      hotnessSum += hotness;
    }
    if (errorCount == 0) { // Can relax if needed
      ASSERT_LE(hotnessSum, numHotHashes * 2);
      break;
    }
    ++attempts;
  }
  ASSERT_NE(attempts, MAX_ATTEMPTS);
}

TEST(HotHashDetectorTest, Basic) {
  for (uint32_t seed = 1000; seed < 1020; ++seed) {
    // Passing parameters to testHotHashes:
    // seed, numBuckets, numOfWarmItems, hotnessMultiplier, numHashes,
    // numHotHashes, hotHashProbabilityMultiplier, batchSize, maxIterations
    testHotHashes(seed, 32, 8, 4, 100, 4, 50, 4000, 30);
    testHotHashes(seed, 128, 8, 6, 200, 5, 30, 12000, 50);
  }
}

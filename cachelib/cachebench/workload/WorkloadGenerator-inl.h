#include <algorithm>
#include <chrono>
#include <iostream>

namespace facebook {
namespace cachelib {
namespace cachebench {

template <typename Distribution>
WorkloadGenerator<Distribution>::WorkloadGenerator(const StressorConfig& config)
    : config_{config} {
  for (const auto& c : config.poolDistributions) {
    if (c.keySizeRange.size() != c.keySizeRangeProbability.size() + 1) {
      throw std::invalid_argument(
          "Key size range and their probabilities do not match up. Check your "
          "test config.");
    }
    workloadDist_.push_back(Distribution(c));
  }

  if (config_.numKeys > std::numeric_limits<uint32_t>::max()) {
    throw std::invalid_argument(folly::sformat(
        "Too many keys specified: {}. Maximum allowed is 4 Billion.",
        config_.numKeys));
  }

  generateReqs();
  generateKeyDistributions();
}

template <typename Distribution>
const Request& WorkloadGenerator<Distribution>::getReq(uint8_t poolId,
                                                       std::mt19937& gen) {
  XDCHECK_LT(poolId, keyIndicesForPool_.size());
  XDCHECK_LT(poolId, keyGenForPool_.size());
  size_t idx = keyIndicesForPool_[poolId][keyGenForPool_[poolId](gen)];
  return reqs_[idx];
}

template <typename Distribution>
OpType WorkloadGenerator<Distribution>::getOp(uint8_t pid, std::mt19937& gen) {
  return static_cast<OpType>(workloadDist_[workloadIdx(pid)].sampleOpDist(gen));
}

template <typename Distribution>
void WorkloadGenerator<Distribution>::generateKeys() {
  uint8_t pid = 0;
  auto fn = [pid, this](size_t start, size_t end) {
    // All keys are printable lower case english alphabet.
    std::uniform_int_distribution<char> charDis('a', 'z');
    std::mt19937 gen(folly::Random::rand32());
    for (uint64_t i = start; i < end; i++) {
      auto keySize = workloadDist_[pid].sampleKeySizeDist(gen);
      keys_[i].resize(keySize);
      for (auto& c : keys_[i]) {
        c = charDis(gen);
      }
    }
  };

  size_t totalKeys(0);
  std::chrono::seconds keyGenDuration(0);
  keys_.resize(config_.numKeys);
  for (size_t i = 0; i < config_.keyPoolDistribution.size(); i++) {
    pid = workloadIdx(i);
    size_t numKeysForPool =
        firstKeyIndexForPool_[i + 1] - firstKeyIndexForPool_[i];
    totalKeys += numKeysForPool;
    keyGenDuration +=
        detail::executeParallel(fn, numKeysForPool, firstKeyIndexForPool_[i]);
  }

  auto startTime = std::chrono::steady_clock::now();
  for (size_t i = 0; i < config_.keyPoolDistribution.size(); i++) {
    auto poolKeyBegin = keys_.begin() + firstKeyIndexForPool_[i];
    // past the end iterator
    auto poolKeyEnd = keys_.begin() + (firstKeyIndexForPool_[i + 1]);
    std::sort(poolKeyBegin, poolKeyEnd);
    auto newEnd = std::unique(poolKeyBegin, poolKeyEnd);
    // update pool key boundary before invalidating iterators
    for (size_t j = i + 1; j < firstKeyIndexForPool_.size(); j++) {
      firstKeyIndexForPool_[j] -= std::distance(newEnd, poolKeyEnd);
    }
    totalKeys -= std::distance(newEnd, poolKeyEnd);
    keys_.erase(newEnd, poolKeyEnd);
  }
  auto sortDuration = std::chrono::duration_cast<std::chrono::seconds>(
      std::chrono::steady_clock::now() - startTime);

  std::cout << folly::sformat("Created {:,} keys in {:.2f} mins",
                              totalKeys,
                              (keyGenDuration + sortDuration).count() / 60.)
            << std::endl;
}

template <typename Distribution>
void WorkloadGenerator<Distribution>::generateReqs() {
  generateFirstKeyIndexForPool();
  generateKeys();
  std::mt19937 gen(folly::Random::rand32());
  for (size_t i = 0; i < config_.keyPoolDistribution.size(); i++) {
    size_t idx = workloadIdx(i);
    for (size_t j = firstKeyIndexForPool_[i]; j < firstKeyIndexForPool_[i + 1];
         j++) {
      std::vector<size_t> chainSizes;
      chainSizes.push_back(workloadDist_[idx].sampleValDist(gen));
      int chainLen = workloadDist_[idx].sampleChainedLenDist(gen);
      for (int k = 0; k < chainLen; k++) {
        chainSizes.push_back(workloadDist_[idx].sampleChainedValDist(gen));
      }
      sizes_.emplace_back(chainSizes);
      auto reqSizes = sizes_.end() - 1;
      reqs_.emplace_back(keys_[j], reqSizes->begin(), reqSizes->end());
    }
  }
}

template <typename Distribution>
void WorkloadGenerator<Distribution>::generateFirstKeyIndexForPool() {
  auto sumProb = std::accumulate(config_.keyPoolDistribution.begin(),
                                 config_.keyPoolDistribution.end(), 0.);
  auto accumProb = 0.;
  firstKeyIndexForPool_.push_back(0);
  for (auto prob : config_.keyPoolDistribution) {
    accumProb += prob;
    firstKeyIndexForPool_.push_back(config_.numKeys * accumProb / sumProb);
  }
}

template <typename Distribution>
void WorkloadGenerator<Distribution>::generateKeyDistributions() {
  // We are trying to generate a gaussian distribution for each pool's part
  // in the overall cache ops. To keep the amount of memory finite, we only
  // generate a max of 4 billion op traces across all the pools and replay
  // the same when we need longer traces.
  std::chrono::seconds duration{0};
  for (uint64_t i = 0; i < config_.opPoolDistribution.size(); i++) {
    auto left = firstKeyIndexForPool_[i];
    auto right = firstKeyIndexForPool_[i + 1] - 1;
    size_t idx = workloadIdx(i);

    size_t numOpsForPool = std::min<size_t>(
        config_.numOps * config_.numThreads * config_.opPoolDistribution[i],
        std::numeric_limits<uint32_t>::max());
    std::cout << folly::sformat("Generating {:.2f}M sampled accesses",
                                numOpsForPool / 1e6)
              << std::endl;
    keyGenForPool_.push_back(std::uniform_int_distribution<uint32_t>(
        0, static_cast<uint32_t>(numOpsForPool) - 1));
    keyIndicesForPool_.push_back(std::vector<uint32_t>(numOpsForPool));

    duration += detail::executeParallel(
        [&, this](size_t start, size_t end) {
          std::mt19937 gen(folly::Random::rand32());
          auto popDist = workloadDist_[idx].getPopDist(left, right);
          for (uint64_t j = start; j < end; j++) {
            double idx;
            do {
              idx = std::round(popDist(gen));
            } while (idx < left || idx > right);
            keyIndicesForPool_[i][j] = idx;
          }
        },
        numOpsForPool);
  }

  std::cout << folly::sformat("Generated access patterns in {:.2f} mins",
                              duration.count() / 60.)
            << std::endl;
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook

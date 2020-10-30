#include <algorithm>
#include <chrono>
#include <iostream>

#include "cachelib/common/Utils.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

template <typename Distribution>
OnlineGenerator<Distribution>::OnlineGenerator(const StressorConfig& config)
    : config_{config},
      key_([]() { return new std::string(); }),
      req_([&]() { return new Request(*key_, dummy_.begin(), dummy_.end()); }) {
  for (const auto& c : config_.poolDistributions) {
    if (c.keySizeRange.size() != c.keySizeRangeProbability.size() + 1) {
      throw std::invalid_argument(
          "Key size range and their probabilities do not match up. Check "
          "your "
          "test config.");
    }
    workloadDist_.push_back(Distribution(c));
  }

  if (config_.numKeys > std::numeric_limits<uint64_t>::max()) {
    throw std::invalid_argument(
        folly::sformat("Too many keys specified: {}. Maximum allowed is 2**64.",
                       config_.numKeys));
  }
  generateFirstKeyIndexForPool();
  generateKeyLengths();
  generateSizes();
  generateKeyDistributions();
}

template <typename Distribution>
const Request& OnlineGenerator<Distribution>::getReq(uint8_t poolId,
                                                     std::mt19937_64& gen,
                                                     std::optional<uint64_t>) {
  size_t keyIdx = getKeyIdx(poolId, gen);

  generateKey(poolId, keyIdx, req_->key);
  auto sizes = generateSize(poolId, keyIdx);
  req_->sizeBegin = sizes->begin();
  req_->sizeEnd = sizes->end();
  auto op =
      static_cast<OpType>(workloadDist_[workloadIdx(poolId)].sampleOpDist(gen));
  req_->setOp(op);
  return *req_;
}

template <typename Distribution>
void OnlineGenerator<Distribution>::generateKeyLengths() {
  std::mt19937_64 gen(folly::Random::rand64());
  for (size_t i = 0; i < config_.keyPoolDistribution.size(); i++) {
    keyLengths_.emplace_back();
    for (size_t j = 0; j < kNumUniqueKeyLengths; j++) {
      // we generate keys uniquely identified by size_t bits.
      auto keySize =
          std::max(util::narrow_cast<size_t>(
                       workloadDist_[workloadIdx(i)].sampleKeySizeDist(gen)),
                   sizeof(size_t));
      keyLengths_.back().emplace_back(keySize);
    }
  }
}

template <typename Distribution>
void OnlineGenerator<Distribution>::generateKey(uint8_t pid,
                                                size_t idx,
                                                std::string& key) {
  // All keys are printable lower case english alphabet. we need to ensure the
  // key lengths are consistent for an idx.
  const auto keySize = keyLengths_[pid][idx % keyLengths_[pid].size()];
  XDCHECK_GE(keySize, sizeof(idx));
  key.resize(keySize);

  // write the idx into the key and pad any additional bytes with same bytes.
  auto* idxChars = reinterpret_cast<char*>(&idx);
  std::memcpy(key.data(), idxChars, sizeof(idx));
  // pad (deterministically)
  for (size_t i = sizeof(idx); i < keySize; i++) {
    key[i] = 'a';
  }
}

template <typename Distribution>
typename std::vector<std::vector<size_t>>::iterator
OnlineGenerator<Distribution>::generateSize(uint8_t pid, size_t idx) {
  return sizes_[workloadIdx(pid)].begin() +
         idx % sizes_[workloadIdx(pid)].size();
}

template <typename Distribution>
void OnlineGenerator<Distribution>::generateSizes() {
  std::mt19937_64 gen(folly::Random::rand64());
  // populate this per pool if there is a pool specific workload distribution.
  for (size_t i = 0; i < config_.keyPoolDistribution.size(); i++) {
    sizes_.emplace_back();
    size_t idx = workloadIdx(i);
    for (size_t j = 0; j < kNumUniqueSizes; j++) {
      std::vector<size_t> chainSizes;
      chainSizes.push_back(
          util::narrow_cast<size_t>(workloadDist_[idx].sampleValDist(gen)));
      int chainLen =
          util::narrow_cast<int>(workloadDist_[idx].sampleChainedLenDist(gen));
      for (int k = 0; k < chainLen; k++) {
        chainSizes.push_back(util::narrow_cast<size_t>(
            workloadDist_[idx].sampleChainedValDist(gen)));
      }
      sizes_[idx].emplace_back(chainSizes);
    }
  }
}

template <typename Distribution>
void OnlineGenerator<Distribution>::generateFirstKeyIndexForPool() {
  auto sumProb = std::accumulate(config_.keyPoolDistribution.begin(),
                                 config_.keyPoolDistribution.end(), 0.);
  auto accumProb = 0.;
  firstKeyIndexForPool_.push_back(0);
  for (auto prob : config_.keyPoolDistribution) {
    accumProb += prob;
    firstKeyIndexForPool_.push_back(
        util::narrow_cast<uint64_t>(config_.numKeys * accumProb / sumProb));
  }
}

template <typename Distribution>
uint64_t OnlineGenerator<Distribution>::getKeyIdx(uint8_t poolId,
                                                  std::mt19937_64& gen) {
  size_t left = firstKeyIndexForPool_[poolId];
  size_t right = firstKeyIndexForPool_[poolId + 1] - 1;
  uint64_t ret;
  do {
    ret =
        util::narrow_cast<uint64_t>(std::round(workloadPopDist_[poolId](gen)));
  } while (ret < left || ret > right);
  return ret;
}

template <typename Distribution>
void OnlineGenerator<Distribution>::generateKeyDistributions() {
  for (uint64_t i = 0; i < config_.opPoolDistribution.size(); i++) {
    auto left = firstKeyIndexForPool_[i];
    auto right = firstKeyIndexForPool_[i + 1] - 1;
    workloadPopDist_.push_back(
        workloadDist_[workloadIdx(i)].getPopDist(left, right));
  }
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook

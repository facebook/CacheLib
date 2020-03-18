#include <algorithm>
#include <chrono>
#include <iostream>

namespace facebook {
namespace cachelib {
namespace cachebench {

template <typename Distribution>
OnlineGenerator<Distribution>::OnlineGenerator(const StressorConfig& config)
    : config_{config} {
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
void OnlineGenerator<Distribution>::registerThread() {
  registryLock_.lock();
  auto tid = std::this_thread::get_id();
  keys_[tid] = std::string("");
  reqs_.emplace(tid,
                Request(keys_[tid], sizes_[0][0].begin(), sizes_[0][0].end()));
  registryLock_.unlock();
  while (true) {
    registryLock_.lock();
    if (reqs_.size() == config_.numThreads) {
      registryLock_.unlock();
      break;
    }
    registryLock_.unlock();
  }
}

template <typename Distribution>
const Request& OnlineGenerator<Distribution>::getReq(uint8_t poolId,
                                                     std::mt19937& gen,
                                                     std::optional<uint64_t>) {
  XDCHECK_LT(poolId, keyIndicesForPool_.size());
  XDCHECK_LT(poolId, keyGenForPool_.size());

  size_t idx = keyIndicesForPool_[poolId][keyGenForPool_[poolId](gen)];
  if (reqs_.count(std::this_thread::get_id()) == 0) {
    std::cout << "bad id " << std::this_thread::get_id() << std::endl;
  }

  Request& localReq(reqs_.at(std::this_thread::get_id()));
  generateKey(poolId, idx, localReq.key);
  auto sizes = generateSize(poolId, idx);
  localReq.sizeBegin = sizes->begin();
  localReq.sizeEnd = sizes->end();
  auto op =
      static_cast<OpType>(workloadDist_[workloadIdx(poolId)].sampleOpDist(gen));
  localReq.setOp(op);
  return localReq;
}

template <typename Distribution>
void OnlineGenerator<Distribution>::generateKeyLengths() {
  std::mt19937 gen(folly::Random::rand32());
  for (size_t i = 0; i < config_.keyPoolDistribution.size(); i++) {
    keyLengths_.emplace_back();
    for (int j = 0; j < (1 << 15); j++) {
      keyLengths_.back().emplace_back(
          workloadDist_[workloadIdx(i)].sampleKeySizeDist(gen));
    }
  }
}

template <typename Distribution>
void OnlineGenerator<Distribution>::generateKey(uint8_t pid,
                                                size_t idx,
                                                std::string& key) {
  key.clear();
  // All keys are printable lower case english alphabet.
  auto keySize = keyLengths_[pid][idx % keyLengths_[pid].size()];
  keySize = keySize < sizeof(idx) ? 0 : keySize - sizeof(idx);
  key.resize(keySize + sizeof(idx));
  // pack
  char* idxChars = reinterpret_cast<char*>(&idx);
  std::memcpy(key.data(), idxChars, sizeof(idx));
  // pad (deterministically)
  for (size_t i = sizeof(idx); i < keySize; i++) {
    key[i] = 'a';
  }
}

template <typename Distribution>
typename std::vector<std::vector<size_t>>::iterator
OnlineGenerator<Distribution>::generateSize(uint8_t pid, size_t idx) {
  return sizes_[pid].begin() + idx % sizes_[pid].size();
}

template <typename Distribution>
void OnlineGenerator<Distribution>::generateSizes() {
  std::mt19937 gen(folly::Random::rand32());
  for (size_t i = 0; i < config_.keyPoolDistribution.size(); i++) {
    size_t idx = workloadIdx(i);
    sizes_.emplace_back();
    for (size_t j = 0; j < (1 << 15); j++) {
      std::vector<size_t> chainSizes;
      chainSizes.push_back(workloadDist_[idx].sampleValDist(gen));
      int chainLen = workloadDist_[idx].sampleChainedLenDist(gen);
      for (int k = 0; k < chainLen; k++) {
        chainSizes.push_back(workloadDist_[idx].sampleChainedValDist(gen));
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
    firstKeyIndexForPool_.push_back(config_.numKeys * accumProb / sumProb);
  }
}

template <typename Distribution>
void OnlineGenerator<Distribution>::generateKeyDistributions() {
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
        config_.numThreads,
        numOpsForPool);
  }

  std::cout << folly::sformat("Generated access patterns in {:.2f} mins",
                              duration.count() / 60.)
            << std::endl;
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook

#pragma once
#define NUM_REQS 1 << 22

#include <atomic>

#include "cachelib/cachebench/cache/Cache.h"
#include "cachelib/cachebench/util/Exceptions.h"
#include "cachelib/cachebench/util/Parallel.h"
#include "cachelib/cachebench/util/Request.h"
#include "cachelib/cachebench/workload/ReplayGeneratorBase.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

class ReplayBufferedGenerator : public ReplayGeneratorBase {
 public:
  explicit ReplayBufferedGenerator(StressorConfig config)
      : ReplayGeneratorBase(config), config_(config) {
    startReader();
  }

  virtual ~ReplayBufferedGenerator() { endOfTrace_ = true; }

  void notifyResultWithReq(const Request& req, OpResultType /*result*/) {
    auto tid = std::this_thread::get_id();
    Request* doneReq = const_cast<Request*>(&req);
    auto complete = reqMap_[doneReq];
    if (--std::get<2>(*complete) == 0) {
      doneLock_[tid].lock();
      done_[tid].push_front(doneReq);
      doneLock_[tid].unlock();
    }
  }

  virtual void registerThread() {
    registryLock_.lock();
    auto tid = std::this_thread::get_id();
    threadIds_.push_back(tid);
    ready_.try_emplace(tid);
    readyLock_.try_emplace(tid);
    readyReady_.try_emplace(tid);
    done_.try_emplace(tid);
    doneLock_.try_emplace(tid);
    registryLock_.unlock();
    while (true) {
      registryLock_.lock();
      if (threadIds_.size() == config_.numThreads) {
        registryLock_.unlock();
        break;
      }
      registryLock_.unlock();
    }
  }

  // getReq generates the next request from the named trace file.
  // it expects a comma separated file (possibly with a header)
  // which consists of the fields:
  // fbid,OpType,size,repeats
  //
  // Here, repeats gives a number of times to repeat the request specified on
  // this line before reading the next line of the file.
  const Request& getReq(
      uint8_t,
      std::mt19937_64&,
      std::optional<uint64_t> lastRequestId = std::nullopt) override;

  void startReader();
  void readReqs();

 private:
  typedef std::tuple<std::string, std::vector<size_t>, uint32_t> ReqDataT;
  ReqDataT reqData_[NUM_REQS];
  std::vector<Request> reqs_;
  std::unordered_map<Request*, ReqDataT*> reqMap_;
  std::unordered_map<std::thread::id, std::deque<Request*>> done_;
  std::unordered_map<std::thread::id, std::mutex> doneLock_;

  std::unordered_map<std::thread::id, std::deque<Request*>> ready_;
  std::unordered_map<std::thread::id, std::mutex> readyLock_;
  std::unordered_map<std::thread::id, std::condition_variable> readyReady_;
  std::vector<std::thread::id> threadIds_;

  std::mutex registryLock_;
  const StressorConfig config_;
  std::atomic_bool endOfTrace_{false};

  static thread_local std::istringstream ss_;
  static thread_local std::string key_;
  static thread_local std::string token_;
};

void ReplayBufferedGenerator::readReqs() {
  std::string token;
  int32_t repeats(1);
  std::string line;
  while (true) {
    registryLock_.lock();
    if (threadIds_.size() == config_.numThreads) {
      break;
    }
    registryLock_.unlock();
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  for (int i = 0; i < NUM_REQS; i++) {
    std::get<1>(reqData_[i]).push_back(0);
    reqs_.emplace_back(std::get<0>(reqData_[i]),
                       std::get<1>(reqData_[i]).begin(),
                       std::get<1>(reqData_[i]).end(),
                       i);
  }
  for (int i = 0; i < NUM_REQS; i++) {
    reqMap_[&(reqs_[i])] = &(reqData_[i]);
  }
  auto tid = threadIds_.begin();
  for (auto it = reqs_.begin(); it != reqs_.end(); it++) {
    doneLock_[*tid].lock();
    done_[*tid].push_back(&(*it));
    doneLock_[*tid].unlock();
    tid++;
    if (tid == threadIds_.end()) {
      tid = threadIds_.begin();
    }
  }

  std::vector<Request*> doneBuffer;
  while (!endOfTrace_) {
    if (!doneLock_[*tid].try_lock()) {
      tid++;
      if (tid == threadIds_.end()) {
        tid = threadIds_.begin();
      }
      continue;
    }
    if (done_[*tid].empty()) {
      doneLock_[*tid].unlock();
      tid++;
      if (tid == threadIds_.end()) {
        tid = threadIds_.begin();
      }
      continue;
    }
    doneBuffer.clear();
    doneBuffer.insert(
        doneBuffer.begin(), done_[*tid].begin(), done_[*tid].end());
    done_[*tid].clear();
    doneLock_[*tid].unlock();
    while (!readyLock_[*tid].try_lock()) {
      tid++;
      if (tid == threadIds_.end()) {
        tid = threadIds_.begin();
      }
    }
    std::mutex& readyLock(readyLock_[*tid]);
    for (auto next : doneBuffer) {
      ReqDataT* nextData = reqMap_[next];
      if (!std::getline(infile_, next->key)) {
        repeats = 1;
        endOfTrace_ = true;
        std::cout << "trace done!" << std::endl;
        break;
      }
      std::get<2>(*nextData) = -1;
      ready_[*tid].push_front(next);
    }
    readyLock.unlock();
    tid++;
    if (tid == threadIds_.end()) {
      tid = threadIds_.begin();
    }
  }
}

const Request& ReplayBufferedGenerator::getReq(uint8_t,
                                               std::mt19937_64&,
                                               std::optional<uint64_t>) {
  auto tid = std::this_thread::get_id();
  while (true) {
    readyLock_[tid].lock();
    if (!ready_[tid].empty()) {
      break;
    }
    readyLock_[tid].unlock();
    if (endOfTrace_) {
      throw cachelib::cachebench::EndOfTrace("");
    }
    std::this_thread::sleep_for(std::chrono::microseconds(10));
  }
  Request* next = ready_[tid].back();
  ReqDataT* nextData = reqMap_[next];
  if (std::get<2>(*nextData) == static_cast<uint32_t>(-1)) {
    ss_.str(next->key);
    ss_.clear();
    std::getline(ss_, key_, ',');
    std::getline(ss_, token_, ',');
    try {
      *(next->sizeBegin) = std::stoi(token_);
      std::getline(ss_, token_);
      std::get<2>(*nextData) = std::stoi(token_);
    } catch (const std::invalid_argument& e) {
      std::cout << "line " << std::get<0>(*nextData) << std::endl;
      std::cout << "token " << token_ << std::endl;
      throw;
    }
    next->key = key_;
  }
  if (std::get<2>(*nextData) == 1) {
    ready_[tid].pop_back();
  }
  readyLock_[tid].unlock();
  return *next;
}

void ReplayBufferedGenerator::startReader() {
  auto reader = std::thread([this] { readReqs(); });
  reader.detach();
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook

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

#include "cachelib/navy/common/AsyncDevice.h"

#include <folly/Function.h>
#include <folly/ThreadLocal.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/experimental/io/AsyncIO.h>
#include <folly/experimental/io/IoUring.h>
#include <folly/fibers/TimedMutex.h>
#include <folly/futures/Future.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/EventBaseManager.h>
#include <folly/io/async/EventHandler.h>

#include <chrono>
#include <fstream>
#include <unordered_map>

#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/NavyThread.h"
#include "cachelib/navy/common/Utils.h"
#include "common/time/Time.h"

using folly::EventBaseManager;

namespace facebook {
namespace cachelib {
namespace navy {

using folly::fibers::TimedMutex;

// Forward declarations
struct IoContext;

// IO timeout in milliseconds (1s)
static constexpr size_t kIOTimeoutMs = 1000;

// Data structure to hold the info about an IO operation.
struct AsyncRequest {
  enum { INVALID = 0, READ, WRITE };

  explicit AsyncRequest(folly::AsyncBaseOp* op) : op_(op) {}

  const char* getOpName() {
    switch (opType_) {
    case READ:
      return "read";
    case WRITE:
      return "write";
    default:
      XDCHECK(false);
    }
    return "unknown";
  }

  void waitCompletion();

  void notifyResult(bool result);

  std::string toString() {
    return fmt::format("{} offset {} size {} resubmitted {} result {}",
                       getOpName(), offset_, size_, resubmitted_, result_);
  }

  void done();

  uint8_t opType_ = INVALID;
  uint64_t offset_ = 0;
  uint32_t size_ = 0;
  uint8_t resubmitted_ = 0;
  union {
    void* readData_;
    const void* writeData_;
  };
  bool result_ = false;

  IoContext* context_ = nullptr;
  folly::fibers::Baton baton_;
  std::unique_ptr<folly::AsyncBaseOp> op_;

  std::chrono::nanoseconds startTime_;
  std::chrono::nanoseconds submitTime_;
  std::chrono::nanoseconds compTime_;
};

// Async IO handler to handle the events happened on IO_Uring.
class CompletionHandler : public folly::EventHandler {
 public:
  CompletionHandler(IoContext& ioContext, folly::EventBase* evb, int pollFd)
      : folly::EventHandler(evb, folly::NetworkSocket::fromFd(pollFd)),
        ioContext_(ioContext) {
    registerHandler(EventHandler::READ | EventHandler::PERSIST);
  }

  ~CompletionHandler() override { unregisterHandler(); }

  void handlerReady(uint16_t /*events*/) noexcept override;

 private:
  IoContext& ioContext_;
};

// A single block device can be accessed from multiple threads, so we keep a
// per-thread context.
struct IoContext {
 public:
  IoContext(std::unique_ptr<folly::AsyncBase>&& asyncBase,
            size_t id,
            int fd,
            folly::EventBase* evb,
            size_t capacity)
      : asyncBase_(std::move(asyncBase)),
        id_(id),
        fd_(fd),
        qDepth_(capacity),
        compHandler_(*this, evb, asyncBase_->pollFd()) {}
  virtual ~IoContext() {}

  std::string getName() { return fmt::format("ctx {}", id_); }
  // Invoked by event loop handler whenever AIO signals that one or more
  // operation have finished.
  void pollCompletion();
  void submitReq(AsyncRequest* req);

 private:
  static constexpr size_t kQueueTimeOutMs = 2000;
  // The maximum number of retries when IO failed with EBUSY. 5 is arbitrary
  static constexpr size_t kRetryLimit = 5;

  struct Waiter {
    folly::fibers::Baton baton_;
    folly::SafeIntrusiveListHook hook_;
  };

  using WaiterList = folly::SafeIntrusiveList<Waiter, &Waiter::hook_>;

  std::unique_ptr<folly::AsyncBase> asyncBase_;
  const size_t id_;
  const int fd_;
  const size_t qDepth_;
  CompletionHandler compHandler_;

  // The IO operations that have been submit but not completed yet.
  size_t numOutstanding_ = 0;
  size_t numSubmitted_ = 0;
  size_t numCompleted_ = 0;
  WaiterList waitList_;
};

// An AsyncDevice manages direct, asynchronous I/O to either a
// block device or a file.
class AsyncDevice : public Device {
 public:
  AsyncDevice(folly::File file,
              uint64_t size,
              uint32_t blockSize,
              uint32_t numThreads,
              uint32_t qDepthPerThread,
              std::shared_ptr<DeviceEncryptor> encryptor,
              uint32_t maxDeviceWriteSize,
              bool useIoUring);
  AsyncDevice(const AsyncDevice&) = delete;
  AsyncDevice& operator=(const AsyncDevice&) = delete;

  void getCounters(const CounterVisitor& visitor) const override;

  std::shared_ptr<AsyncRequest> getRequest();

 private:
  IoContext* getTLContext();

  NavyThread& getNextWorker() { return *workers_[(numOps_++) % numThreads_]; }

  bool writeImpl(uint64_t, uint32_t, const void*) override;

  bool readImpl(uint64_t, uint32_t, void*) override;

  void flushImpl() override { ::fsync(file_.fd()); }

  // writeSync and readSync are for legacy path running IOs not from
  // the EventBase thread. E.g., persistence/recovery without IO threads
  bool writeSync(uint64_t offset, uint32_t size, const void* value) {
    ssize_t bytesWritten = ::pwrite(file_.fd(), value, size, offset);
    if (bytesWritten != size) {
      reportIOError("write", offset, size, bytesWritten);
    }
    syncWrites_.inc();
    return bytesWritten == size;
  }

  bool readSync(uint64_t offset, uint32_t size, void* value) {
    ssize_t bytesRead = ::pread(file_.fd(), value, size, offset);
    if (bytesRead != size) {
      reportIOError("read", offset, size, bytesRead);
    }
    syncReads_.inc();
    return bytesRead == size;
  }

  // return completed sync or not
  bool submitReq(std::shared_ptr<AsyncRequest> req);

  const folly::File file_{};
  uint32_t numThreads_;
  const uint32_t blockSize_;

  // The max number of outstanding requests per IO thread. This is used to
  // create the queue depth of an IO_Uring.
  const uint32_t qDepthPerThread_;

  const bool useIoUring_;

  // Atomic index used to assign unique context ID
  std::atomic<uint32_t> incrementalIdx_{0};
  // Thread-local context, created on demand
  folly::ThreadLocalPtr<IoContext> tlContext_;
  // Keep list of contexts pointer for debugging
  TimedMutex mutex_;
  std::vector<IoContext*> uringContexts_;
  // NavyThread pool used for submitting IO requests and handling IO
  // completion events.
  std::vector<std::unique_ptr<NavyThread>> workers_;
  std::atomic<size_t> numOps_{0};

  std::atomic<size_t> numOutstanding_{0};

  mutable AtomicCounter syncReads_;
  mutable AtomicCounter syncWrites_;
};

/*
 * AsyncRequest
 */

void AsyncRequest::waitCompletion() {
  baton_.wait();

  auto curTime = getSteadyClock();
  auto delayMs = toMillis(curTime - startTime_).count();
  if (delayMs > static_cast<int64_t>(kIOTimeoutMs)) {
    XLOG_N_PER_MS(ERR, 10, 1000) << fmt::format(
        "[{}] IO {} timeout {}ms (submit +{}ms comp +{}ms notify +{}ms)",
        context_ ? context_->getName() : "NA", toString(), delayMs,
        toMillis(submitTime_ - startTime_).count(),
        toMillis(compTime_ - submitTime_).count(),
        toMillis(curTime - compTime_).count());
  }
}

void AsyncRequest::notifyResult(bool result) {
  result_ = result;
  baton_.post();
}

void AsyncRequest::done() {
  XDCHECK_EQ(op_->state(), folly::AsyncIOOp::State::COMPLETED);
  XDCHECK(opType_ == READ || opType_ == WRITE);

  if (op_->result() == size_) {
    notifyResult(true);
  } else {
    reportIOError(getOpName(), offset_, size_, op_->result());
    notifyResult(false);
  }
}

/*
 * CompletionHandler
 */
void CompletionHandler::handlerReady(uint16_t /*events*/) noexcept {
  ioContext_.pollCompletion();
}

/*
 * IoContext
 */

void IoContext::pollCompletion() {
  auto completed = asyncBase_->pollCompleted();
  for (auto& op : completed) {
    auto ioReq = reinterpret_cast<AsyncRequest*>(op->getUserData());
    XDCHECK(ioReq);

    XDCHECK_GE(numOutstanding_, 0u);
    numOutstanding_--;
    numCompleted_++;

    // handle retry
    if (ioReq->op_->result() == -EAGAIN && ioReq->resubmitted_ < kRetryLimit) {
      ioReq->resubmitted_++;
      XLOG_EVERY_N_THREAD(ERR, 1000) << fmt::format(
          "[{}] resubmitting IO {}", getName(), ioReq->toString());
      ioReq->op_->reset();
      submitReq(ioReq);
    } else {
      if (ioReq->resubmitted_ > 0) {
        XLOG_EVERY_N_THREAD(ERR, 1000)
            << fmt::format("[{}] resubmitted IO completed ({}) {}", getName(),
                           ioReq->op_->result(), ioReq->toString());
      }
      ioReq->compTime_ = getSteadyClock();
      ioReq->done();
    }

    if (!waitList_.empty()) {
      auto& waiter = waitList_.front();
      waitList_.pop_front();
      waiter.baton_.post();
    }
  }
}

void IoContext::submitReq(AsyncRequest* req) {
  req->context_ = this;

  while (numOutstanding_ >= qDepth_) {
    XLOG_EVERY_MS(ERR, 1000) << fmt::format(
        "[{}] the number of outstanding requests {} exceeds the limit {}",
        getName(), numOutstanding_, qDepth_);
    Waiter waiter;
    waitList_.push_back(waiter);
    waiter.baton_.wait();
  }

  req->startTime_ = getSteadyClock();

  if (req->opType_ == AsyncRequest::READ) {
    req->op_->setUserData(req);
    req->op_->pread(fd_, req->readData_, req->size_, req->offset_);
  } else {
    XDCHECK_EQ(req->opType_, AsyncRequest::WRITE);
    req->op_->setUserData(req);
    req->op_->pwrite(fd_, req->writeData_, req->size_, req->offset_);
  }

  asyncBase_->submit(req->op_.get());
  req->submitTime_ = getSteadyClock();
  numOutstanding_++;
  numSubmitted_++;
}

/*
 * AsyncDevice
 */
AsyncDevice::AsyncDevice(folly::File file,
                         uint64_t size,
                         uint32_t blockSize,
                         uint32_t numThreads,
                         uint32_t qDepthPerThread,
                         std::shared_ptr<DeviceEncryptor> encryptor,
                         uint32_t maxDeviceWriteSize,
                         bool useIoUring)
    : Device(size, std::move(encryptor), blockSize, maxDeviceWriteSize),
      file_(std::move(file)),
      numThreads_(numThreads),
      blockSize_(blockSize),
      qDepthPerThread_(qDepthPerThread),
      useIoUring_(useIoUring) {
  XLOGF(INFO,
        "Creating async device with block size {} "
        "max outstanding requests {} number of threads {} io engine {}",
        blockSize_, qDepthPerThread_, numThreads,
        useIoUring_ ? "io_uring" : "libaio");
  XDCHECK_GT(blockSize_, 0u);
  XDCHECK_GT(qDepthPerThread_, 0u);

  for (uint32_t i = 0; i < numThreads_; i++) {
    workers_.emplace_back(
        make_unique<NavyThread>(fmt::format("asyncdevice_{}", i)));
  }
}

void AsyncDevice::getCounters(const CounterVisitor& visitor) const {
  Device::getCounters(visitor);

  visitor("navy_async_device_sync_reads", syncReads_.get(),
          CounterVisitor::CounterType::COUNT);
  visitor("navy_async_device_sync_writes", syncWrites_.get(),
          CounterVisitor::CounterType::COUNT);
}

std::shared_ptr<AsyncRequest> AsyncDevice::getRequest() {
  if (useIoUring_) {
    return std::make_shared<AsyncRequest>(new folly::IoUringOp());
  }

  return std::make_shared<AsyncRequest>(new folly::AsyncIOOp());
}

bool AsyncDevice::readImpl(uint64_t offset, uint32_t length, void* value) {
  auto req = getRequest();

  req->opType_ = AsyncRequest::READ;
  req->offset_ = offset;
  req->size_ = length;
  req->readData_ = value;

  if (!submitReq(req)) {
    req->waitCompletion();
  }
  return req->result_;
}

bool AsyncDevice::writeImpl(uint64_t offset, uint32_t size, const void* value) {
  auto req = getRequest();
  req->opType_ = AsyncRequest::WRITE;
  req->offset_ = offset;
  req->size_ = size;
  req->writeData_ = value;

  if (!submitReq(req)) {
    req->waitCompletion();
  }
  return req->result_;
}

bool AsyncDevice::submitReq(std::shared_ptr<AsyncRequest> req) {
  if (numThreads_ > 0) {
    // delegate to the IO thread pool
    getNextWorker().addTaskRemote([this, req]() mutable {
      auto ctx = getTLContext();
      XDCHECK(ctx);
      ctx->submitReq(req.get());
    });
    return false;
  }

  // Try to get the IoContext if we are on the event base thread
  // (e.g., NavyThread)
  auto ctx = getTLContext();
  if (ctx) {
    ctx->submitReq(req.get());
    return false;
  }

  // Fallback to sync for legacy path (e.g., IOs for persistence/recovery
  // from the main thread)
  if (req->opType_ == AsyncRequest::READ) {
    req->result_ = readSync(req->offset_, req->size_, req->readData_);
  } else {
    XDCHECK_EQ(req->opType_, AsyncRequest::WRITE);
    req->result_ = writeSync(req->offset_, req->size_, req->writeData_);
  }
  return true;
}

IoContext* AsyncDevice::getTLContext() {
  if (!tlContext_) {
    // Create new context if on event base thread
    auto evb = EventBaseManager::get()->getExistingEventBase();
    if (!evb) {
      return nullptr;
    }
    auto idx = incrementalIdx_++;

    std::unique_ptr<folly::AsyncBase> asyncBase;
    if (useIoUring_) {
      asyncBase = make_unique<folly::IoUring>(
          qDepthPerThread_, folly::IoUring::PollMode::POLLABLE,
          qDepthPerThread_);
    } else {
      asyncBase = std::make_unique<folly::AsyncIO>(
          qDepthPerThread_, folly::IoUring::PollMode::POLLABLE);
    }

    tlContext_.reset(new IoContext(std::move(asyncBase), idx, file_.fd(), evb,
                                   qDepthPerThread_));

    {
      std::lock_guard<TimedMutex> lock{mutex_};
      uringContexts_.resize(idx + 1);
      uringContexts_[idx] = tlContext_.get();
    }
  }

  return tlContext_.get();
}

std::unique_ptr<Device> createAsyncIoFileDevice(
    folly::File f,
    uint64_t size,
    uint32_t blockSize,
    uint32_t numIoThreads,
    uint32_t qDepthPerThread,
    std::shared_ptr<DeviceEncryptor> encryptor,
    uint32_t maxDeviceWriteSize,
    bool enableIoUring) {
  XDCHECK(folly::isPowTwo(blockSize));
  return std::make_unique<AsyncDevice>(std::move(f),
                                       size,
                                       blockSize,
                                       numIoThreads,
                                       qDepthPerThread,
                                       encryptor,
                                       maxDeviceWriteSize,
                                       enableIoUring);
}

} // namespace navy
} // namespace cachelib
} // namespace facebook

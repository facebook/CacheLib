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

#include "cachelib/navy/common/Device.h"

#include <folly/File.h>
#include <folly/Format.h>
#include <folly/Function.h>
#include <folly/ThreadLocal.h>
#include <folly/experimental/io/AsyncIO.h>
#include <folly/experimental/io/IoUring.h>
#include <folly/fibers/TimedMutex.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/EventBaseManager.h>
#include <folly/io/async/EventHandler.h>

#include <chrono>
#include <cstring>
#include <numeric>

#include "cachelib/navy/common/FdpNvme.h"
#include "cachelib/navy/common/Utils.h"

namespace facebook::cachelib::navy {

namespace {
using IOOperation =
    std::function<ssize_t(int fd, void* buf, size_t count, off_t offset)>;

// Forward declarations
struct IOReq;
class IoContext;
class AsyncIoContext;
class FileDevice;

// IO timeout in milliseconds (1s)
static constexpr size_t kIOTimeoutMs = 1000;

// IO Operation type supported by IOReq
enum OpType : uint8_t { INVALID = 0, READ, WRITE };

struct IOOp {
  explicit IOOp(IOReq& parent,
                int idx,
                int fd,
                uint64_t offset,
                uint32_t size,
                void* data,
                std::optional<int> placeHandle = std::nullopt)
      : parent_(parent),
        idx_(idx),
        fd_(fd),
        offset_(offset),
        size_(size),
        data_(data),
        placeHandle_(placeHandle) {}

  std::string toString() const;

  bool done(ssize_t len);

  IOReq& parent_;
  // idx_ is the index of this op in the request
  const uint32_t idx_;

  // Params for read/write
  const int fd_;
  const uint64_t offset_ = 0;
  const uint32_t size_ = 0;
  void* const data_;
  std::optional<int> placeHandle_;

  // The number of resubmission on EAGAIN error
  uint8_t resubmitted_ = 0;

  // Time when the processing of this op started
  std::chrono::nanoseconds startTime_;
  // Time when the op has been submitted
  std::chrono::nanoseconds submitTime_;
};

// Data structure to hold the info about an IO request.
// Each IO request can be split into multiple IOOp for RAID0
struct IOReq {
  explicit IOReq(IoContext& context_,
                 const std::vector<folly::File>& fvec,
                 uint32_t stripeSize,
                 OpType opType,
                 uint64_t offset,
                 uint32_t size,
                 void* data,
                 std::optional<int> placeHandle = std::nullopt);

  const char* getOpName() const {
    switch (opType_) {
    case OpType::READ:
      return "read";
    case OpType::WRITE:
      return "write";
    default:
      XDCHECK(false);
    }
    return "unknown";
  }

  // Used to wait for completion of all pending IOOp
  bool waitCompletion();

  // Notify the result of individual operation completion
  void notifyOpResult(bool result);

  std::string toString() const {
    return fmt::format(
        "[req {}] {} offset {} size {} data {} ops {} remaining {} result {}",
        reinterpret_cast<const void*>(this), getOpName(), offset_, size_, data_,
        ops_.size(), numRemaining_, result_);
  }

  IoContext& context_;
  const OpType opType_ = OpType::INVALID;
  const uint64_t offset_ = 0;
  const uint32_t size_ = 0;
  void* const data_;
  std::optional<int> placeHandle_;

  // Aggregate result of operations
  bool result_ = true;

  uint32_t numRemaining_ = 0;
  std::vector<IOOp> ops_;
  // Baton is used to wait for the completion of the entire request
  folly::fibers::Baton baton_;

  // Time when the processing of this req started
  std::chrono::nanoseconds startTime_;
  // Time when all of ops have been completed from device.
  // This could be differnt from the completion time of individual ops
  // for RAID device
  std::chrono::nanoseconds compTime_;
};

class IoContext {
 public:
  IoContext() = default;
  virtual ~IoContext() = default;

  virtual std::string getName() = 0;
  // Return true if IO submitted via submitIo can complete async
  virtual bool isAsyncIoCompletion() = 0;

  // Create and submit read req
  std::shared_ptr<IOReq> submitRead(const std::vector<folly::File>& fvec,
                                    uint32_t stripeSize,
                                    uint64_t offset,
                                    uint32_t size,
                                    void* data);

  // Create and submit write req
  std::shared_ptr<IOReq> submitWrite(const std::vector<folly::File>& fvec,
                                     uint32_t stripeSize,
                                     uint64_t offset,
                                     uint32_t size,
                                     const void* data,
                                     int placeHandle);

  // Submit a IOOp to the device; should not fail for AsyncIoContext
  virtual bool submitIo(IOOp& op) = 0;

 protected:
  void submitReq(std::shared_ptr<IOReq> req);
};

class SyncIoContext : public IoContext {
 public:
  SyncIoContext() = default;

  std::string getName() override { return "sync"; }
  bool isAsyncIoCompletion() override { return false; }

  bool submitIo(IOOp& op) override;

 private:
  ssize_t writeSync(int fd, uint64_t offset, uint32_t size, const void* value);
  ssize_t readSync(int fd, uint64_t offset, uint32_t size, void* value);
};

// Async IO handler to handle the events happened on poll fd used by AsyncBase
// (common for both IoUring and AsyncIO)
class CompletionHandler : public folly::EventHandler {
 public:
  CompletionHandler(AsyncIoContext& ioContext,
                    folly::EventBase* evb,
                    int pollFd)
      : folly::EventHandler(evb, folly::NetworkSocket::fromFd(pollFd)),
        ioContext_(ioContext) {
    registerHandler(EventHandler::READ | EventHandler::PERSIST);
  }

  ~CompletionHandler() override { unregisterHandler(); }

  void handlerReady(uint16_t /*events*/) noexcept override;

 private:
  AsyncIoContext& ioContext_;
};

// Per-thread context for AsyncIO like libaio or io_uring
class AsyncIoContext : public IoContext {
 public:
  AsyncIoContext(std::unique_ptr<folly::AsyncBase>&& asyncBase,
                 size_t id,
                 folly::EventBase* evb,
                 size_t capacity,
                 bool useIoUring,
                 std::vector<std::shared_ptr<FdpNvme>> fdpNvmeVec);

  ~AsyncIoContext() override = default;

  std::string getName() override { return fmt::format("ctx_{}", id_); }
  // IO is completed sync if compHandler_ is not available
  bool isAsyncIoCompletion() override { return !!compHandler_; }

  bool submitIo(IOOp& op) override;

  // Invoked by event loop handler whenever AIO signals that one or more
  // operation have finished
  void pollCompletion();

 private:
  void handleCompletion(folly::Range<folly::AsyncBaseOp**>& completed);

  std::unique_ptr<folly::AsyncBaseOp> prepAsyncIo(IOOp& op);

  // Prepare an Nvme CMD IO through IOUring
  std::unique_ptr<folly::AsyncBaseOp> prepNvmeIo(IOOp& op);

  // The maximum number of retries when IO failed with EBUSY. 5 is arbitrary
  static constexpr size_t kRetryLimit = 5;

  // Waiter context to enforce the qdepth limit
  struct Waiter {
    folly::fibers::Baton baton_;
    folly::SafeIntrusiveListHook hook_;
  };

  using WaiterList = folly::SafeIntrusiveList<Waiter, &Waiter::hook_>;

  std::unique_ptr<folly::AsyncBase> asyncBase_;
  // Sequential id assigned to this context
  const size_t id_;
  const size_t qDepth_;
  // Waiter list for enforcing the qdepth
  WaiterList waitList_;
  std::unique_ptr<CompletionHandler> compHandler_;
  // Use io_uring or libaio
  bool useIoUring_;
  size_t retryLimit_ = kRetryLimit;

  // The IO operations that have been submit but not completed yet.
  size_t numOutstanding_ = 0;
  size_t numSubmitted_ = 0;
  size_t numCompleted_ = 0;

  // Device info vector for FDP support
  const std::vector<std::shared_ptr<FdpNvme>> fdpNvmeVec_{};
  // As of now, only one FDP enabled Device is supported
  static constexpr uint16_t kDefaultFdpIdx = 0u;
};

// An FileDevice manages direct I/O to either a single or multiple (RAID0)
// block device(s) or regular file(s).
class FileDevice : public Device {
 public:
  FileDevice(std::vector<folly::File>&& fvec,
             std::vector<std::shared_ptr<FdpNvme>>&& fdpNvmeVec,
             uint64_t size,
             uint32_t blockSize,
             uint32_t stripeSize,
             uint32_t maxIOSize,
             uint32_t maxDeviceWriteSize,
             IoEngine ioEngine,
             uint32_t qDepthPerContext,
             std::shared_ptr<DeviceEncryptor> encryptor);

  FileDevice(const FileDevice&) = delete;
  FileDevice& operator=(const FileDevice&) = delete;

 private:
  IoContext* getIoContext();

  bool writeImpl(uint64_t, uint32_t, const void*, int) override;

  bool readImpl(uint64_t, uint32_t, void*) override;

  void flushImpl() override;

  int allocatePlacementHandle() override;

  // File vector for devices or regular files
  const std::vector<folly::File> fvec_{};

  // Device info vector for FDP support
  const std::vector<std::shared_ptr<FdpNvme>> fdpNvmeVec_{};

  // RAID stripe size when multiple devices are used
  const uint32_t stripeSize_;

  // SyncIoContext is the IoContext used when async IO is not enabled.
  std::unique_ptr<SyncIoContext> syncIoContext_;

  // Atomic index used to assign unique context ID
  std::atomic<uint32_t> incrementalIdx_{0};
  // Thread-local context, created on demand
  folly::ThreadLocalPtr<AsyncIoContext> tlContext_;
  // Keep list of contexts pointer for gdb debugging
  folly::fibers::TimedMutex dbgAsyncIoContextsMutex_;
  std::vector<AsyncIoContext*> dbgAsyncIoContexts_;

  // io engine to be used
  const IoEngine ioEngine_;
  // The max number of outstanding requests per IO context. This is used to
  // determine the capacity of an io_uring/libaio queue
  const uint32_t qDepthPerContext_;

  AtomicCounter numProcessed_{0};

  friend class IoContext;
};

// Device on memory buffer
class MemoryDevice final : public Device {
 public:
  explicit MemoryDevice(uint64_t size,
                        std::shared_ptr<DeviceEncryptor> encryptor,
                        uint32_t ioAlignSize)
      : Device{size, std::move(encryptor), ioAlignSize, 0 /* max IO size */,
               0 /* max device write size */},
        buffer_{std::make_unique<uint8_t[]>(size)} {}
  MemoryDevice(const MemoryDevice&) = delete;
  MemoryDevice& operator=(const MemoryDevice&) = delete;
  ~MemoryDevice() override = default;

 private:
  bool writeImpl(uint64_t offset,
                 uint32_t size,
                 const void* value,
                 int /* unused */) noexcept override {
    XDCHECK_LE(offset + size, getSize());
    std::memcpy(buffer_.get() + offset, value, size);
    return true;
  }

  bool readImpl(uint64_t offset, uint32_t size, void* value) override {
    XDCHECK_LE(offset + size, getSize());
    std::memcpy(value, buffer_.get() + offset, size);
    return true;
  }

  int allocatePlacementHandle() override { return -1; }

  void flushImpl() override {
    // Noop
  }

  std::unique_ptr<uint8_t[]> buffer_;
};
} // namespace

bool Device::write(uint64_t offset, BufferView view, int placeHandle) {
  if (encryptor_) {
    auto writeBuffer = makeIOBuffer(view.size());
    writeBuffer.copyFrom(0, view);
    return write(offset, std::move(writeBuffer), placeHandle);
  }

  const auto size = view.size();
  XDCHECK_LE(offset + size, size_);
  const uint8_t* data = reinterpret_cast<const uint8_t*>(view.data());
  return writeInternal(offset, data, size, placeHandle);
}

bool Device::write(uint64_t offset, Buffer buffer, int placeHandle) {
  const auto size = buffer.size();
  XDCHECK_LE(offset + buffer.size(), size_);
  uint8_t* data = reinterpret_cast<uint8_t*>(buffer.data());
  XDCHECK_EQ(reinterpret_cast<uint64_t>(data) % ioAlignmentSize_, 0ul);
  if (encryptor_) {
    XCHECK_EQ(offset % encryptor_->encryptionBlockSize(), 0ul);
    auto res = encryptor_->encrypt(folly::MutableByteRange{data, size}, offset);
    if (!res) {
      encryptionErrors_.inc();
      return false;
    }
  }
  return writeInternal(offset, data, size, placeHandle);
}

bool Device::writeInternal(uint64_t offset,
                           const uint8_t* data,
                           size_t size,
                           int placeHandle) {
  auto remainingSize = size;
  auto maxWriteSize = (maxWriteSize_ == 0) ? remainingSize : maxWriteSize_;
  bool result = true;
  while (remainingSize > 0) {
    auto writeSize = std::min<size_t>(maxWriteSize, remainingSize);
    XDCHECK_EQ(offset % ioAlignmentSize_, 0ul);
    XDCHECK_EQ(writeSize % ioAlignmentSize_, 0ul);

    auto timeBegin = getSteadyClock();
    result = writeImpl(offset, writeSize, data, placeHandle);
    writeLatencyEstimator_.trackValue(
        toMicros((getSteadyClock() - timeBegin)).count());

    if (result) {
      bytesWritten_.add(writeSize);
    } else {
      // One part of the write failed so we abort the rest
      break;
    }
    offset += writeSize;
    data += writeSize;
    remainingSize -= writeSize;
  }
  if (!result) {
    writeIOErrors_.inc();
  }
  return result;
}

// reads size number of bytes from the device from the offset into value.
// Both offset and size are expected to be aligned for device IO operations.
// If successful and encryptor_ is defined, size bytes from
// validDataOffsetInValue offset in value are decrypted.
//
// returns true if successful, false otherwise.
bool Device::readInternal(uint64_t offset, uint32_t size, void* value) {
  XDCHECK_EQ(reinterpret_cast<uint64_t>(value) % ioAlignmentSize_, 0ul);
  XDCHECK_LE(offset + size, size_);
  uint8_t* data = reinterpret_cast<uint8_t*>(value);
  auto remainingSize = size;
  auto maxReadSize = (maxIOSize_ == 0) ? remainingSize : maxIOSize_;
  bool result = true;
  uint64_t curOffset = offset;
  while (remainingSize > 0) {
    auto readSize = std::min<size_t>(maxReadSize, remainingSize);
    XDCHECK_EQ(curOffset % ioAlignmentSize_, 0ul);
    XDCHECK_EQ(size % ioAlignmentSize_, 0ul);

    auto timeBegin = getSteadyClock();
    result = readImpl(curOffset, readSize, data);
    readLatencyEstimator_.trackValue(
        toMicros(getSteadyClock() - timeBegin).count());

    if (!result) {
      readIOErrors_.inc();
      return false;
    }
    bytesRead_.add(readSize);
    curOffset += readSize;
    data += readSize;
    remainingSize -= readSize;
  }
  if (encryptor_) {
    XCHECK_EQ(offset % encryptor_->encryptionBlockSize(), 0ul);
    auto res = encryptor_->decrypt(
        folly::MutableByteRange{reinterpret_cast<uint8_t*>(value), size},
        offset);
    if (!res) {
      decryptionErrors_.inc();
      return false;
    }
  }
  return result;
}

// This API reads size bytes from the Device from offset into a Buffer and
// returns the Buffer. If offset and size are not aligned to device's
// ioAlignmentSize_, IO aligned offset and IO aligned size are determined
// and passed to device read. Upon successful read from the device, the
// buffer is adjusted to return the intended data by trimming the data in
// the front and back.
// An empty buffer is returned in case of error and the caller must check
// the buffer size returned with size passed in to check for errors.
Buffer Device::read(uint64_t offset, uint32_t size) {
  XDCHECK_LE(offset + size, size_);
  uint64_t readOffset =
      offset & ~(static_cast<uint64_t>(ioAlignmentSize_) - 1ul);
  uint64_t readPrefixSize =
      offset & (static_cast<uint64_t>(ioAlignmentSize_) - 1ul);
  auto readSize = getIOAlignedSize(readPrefixSize + size);
  auto buffer = makeIOBuffer(readSize);
  bool result = readInternal(readOffset, readSize, buffer.data());
  if (!result) {
    return Buffer{};
  }
  buffer.trimStart(readPrefixSize);
  buffer.shrink(size);
  return buffer;
}

// This API reads size bytes from the Device from the offset into value.
// Both offset and size are expected to be IO aligned.
bool Device::read(uint64_t offset, uint32_t size, void* value) {
  return readInternal(offset, size, value);
}

void Device::getCounters(const CounterVisitor& visitor) const {
  visitor("navy_device_bytes_written", getBytesWritten(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_device_bytes_read", getBytesRead(),
          CounterVisitor::CounterType::RATE);
  readLatencyEstimator_.visitQuantileEstimator(visitor,
                                               "navy_device_read_latency_us");
  writeLatencyEstimator_.visitQuantileEstimator(visitor,
                                                "navy_device_write_latency_us");
  visitor("navy_device_read_errors", readIOErrors_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_device_write_errors", writeIOErrors_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_device_encryption_errors", encryptionErrors_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_device_decryption_errors", decryptionErrors_.get(),
          CounterVisitor::CounterType::RATE);
}

namespace {
/*
 * IOOp
 */
std::string IOOp::toString() const {
  return fmt::format(
      "[req {}] idx {} fd {} op {} offset {} size {} data {} resubmitted {}",
      reinterpret_cast<const void*>(&parent_), idx_, fd_, parent_.getOpName(),
      offset_, size_, data_, resubmitted_);
}

bool IOOp::done(ssize_t len) {
  XDCHECK(parent_.opType_ == READ || parent_.opType_ == WRITE);

  bool result = (len == size_);
  if (!result) {
    // Report IO errors
    XLOG_N_PER_MS(ERR, 10, 1000) << folly::sformat(
        "[{}] IO error: {} len={} errno={} ({})", parent_.context_.getName(),
        toString(), len, errno, std::strerror(errno));
  }

  // Check for timeout
  auto curTime = getSteadyClock();
  auto delayMs = toMillis(curTime - startTime_).count();
  if (delayMs > static_cast<int64_t>(kIOTimeoutMs)) {
    XLOG_N_PER_MS(ERR, 10, 1000)
        << fmt::format("[{}] IO timeout {}ms (submit +{}ms comp +{}ms): {}",
                       parent_.context_.getName(), delayMs,
                       toMillis(submitTime_ - startTime_).count(),
                       toMillis(curTime - submitTime_).count(), toString());
  }

  parent_.notifyOpResult(result);
  return result;
}

/*
 * IOReq
 */

IOReq::IOReq(IoContext& context,
             const std::vector<folly::File>& fvec,
             uint32_t stripeSize,
             OpType opType,
             uint64_t offset,
             uint32_t size,
             void* data,
             std::optional<int> placeHandle)
    : context_(context),
      opType_(opType),
      offset_(offset),
      size_(size),
      data_(data),
      placeHandle_(placeHandle) {
  uint8_t* buf = reinterpret_cast<uint8_t*>(data_);
  uint32_t idx = 0;
  if (fvec.size() > 1) {
    // For RAID devices
    while (size > 0) {
      uint64_t stripe = offset / stripeSize;
      uint32_t fdIdx = stripe % fvec.size();
      uint64_t stripeStartOffset = (stripe / fvec.size()) * stripeSize;
      uint32_t ioOffsetInStripe = offset % stripeSize;
      uint32_t allowedIOSize = std::min(size, stripeSize - ioOffsetInStripe);

      ops_.emplace_back(*this, idx++, fvec[fdIdx].fd(),
                        stripeStartOffset + ioOffsetInStripe, allowedIOSize,
                        buf, placeHandle_);

      size -= allowedIOSize;
      offset += allowedIOSize;
      buf += allowedIOSize;
    }
  } else {
    ops_.emplace_back(*this, idx++, fvec[0].fd(), offset_, size_, data_,
                      placeHandle_);
  }

  numRemaining_ = ops_.size();
}

bool IOReq::waitCompletion() {
  // Need to wait for Baton only for async io completion
  if (context_.isAsyncIoCompletion()) {
    baton_.wait();
  }

  // Check for timeout
  auto curTime = getSteadyClock();

  int64_t delayMs = 0;
  if (ops_.size() > 1) {
    // For RAID device, check req level completion timeout
    delayMs = toMillis(curTime - startTime_).count();
  } else {
    // For single device, check only the notification timeout
    delayMs = toMillis(curTime - compTime_).count();
  }

  if (delayMs > static_cast<int64_t>(kIOTimeoutMs)) {
    XLOG_N_PER_MS(ERR, 10, 1000) << fmt::format(
        "[{}] IOReq timeout {}ms (comp +{}ms notify +{}ms): {}",
        context_.getName(), delayMs, toMillis(compTime_ - startTime_).count(),
        toMillis(curTime - compTime_).count(), toString());
  }

  return result_;
}

void IOReq::notifyOpResult(bool result) {
  // aggregate result
  result_ = result_ && result;
  XDCHECK_GT(numRemaining_, 0u);
  if (--numRemaining_ > 0) {
    return;
  }

  // all done
  compTime_ = getSteadyClock();
  if (context_.isAsyncIoCompletion()) {
    baton_.post();
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
std::shared_ptr<IOReq> IoContext::submitRead(
    const std::vector<folly::File>& fvec,
    uint32_t stripeSize,
    uint64_t offset,
    uint32_t size,
    void* data) {
  auto req = std::make_shared<IOReq>(*this, fvec, stripeSize, OpType::READ,
                                     offset, size, data);
  submitReq(req);
  return req;
}

std::shared_ptr<IOReq> IoContext::submitWrite(
    const std::vector<folly::File>& fvec,
    uint32_t stripeSize,
    uint64_t offset,
    uint32_t size,
    const void* data,
    int placeHandle) {
  auto req =
      std::make_shared<IOReq>(*this, fvec, stripeSize, OpType::WRITE, offset,
                              size, const_cast<void*>(data), placeHandle);
  submitReq(req);
  return req;
}

void IoContext::submitReq(std::shared_ptr<IOReq> req) {
  // Now submit IOOp
  req->startTime_ = getSteadyClock();
  for (auto& op : req->ops_) {
    if (!submitIo(op)) {
      // Async IO submit should not fail
      XCHECK(!isAsyncIoCompletion());
      break;
    }
  }
}

/*
 * SyncIoContext
 */
ssize_t SyncIoContext::writeSync(int fd,
                                 uint64_t offset,
                                 uint32_t size,
                                 const void* value) {
  return ::pwrite(fd, value, size, offset);
}

ssize_t SyncIoContext::readSync(int fd,
                                uint64_t offset,
                                uint32_t size,
                                void* value) {
  return ::pread(fd, value, size, offset);
}

bool SyncIoContext::submitIo(IOOp& op) {
  op.startTime_ = getSteadyClock();

  ssize_t len;
  if (op.parent_.opType_ == OpType::READ) {
    len = readSync(op.fd_, op.offset_, op.size_, op.data_);
  } else {
    XDCHECK_EQ(op.parent_.opType_, OpType::WRITE);
    len = writeSync(op.fd_, op.offset_, op.size_, op.data_);
  }
  op.submitTime_ = getSteadyClock();

  return op.done(len);
}

/*
 * AsyncIoContext
 */
AsyncIoContext::AsyncIoContext(std::unique_ptr<folly::AsyncBase>&& asyncBase,
                               size_t id,
                               folly::EventBase* evb,
                               size_t capacity,
                               bool useIoUring,
                               std::vector<std::shared_ptr<FdpNvme>> fdpNvmeVec)
    : asyncBase_(std::move(asyncBase)),
      id_(id),
      qDepth_(capacity),
      useIoUring_(useIoUring),
      fdpNvmeVec_(fdpNvmeVec) {
#ifdef CACHELIB_IOURING_DISABLE
  // io_uring is not available on the system
  XDCHECK(!useIoUring_ && !(fdpNvmeVec_.size() > 0));
  useIoUring_ = false;
#endif
  if (evb) {
    compHandler_ =
        std::make_unique<CompletionHandler>(*this, evb, asyncBase_->pollFd());
  } else {
    // If EventBase is not provided, the completion will be waited
    // synchronously instead of being notified via epoll
    XDCHECK_EQ(qDepth_, 1u);
    // Retry is not supported without epoll for now
    retryLimit_ = 0;
  }

  XLOGF(INFO,
        "[{}] Created new async io context with qdepth {}{} io_engine {} {}",
        getName(), qDepth_, qDepth_ == 1 ? " (sync wait)" : "",
        useIoUring_ ? "io_uring" : "libaio",
        (fdpNvmeVec_.size() > 0) ? "FDP enabled" : "");
}

void AsyncIoContext::pollCompletion() {
  auto completed = asyncBase_->pollCompleted();
  handleCompletion(completed);
}

void AsyncIoContext::handleCompletion(
    folly::Range<folly::AsyncBaseOp**>& completed) {
  for (auto op : completed) {
    // AsyncBaseOp should be freed after completion
    std::unique_ptr<folly::AsyncBaseOp> aop(op);
    XDCHECK_EQ(aop->state(), folly::AsyncBaseOp::State::COMPLETED);

    auto iop = reinterpret_cast<IOOp*>(aop->getUserData());
    XDCHECK(iop);

    XDCHECK_GE(numOutstanding_, 0u);
    numOutstanding_--;
    numCompleted_++;

    // handle retry
    if (aop->result() == -EAGAIN && iop->resubmitted_ < retryLimit_) {
      iop->resubmitted_++;
      XLOG_N_PER_MS(ERR, 100, 1000)
          << fmt::format("[{}] resubmitting IO {}", getName(), iop->toString());
      submitIo(*iop);
      continue;
    }

    // Complete the IO and wake up waiter if needed
    if (iop->resubmitted_ > 0) {
      XLOG_N_PER_MS(ERR, 100, 1000)
          << fmt::format("[{}] resubmitted IO completed ({}) {}", getName(),
                         aop->result(), iop->toString());
    }

    auto len = aop->result();
    if (fdpNvmeVec_.size() > 0) {
      // 0 means success here, so get the completed size from iop
      len = !len ? iop->size_ : 0;
    }
    iop->done(len);

    if (!waitList_.empty()) {
      auto& waiter = waitList_.front();
      waitList_.pop_front();
      waiter.baton_.post();
    }
  }
}

bool AsyncIoContext::submitIo(IOOp& op) {
  op.startTime_ = getSteadyClock();

  while (numOutstanding_ >= qDepth_) {
    if (qDepth_ > 1) {
      XLOG_EVERY_MS(ERR, 10000) << fmt::format(
          "[{}] the number of outstanding requests {} exceeds the limit {}",
          getName(), numOutstanding_, qDepth_);
    }
    Waiter waiter;
    waitList_.push_back(waiter);
    waiter.baton_.wait();
  }

  std::unique_ptr<folly::AsyncBaseOp> asyncOp;
  asyncOp = prepAsyncIo(op);
  asyncOp->setUserData(&op);
  asyncBase_->submit(asyncOp.release());

  op.submitTime_ = getSteadyClock();

  numOutstanding_++;
  numSubmitted_++;

  if (!compHandler_) {
    // Wait completion synchronously if completion handler is not available.
    // i.e., when async io is used with non-epoll mode
    auto completed = asyncBase_->wait(1 /* minRequests */);
    handleCompletion(completed);
  }

  return true;
}

std::unique_ptr<folly::AsyncBaseOp> AsyncIoContext::prepAsyncIo(IOOp& op) {
  if (fdpNvmeVec_.size() > 0) {
    return prepNvmeIo(op);
  }

  std::unique_ptr<folly::AsyncBaseOp> asyncOp;
  IOReq& req = op.parent_;
  if (useIoUring_) {
#ifndef CACHELIB_IOURING_DISABLE
    asyncOp = std::make_unique<folly::IoUringOp>();
#endif
  } else {
    asyncOp = std::make_unique<folly::AsyncIOOp>();
  }

  if (req.opType_ == OpType::READ) {
    asyncOp->pread(op.fd_, op.data_, op.size_, op.offset_);
  } else {
    XDCHECK_EQ(req.opType_, OpType::WRITE);
    asyncOp->pwrite(op.fd_, op.data_, op.size_, op.offset_);
  }

  return asyncOp;
}

std::unique_ptr<folly::AsyncBaseOp> AsyncIoContext::prepNvmeIo(IOOp& op) {
#ifndef CACHELIB_IOURING_DISABLE
  std::unique_ptr<folly::IoUringOp> iouringCmdOp;
  IOReq& req = op.parent_;

  auto& options = static_cast<folly::IoUring*>(asyncBase_.get())->getOptions();
  iouringCmdOp = std::make_unique<folly::IoUringOp>(
      folly::AsyncBaseOp::NotificationCallback(), options);

  iouringCmdOp->initBase();
  struct io_uring_sqe& sqe = iouringCmdOp->getSqe();
  if (req.opType_ == OpType::READ) {
    fdpNvmeVec_[kDefaultFdpIdx]->prepReadUringCmdSqe(sqe, op.data_, op.size_,
                                                     op.offset_);
  } else {
    fdpNvmeVec_[kDefaultFdpIdx]->prepWriteUringCmdSqe(
        sqe, op.data_, op.size_, op.offset_, op.placeHandle_.value_or(-1));
  }
  io_uring_sqe_set_data(&sqe, iouringCmdOp.get());
  return std::move(iouringCmdOp);
#else
  return nullptr;
#endif
}

/*
 * FileDevice
 */
FileDevice::FileDevice(std::vector<folly::File>&& fvec,
                       std::vector<std::shared_ptr<FdpNvme>>&& fdpNvmeVec,
                       uint64_t fileSize,
                       uint32_t blockSize,
                       uint32_t stripeSize,
                       uint32_t maxIOSize,
                       uint32_t maxDeviceWriteSize,
                       IoEngine ioEngine,
                       uint32_t qDepthPerContext,
                       std::shared_ptr<DeviceEncryptor> encryptor)
    : Device(fileSize * fvec.size(),
             std::move(encryptor),
             blockSize,
             maxIOSize,
             maxDeviceWriteSize),
      fvec_(std::move(fvec)),
      fdpNvmeVec_(std::move(fdpNvmeVec)),
      stripeSize_(stripeSize),
      ioEngine_(ioEngine),
      qDepthPerContext_(qDepthPerContext) {
  XDCHECK_GT(blockSize, 0u);
  if (fvec_.size() > 1) {
    XDCHECK_GT(stripeSize_, 0u);
    XDCHECK_GE(stripeSize_, blockSize);
    XDCHECK_EQ(0u, stripeSize_ % 2) << stripeSize_;
    XDCHECK_EQ(0u, stripeSize_ % blockSize) << stripeSize_ << ", " << blockSize;

    if (fileSize % stripeSize != 0) {
      throw std::invalid_argument(
          folly::sformat("Invalid size because individual device size: {} is "
                         "not aligned to stripe size: {}",
                         fileSize, stripeSize));
    }
  }

  // Check qdepth configuration
  // 1. if io engine is Sync, then qdepth per context must be 0
  XDCHECK(ioEngine_ != IoEngine::Sync || qDepthPerContext_ == 0u);
  // 2. if io engine is Async, then qdepth per context must be greater than 0
  XDCHECK(ioEngine_ == IoEngine::Sync || qDepthPerContext_ > 0u);

  // Create sync io context. It will be also used for async io as well
  // for the path where device IO is called from non-fiber thread
  // (e.g., recovery path, read random alloc path)
  syncIoContext_ = std::make_unique<SyncIoContext>();

  XLOGF(
      INFO,
      "Created device with num_devices {} size {} block_size {},"
      "stripe_size {} max_write_size {} max_io_size {} io_engine {} qdepth {},"
      "num_fdp_devices {}",
      fvec_.size(), getSize(), blockSize, stripeSize, maxDeviceWriteSize,
      maxIOSize, getIoEngineName(ioEngine_), qDepthPerContext_,
      fdpNvmeVec_.size());
}

bool FileDevice::readImpl(uint64_t offset, uint32_t size, void* value) {
  auto req =
      getIoContext()->submitRead(fvec_, stripeSize_, offset, size, value);
  return req->waitCompletion();
}

bool FileDevice::writeImpl(uint64_t offset,
                           uint32_t size,
                           const void* value,
                           int placeHandle) {
  auto req = getIoContext()->submitWrite(fvec_, stripeSize_, offset, size,
                                         value, placeHandle);
  return req->waitCompletion();
}

void FileDevice::flushImpl() {
  for (const auto& f : fvec_) {
    ::fsync(f.fd());
  }
}

IoContext* FileDevice::getIoContext() {
  if (ioEngine_ == IoEngine::Sync) {
    return syncIoContext_.get();
  }

  if (!tlContext_) {
    bool onFiber = folly::fibers::onFiber();
    if (!onFiber && qDepthPerContext_ != 1) {
      // This is the case when IO is submitted from non-fiber thread
      // directly. E.g., recovery path at init, get sample item from
      // function scheduler. So, fallback to sync IO context instead
      return syncIoContext_.get();
    }

    // Create new context if on event base thread or useIoUring_ is enabled
    bool useIoUring = ioEngine_ == IoEngine::IoUring;

    folly::EventBase* evb = nullptr;
    auto pollMode = folly::AsyncBase::POLLABLE;
    if (onFiber) {
      evb = folly::EventBaseManager::get()->getExistingEventBase();
      XDCHECK(evb);
    } else {
      // If we are not on fiber and eventbase, we run in no-epoll mode with
      // qdepth of 1, i.e., async submission and sync wait
      XDCHECK_EQ(qDepthPerContext_, 1u);
      pollMode = folly::AsyncBase::NOT_POLLABLE;
    }

    std::unique_ptr<folly::AsyncBase> asyncBase;
    if (useIoUring) {
#ifndef CACHELIB_IOURING_DISABLE
      if (fdpNvmeVec_.size() > 0) {
        // Big sqe/cqe is mandatory for NVMe passthrough
        // https://elixir.bootlin.com/linux/v6.7/source/drivers/nvme/host/ioctl.c#L742
        folly::IoUringOp::Options options;
        options.sqe128 = true;
        options.cqe32 = true;
        asyncBase = std::make_unique<folly::IoUring>(
            qDepthPerContext_, pollMode, qDepthPerContext_, options);
      } else {
        asyncBase = std::make_unique<folly::IoUring>(
            qDepthPerContext_, pollMode, qDepthPerContext_);
      }
#endif
    } else {
      XDCHECK_EQ(ioEngine_, IoEngine::LibAio);
      asyncBase = std::make_unique<folly::AsyncIO>(qDepthPerContext_, pollMode);
    }

    auto idx = incrementalIdx_++;
    tlContext_.reset(new AsyncIoContext(std::move(asyncBase), idx, evb,
                                        qDepthPerContext_, useIoUring,
                                        fdpNvmeVec_));

    {
      // Keep pointers in a vector to ease the gdb debugging
      std::lock_guard<folly::fibers::TimedMutex> lock{dbgAsyncIoContextsMutex_};
      if (dbgAsyncIoContexts_.size() < idx + 1) {
        dbgAsyncIoContexts_.resize(idx + 1);
      }
      dbgAsyncIoContexts_[idx] = tlContext_.get();
    }
  }

  return tlContext_.get();
}

int FileDevice::allocatePlacementHandle() {
  static constexpr uint16_t kDefaultFdpIdx = 0u;
#ifndef CACHELIB_IOURING_DISABLE
  if (fdpNvmeVec_.size() > 0) {
    return fdpNvmeVec_[kDefaultFdpIdx]->allocateFdpHandle();
  }
#endif
  return -1;
}

} // namespace

std::unique_ptr<Device> createMemoryDevice(
    uint64_t size,
    std::shared_ptr<DeviceEncryptor> encryptor,
    uint32_t ioAlignSize) {
  return std::make_unique<MemoryDevice>(size, std::move(encryptor),
                                        ioAlignSize);
}

std::unique_ptr<Device> createDirectIoFileDevice(
    std::vector<folly::File> fVec,
    std::vector<std::string> filePaths,
    uint64_t fileSize,
    uint32_t blockSize,
    uint32_t stripeSize,
    uint32_t maxDeviceWriteSize,
    IoEngine ioEngine,
    uint32_t qDepthPerContext,
    bool isFDPEnabled,
    std::shared_ptr<DeviceEncryptor> encryptor) {
  XDCHECK(folly::isPowTwo(blockSize));

  uint32_t maxIOSize = maxDeviceWriteSize;
  std::vector<std::shared_ptr<FdpNvme>> fdpNvmeVec{};
#ifndef CACHELIB_IOURING_DISABLE
  if (isFDPEnabled) {
    try {
      if (filePaths.size() > 1) {
        throw std::invalid_argument(folly::sformat(
            "{} input files; but FDP mode does not support RAID files yet",
            filePaths.size()));
      }

      for (const auto& path : filePaths) {
        auto fdpNvme = std::make_shared<FdpNvme>(path);

        auto maxDevIOSize = fdpNvme->getMaxIOSize();
        if (maxDevIOSize != 0u &&
            (maxIOSize == 0u || maxDevIOSize < maxIOSize)) {
          maxIOSize = maxDevIOSize;
        }

        fdpNvmeVec.push_back(std::move(fdpNvme));
      }
    } catch (const std::exception& e) {
      XLOGF(ERR, "NVMe FDP mode could not be enabled {}, Errno: {}", e.what(),
            errno);
      fdpNvmeVec.clear();
      maxIOSize = 0u;
    }
  }
#endif

  if (maxIOSize != 0u) {
    maxDeviceWriteSize = std::min<size_t>(maxDeviceWriteSize, maxIOSize);
  }

  return std::make_unique<FileDevice>(std::move(fVec),
                                      std::move(fdpNvmeVec),
                                      fileSize,
                                      blockSize,
                                      stripeSize,
                                      maxIOSize,
                                      maxDeviceWriteSize,
                                      ioEngine,
                                      qDepthPerContext,
                                      encryptor);
}

std::unique_ptr<Device> createDirectIoFileDevice(
    std::vector<folly::File> fVec,
    uint64_t fileSize,
    uint32_t blockSize,
    uint32_t stripeSize,
    uint32_t maxDeviceWriteSize,
    std::shared_ptr<DeviceEncryptor> encryptor) {
  return createDirectIoFileDevice(std::move(fVec),
                                  {},
                                  fileSize,
                                  blockSize,
                                  stripeSize,
                                  maxDeviceWriteSize,
                                  IoEngine::Sync,
                                  0,
                                  false,
                                  encryptor);
}

} // namespace facebook::cachelib::navy

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

#include "cachelib/navy/common/ChecksumOffload.h"

#include <folly/fibers/FiberManager.h>
#include <folly/logging/xlog.h>
#include <folly/portability/Asm.h>

#include <cstring>
#include <random>
#include <vector>

#include "cachelib/navy/common/Hash.h"

#ifdef CACHELIB_BUILD_WITH_DTO
#include <dto.h>
#endif

namespace facebook {
namespace cachelib {
namespace navy {

#ifdef CACHELIB_BUILD_WITH_DTO

static_assert(sizeof(dto_async_op) <= 192,
              "AsyncChecksumOp::opStorage_ too small for dto_async_op");
static_assert(alignof(dto_async_op) <= 64,
              "AsyncChecksumOp::opStorage_ under-aligned for dto_async_op");

bool checksumOffloadSupported() { return true; }

AsyncChecksumOp::~AsyncChecksumOp() {
  // A submitted DSA operation writes to opStorage_ (completion record) and,
  // for copies, to dest_. It must be drained before this object dies.
  if (state_ == State::kDsaPending) {
    wait();
  }
}

void AsyncChecksumOp::submitImpl(uint8_t* dest,
                                 BufferView src,
                                 bool cacheControl) {
  XDCHECK(state_ == State::kIdle);
  dest_ = dest;
  src_ = src;
  copy_ = dest != nullptr;

  auto* op = reinterpret_cast<dto_async_op*>(opStorage_);
  const int rc = copy_ ? dto_submit_memcpy_crc(op, dest, src.data(),
                                               src.size(), cacheControl)
                       : dto_submit_crc(op, src.data(), src.size());
  if (rc == DTO_ASYNC_SUBMITTED) {
    state_ = State::kDsaPending;
    return;
  }
  // DTO_ASYNC_FALLBACK: nothing submitted or copied; run on CPU now.
  if (copy_) {
    std::memcpy(dest, src.data(), src.size());
  }
  crc_ = checksum(src);
  state_ = State::kCpuDone;
}

uint32_t AsyncChecksumOp::wait() {
  XDCHECK(state_ != State::kIdle);
  if (state_ == State::kCpuDone) {
    state_ = State::kIdle;
    return crc_;
  }

  auto* op = reinterpret_cast<dto_async_op*>(opStorage_);
  // Yielding suspends this fiber and lets other request fibers run on the
  // same NavyThread while the accelerator works; that is the actual
  // "free the writer thread" mechanism. Yield ONLY when another fiber is
  // ready to run: with a lone fiber, yield() returns immediately and the
  // loop would busy-spin through the FiberManager at full rate for the
  // whole accelerator operation, which is far more expensive than a pause
  // poll. Outside fiber context (plain thread-pool schedulers, tests) this
  // reduces to a pause-poll.
  auto* fm = folly::fibers::onFiber()
                 ? folly::fibers::FiberManager::getFiberManagerUnsafe()
                 : nullptr;
  int rc;
  while ((rc = dto_async_poll(op)) == DTO_ASYNC_PENDING) {
    if (fm && fm->hasReadyTasks()) {
      folly::fibers::yield();
    } else {
      folly::asm_volatile_pause();
    }
  }
  state_ = State::kIdle;
  if (rc == DTO_ASYNC_DONE) {
    return static_cast<uint32_t>(dto_async_crc_val(op));
  }
  // Accelerator failure: destination contents are unspecified, so redo the
  // whole operation on the CPU. dest_ is not yet visible to readers per the
  // submit contract, so overwriting is safe.
  if (copy_) {
    std::memcpy(dest_, src_.data(), src_.size());
  }
  return checksum(src_);
}

#else // !CACHELIB_BUILD_WITH_DTO

bool checksumOffloadSupported() { return false; }

AsyncChecksumOp::~AsyncChecksumOp() = default;

void AsyncChecksumOp::submitImpl(uint8_t* dest,
                                 BufferView src,
                                 bool /* cacheControl */) {
  XDCHECK(state_ == State::kIdle);
  if (dest != nullptr) {
    std::memcpy(dest, src.data(), src.size());
  }
  crc_ = checksum(src);
  state_ = State::kCpuDone;
}

uint32_t AsyncChecksumOp::wait() {
  XDCHECK(state_ == State::kCpuDone);
  state_ = State::kIdle;
  return crc_;
}

#endif // CACHELIB_BUILD_WITH_DTO

void AsyncChecksumOp::submitCopyAndChecksum(uint8_t* dest,
                                            BufferView src,
                                            bool cacheControl) {
  XDCHECK(dest);
  submitImpl(dest, src, cacheControl);
}

void AsyncChecksumOp::submitChecksum(BufferView src) {
  submitImpl(nullptr, src, false);
}

uint32_t copyAndChecksum(uint8_t* dest,
                         BufferView src,
                         folly::FunctionRef<void()> overlap) {
  AsyncChecksumOp op;
  // Cache control: destinations of fused copies (write buffers) are read
  // again shortly, by lookups served from in-memory buffers and by the
  // device flush path.
  op.submitCopyAndChecksum(dest, src, true /* cacheControl */);
  overlap();
  return op.wait();
}

uint32_t checksumWithOverlap(BufferView src,
                             folly::FunctionRef<void()> overlap) {
  AsyncChecksumOp op;
  op.submitChecksum(src);
  overlap();
  return op.wait();
}

bool checksumOffloadSelfCheck() {
  if (!checksumOffloadSupported()) {
    return false;
  }
  // Exercise both operations on a buffer large enough to exceed DTO's
  // minimum-size gates (DTO_CRC_MIN_BYTES / DTO_MIN_BYTES) so the DSA path
  // actually runs, and verify parity with navy::checksum() plus copy
  // fidelity. If DSA is unavailable, the CPU fallback must also match.
  constexpr size_t kSize = 1024 * 1024;
  std::vector<uint8_t> src(kSize);
  std::vector<uint8_t> dst(kSize, 0);
  std::mt19937 gen{12345};
  for (auto& b : src) {
    b = static_cast<uint8_t>(gen());
  }

  const BufferView view{src.size(), src.data()};
  const uint32_t sw = checksum(view);
  const uint32_t viaCrc = checksumWithOverlap(view, [] {});
  const uint32_t viaCopy = copyAndChecksum(dst.data(), view, [] {});
  return viaCrc == sw && viaCopy == sw &&
         std::memcmp(dst.data(), src.data(), kSize) == 0;
}

} // namespace navy
} // namespace cachelib
} // namespace facebook

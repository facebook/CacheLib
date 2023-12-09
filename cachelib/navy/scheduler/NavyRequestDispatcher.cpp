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

#include "cachelib/navy/scheduler/NavyRequestDispatcher.h"

#include "JobScheduler.h"

namespace facebook {
namespace cachelib {
namespace navy {

NavyRequestDispatcher::NavyRequestDispatcher(JobScheduler& scheduler,
                                             folly::StringPiece name,
                                             size_t maxOutstanding,
                                             size_t stackSize)
    : scheduler_(scheduler),
      name_(name),
      maxOutstanding_(maxOutstanding),
      worker_{name_, NavyThread::Options(stackSize)} {
  worker_.addTaskRemote([this]() {
    XLOGF(INFO, "[{}] Starting with max outstanding {}", getName(),
          maxOutstanding_);
  });
}

/*
 * Request dispatch loop
 *
 * The loop function is run by a fiber which is started when the first request
 * is arrived. Once started, the loop claims the incoming request queue by
 * extracting the head with replacing it with sentinel value (i.e., reserved
 * constant pointer value of 0x1) such that no other dispatcher loop can start.
 * The loop will keep polling the incoming request queue until no other requests
 * arrived while processing them. If no more requests arrived, the loop will
 * remove the sentinel value to indicate that the queue has been emptied.
 *
 * Since the request queue is a singly linked list in reverse order of arrival,
 * the queue is reversed to make sure FIFO order is preserved before dispatched.
 *
 * The loop can pause processing if the outstanding requests reached the limit.
 * Once paused, the loop will wait for the completion of any of the current
 * outstanding requests before resuming dispatches.
 */
void NavyRequestDispatcher::processLoop() {
  numPolled_.inc();

  // The sentinel used to claim the queue
  const auto sentinel = reinterpret_cast<NavyRequest*>(1);
  NavyRequest* incoming = nullptr;
  do {
    // Claim the queue so that no new dispatcher loop is started
    // while we are working on those submitted
    incoming = __atomic_exchange_n(&incomingReqs_, sentinel, __ATOMIC_ACQ_REL);

    // Inverse the incoming list to submit in FIFO order
    NavyRequest* pending = nullptr;
    while (incoming && incoming != sentinel) {
      auto* next = incoming->next_;
      incoming->next_ = pending;
      pending = incoming;
      incoming = next;
    }

    while (pending) {
      std::unique_ptr<NavyRequest> req(pending);
      pending = pending->next_;
      req->next_ = nullptr;
      numDispatched_.inc();

      // Enforce the maximum concurrent requests outstanding
      if (numOutstanding_.get() == maxOutstanding_) {
        // We are reusing the baton, so needs to be reset before use.
        // Note that we are supposed to be woken up by another fiber
        // running on the same thread
        baton_.reset();
        baton_.wait();
        XDCHECK_LT(numOutstanding_.get(), maxOutstanding_);
      }
      // Dispatch the Request
      scheduleReq(std::move(req));
    }

    // Try to unclaim the incomingReqs_ queue
    // If the head is not sentinel as expected, it means that some new requests
    // have been arrived while dispatching previous ones
    incoming = sentinel;
  } while (!__atomic_compare_exchange_n(&incomingReqs_, &incoming, nullptr,
                                        false, __ATOMIC_ACQ_REL,
                                        __ATOMIC_RELAXED));
}

void NavyRequestDispatcher::scheduleReq(std::unique_ptr<NavyRequest> req) {
  // Start a new fiber running the given request
  numOutstanding_.inc();
  worker_.addTask([this, rq = std::move(req)]() mutable {
    while (rq->execute() == JobExitCode::Reschedule) {
      folly::fibers::yield();
    }

    auto key = rq->getKey();
    // Release rq to destruct it
    rq.reset();

    scheduler_.notifyCompletion(key);
    numOutstanding_.dec();
    numCompleted_.inc();
    if (numOutstanding_.get() + 1 == maxOutstanding_) {
      baton_.post();
    }
  });
}

void NavyRequestDispatcher::submitReq(std::unique_ptr<NavyRequest> navyReq) {
  XDCHECK(!!navyReq);
  numSubmitted_.inc();

  auto* req = navyReq.release();
  NavyRequest* oldValue = nullptr;

  // Add the list to the head of the list. If the head was nullptr,
  // it means there is no dispatcher task is running, when a new one
  // needs to be started
  bool status = util::atomicUpdateValue(
      &incomingReqs_, &oldValue, [](NavyRequest*) { return true; },
      [&req](NavyRequest* cur) {
        req->next_ = cur;
        return req;
      });
  XDCHECK(status);

  if (!oldValue) {
    // We were the first one submitting to the queue, so start a fiber
    // running the dispatch loop
    worker_.addTaskRemote([this]() { processLoop(); });
  }
}

NavyRequestDispatcher::Stats NavyRequestDispatcher::getStats() {
  Stats stat;
  stat.numPolled = numPolled_.get();
  stat.numSubmitted = numSubmitted_.get();
  stat.numDispatched = numDispatched_.get();
  stat.numCompleted = numCompleted_.get();
  stat.curOutstanding = numOutstanding_.get();

  return stat;
}

} // namespace navy
} // namespace cachelib
} // namespace facebook

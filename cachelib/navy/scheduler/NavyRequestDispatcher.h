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

#pragma once

#include <cstdint>
#include <memory>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/common/Time.h"
#include "cachelib/navy/common/NavyThread.h"
#include "cachelib/navy/scheduler/JobScheduler.h"

namespace facebook {
namespace cachelib {
namespace navy {

// This class represents the parameters describing a job to be scheduled
class NavyRequest {
 public:
  explicit NavyRequest(Job&& job,
                       folly::StringPiece name,
                       JobType type,
                       uint64_t key)
      : job_(std::move(job)),
        name_(name),
        type_(type),
        key_(key),
        beginTime_(util::getCurrentTimeNs()) {}

  virtual ~NavyRequest() = default;

  // Return the type of the job
  JobType getType() const { return type_; }

  // Return the key of the job
  uint64_t getKey() const { return key_; }

  // Main function to run the request
  JobExitCode execute() { return job_(); }

  // next_ is a hook used to implement the atomic singly linked list
  NavyRequest* next_ = nullptr;

 protected:
  Job job_;

  const folly::StringPiece name_;

  const JobType type_;

  // Key of the Request
  const uint64_t key_;

  // Time when the request was scheduled to track the timings
  uint64_t beginTime_;
};

// NavyRequestDispatcher is a request dispatcher with MPSC atomic submission
// queue. For efficiency, this class implements a simple MPSC queue using an
// atomic intrusive singly linked list. Specifically, a new request is added
// at the head of the list in an atomic manner, so that list maintains
// requests in LIFO order. The actual dispatch is performed by a fiber task,
// which is started when the first request is pushed to the queue. In order to
// guarantee that a single dispatcher task is running, the dispatcher task
// claims the submission queue by replacing the head of the list with the
// sentinel value while dispatching. For actual dispatch, the dispatcher task
// needs to reverse the linked list to submit in FIFO order.
class NavyRequestDispatcher {
 public:
  struct Stats {
    // The number of processLoop invocations
    uint64_t numPolled = 0;
    // The number requests submitted
    uint64_t numSubmitted = 0;
    // The number requests dispatched to worker thread
    uint64_t numDispatched = 0;
    // The number of requests currently outstanding
    uint64_t numCompleted = 0;
    // The number of requests completed
    uint64_t curOutstanding = 0;
  };

  // @param scheduler       the parent scheduler to get completion
  // notification
  // @param name            name of the dispatcher
  // @param maxOutstanding  maximum number of concurrently running requests
  NavyRequestDispatcher(JobScheduler& scheduler,
                        folly::StringPiece name,
                        size_t maxOutstanding,
                        size_t stackSize);

  folly::StringPiece getName() { return name_; }

  // Add a new request to the dispatch queue
  void submitReq(std::unique_ptr<NavyRequest> req);

  // Wrapper to add task to the worker thread of this dispatcher
  void addTaskRemote(folly::Func func) {
    worker_.addTaskRemote(std::move(func));
  }

  // Return dispatcher stats
  Stats getStats();

 private:
  // Request dispatch loop
  void processLoop();

  // Actually submit the req to the worker thread
  void scheduleReq(std::unique_ptr<NavyRequest> req);

  // The parent scheduler to get completion notification
  JobScheduler& scheduler_;
  // Name of the dispatcher
  std::string name_;
  // The head of the custom implementation of atomic MPSC queue
  NavyRequest* incomingReqs_{nullptr};
  // Maximum number of outstanding requests
  size_t maxOutstanding_;
  // Baton used for waiting when limited by maxOutstanding_
  folly::fibers::Baton baton_;
  // Worker thread
  NavyThread worker_;

  // Internal atomic counters for stats tracking
  AtomicCounter numPolled_{0};
  AtomicCounter numSubmitted_{0};
  AtomicCounter numDispatched_{0};
  AtomicCounter numOutstanding_{0};
  AtomicCounter numCompleted_{0};
};

} // namespace navy
} // namespace cachelib
} // namespace facebook

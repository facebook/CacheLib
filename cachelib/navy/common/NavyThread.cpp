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

#include "cachelib/navy/common/NavyThread.h"

namespace facebook::cachelib::navy {

static thread_local NavyThread* currentNavyThread_ = nullptr;

NavyThread* getCurrentNavyThread() { return currentNavyThread_; }

NavyThread::NavyThread(folly::StringPiece name, Options options) {
  th_ = std::make_unique<folly::ScopedEventBaseThread>(name.str());

  folly::fibers::FiberManager::Options opts;
  opts.stackSize =
      options.stackSize ? options.stackSize : Options::kDefaultStackSize;
  auto& eb = *th_->getEventBase();
  fm_ = &folly::fibers::getFiberManager(eb, opts);

  eb.runInEventBaseThreadAndWait([this]() { currentNavyThread_ = this; });
}

NavyThread::~NavyThread() { th_.reset(); }

} // namespace facebook::cachelib::navy

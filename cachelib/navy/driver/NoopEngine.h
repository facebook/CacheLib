/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

#include "cachelib/navy/engine/Engine.h"

namespace facebook {
namespace cachelib {
namespace navy {
class NoopEngine final : public Engine {
 public:
  ~NoopEngine() override = default;
  Status insert(HashedKey /* hk */, BufferView /* value */) override {
    return Status::Rejected;
  }
  bool couldExist(HashedKey) override { return false; }
  Status lookup(HashedKey /* hk */, Buffer& /* value */) override {
    return Status::NotFound;
  }
  Status remove(HashedKey /* hk */) override { return Status::NotFound; }
  void flush() override {}
  void reset() override {}
  void persist(RecordWriter& /* rw */) override {}
  bool recover(RecordReader& /* rr */) override { return true; }
  void getCounters(const CounterVisitor& /* visitor */) const override {}
  uint64_t getMaxItemSize() const override { return UINT32_MAX; }
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

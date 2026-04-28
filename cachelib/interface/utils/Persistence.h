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

#include <folly/CPortability.h>

namespace facebook::cachelib::interface::utils {

/**
 * Base configuration for cache persistence. May be used or augmented by
 * components to specify whether the component should persist its state on
 * shutdown and attempt recovery on creation.
 */
class PersistenceConfigBase {
 public:
  PersistenceConfigBase(bool persist, bool recover)
      : persist_(persist), recover_(recover) {}

  FOLLY_ALWAYS_INLINE bool persist() const noexcept { return persist_; }
  FOLLY_ALWAYS_INLINE bool recover() const noexcept { return recover_; }

 private:
  bool persist_; // persist on shutdown
  bool recover_; // recover on startup
};

} // namespace facebook::cachelib::interface::utils

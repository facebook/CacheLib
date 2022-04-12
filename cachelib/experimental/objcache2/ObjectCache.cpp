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
#include "cachelib/experimental/objcache2/ObjectCache.h"

namespace facebook {
namespace cachelib {
namespace objcache2 {

ObjectCache::~ObjectCache() {
  for (auto itr = l1Cache_->begin(); itr != l1Cache_->end(); ++itr) {
    l1Cache_->remove(itr.asHandle());
  }
}

void ObjectCache::remove(folly::StringPiece key) {
  removes_.inc();
  l1Cache_->remove(key);
}

void ObjectCache::getCounters(
    std::function<void(folly::StringPiece, uint64_t)> visitor) const {
  visitor("objcache.lookups", lookups_.get());
  visitor("objcache.lookups.l1_hits", succL1Lookups_.get());
  visitor("objcache.inserts", inserts_.get());
  visitor("objcache.inserts.errors", insertErrors_.get());
  visitor("objcache.replaces", replaces_.get());
  visitor("objcache.removes", removes_.get());
  visitor("objcache.evictions", evictions_.get());
}

} // namespace objcache2
} // namespace cachelib
} // namespace facebook

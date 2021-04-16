// Copyright 2004-present Facebook. All Rights Reserved.
#include "cachelib/persistence/PersistenceManager.h"

#include <folly/File.h>
#include <folly/FileUtil.h>
#include <folly/hash/Checksum.h>

#include <fstream>

#include "cachelib/allocator/CacheAllocatorConfig.h"

namespace facebook::cachelib::persistence {

void PersistenceManager::saveCache(PersistenceStreamWriter& /*writer*/) {}

void PersistenceManager::restoreCache(PersistenceStreamReader& /*reader*/) {}

} // namespace facebook::cachelib::persistence

---
id: Cross_Host_Cache_Persistence
title: Cross Host Cache Persistence
---

Cachelib supports persisting the cache into a remote storage and restoring in a
different machine (so called cross host persistence). This is an enhancement to
[Cache Persistence
feature](Cache_persistence),
which persist the cache in the same machine across restarts. This is useful
when binary is started on a different machine without losing cache data in the
shared infra, but this is not a persistent storage that serves like databases.

## Prerequisites

1. Create a persistence cache with [Cache Persistence](Cache_persistence) user guide.
2. Shutdown cache successfully.
3. Only POSIX is supported.
4. Only default PageSize is supported.

## Implement Stream Reader and Writer APIs

To persist the cache, shutdown cache first, instantiate PersistenceManager with
the cache config, call saveCache function and pass in a writer. To restore a
cache,  instantiate PersistenceManager with the same cache config, call
restoreCache function and pass in a read, then create cache with
`SharedMemAttach`.

The reader and writer interfaces are defined in PersistenceManager, users need
to implement the APIs with their own choice of backend storage, e.g. Manifold,
S3, and NAS.

The interfaces are defined as

```cpp
namespace facebook::cachelib::persistence {
/**
 * Stream reader and writer APIs for PersistenceManager use.
 * read/write functions are called in a single thread.
 * Users should implement read/write functions with their
 * own storage backend, e.g. file, manifold, AWS.
 * Users should throw exception if any error happens during read/write,
 * exceptions are not handled by PersistenceManager so users should catch
 * and handle them properly.
 */
class PersistenceStreamReader {
 public:
  virtual ~PersistenceStreamReader() {}
  // data in IOBuf must remain valid until next read call,
  // it is recommanded to make IObuf maintain the lifetime of data.
  // the IObuf can't be chained.
  virtual folly::IOBuf read(size_t length) = 0;
  virtual char read() = 0;
};
class PersistenceStreamWriter {
 public:
  virtual ~PersistenceStreamWriter() {}
  // PersistenceManager guarantees the data in IOBuf
  // remain valid until flush() is called.
  // The implementation can save the IOBuf (e.g. add to IOBufQueue) here,
  // but the data must be either copied out or sent to remote storage
  // when flush() is called, data in IOBuf might be invalid after that.
  virtual void write(folly::IOBuf buffer) = 0;
  virtual void write(char c) = 0;
  virtual void flush() = 0;
};
}
```

Functions will be called in a single thread, and exceptions are expected to be
thrown in case of any error happens inside the functions. PersistenceManager
will serialize the data and stream to `PersistenceStreamWriter` during
persistence, it is recommended to buffer the data and flush to remote storage
in larger chunks, for example `PersistenceManagerWriter` could read a 1MB
payload from each `write` call, but internally buffers to a 16MB chunk before
flushing to manifold. On restoring, PersistenceManager will rely on the data
provided by `PersistenceStreamReader`, it will only fetch certain size of data
each time so prefetch a large chunk of data is preferred for better
performance, e.g. the  `PersistenceStreamReader` can download 16MB data chunk
from Manifold and return a small portion for each `read` call.

## Persist the Cache

To persist the cache to a remote storage, construct `PersistenceManger` with
CacheLibConfig, and call `saveCache` function with user's writer
implementation. Cache config (currently cache name only) is persisted for
validation purpose, cache metadata, cache data in RAM and Navy are persisted to
restore the cache instance on the different machine.

```cpp
#include "cachelib/persistence/PersistenceManager.h"
using namespace facebook::cachelib;

// Cache::Config config;
// Create cache with ShareMemNew or SharedMemAttach
// cache.shutdown();
YourWriterImplementation writer;
persistence::PersistenceManager manager(config);
manager.saveCache(writer);
```

### Error Handling

1. Errors and exceptions raised by Writer's function are not handled by
PersistenceManger, so users should catch and handle properly.
2. `std::invalid_argument` will be thrown if there is any error with the cache
config or cache instance during `saveCache`.
3. Persistence can be retried upon any error, cache instance is not changed
during `saveCache`.

## Restore the Cache

To restore the cache from a remote storage, construct `PersistenceManger` with
CacheLibConfig, and call `restoreCache` function with user's reader
implementation. Cache config should be compatible with the one provided for
persistence, and the reader should return exact same data as what is written to
writer during persistence.

```cpp
#include "cachelib/persistence/PersistenceManager.h"
using namespace facebook::cachelib;

// Cache::Config config;
YourReaderImplementation reader;
persistence::PersistenceManager manager(config);
manager.restoreCache(reader);
// Create cache with SharedMemAttach
```

### Error Handling

1. Errors and exceptions raised by Writer's function are not handled by
PersistenceManger, so users should catch and handle properly.
2. `std::invalid_argument` will be thrown if there is any error during
`restoreCache` , includes version mismatch, invalid data returned from reader,
wrong config provide (different cache name), etc.
3. Persistence version is required to be matched, while cache version mismatch
is only warning at the restore stage, compatibility check and migration is
handled in cache attachment.
4. By calling `restoreCache` any existing cachelib data/metadata will be
erased, and will not be recovered on any failure.
5. Attachment after restore might fail if the supplied cachelib config is
incompatible with the config of the restored cache content, or cache version is
incompatible.

### Incompatible Config Changes across Restarts (both same host or x-host)

1.  cacheSize
2.  cacheName
3.  usePosixForShm
4.  isCompactCache
5.  isNvmCacheEncryption
6.  isNvmCacheTruncateAllocSize
7.  accessConfig.numBuckets
8.  accessConfig.pageSize
9.  chainedItemAccessConfig.numBuckets
10.  nvmConfig.navyConfig.getFileName
11.  nvmConfig.navyConfig.getRaidPaths
12.  nvmConfig.navyConfig.getFileSize

## Simple Example

### Stream APIs

```
#include <fstream>
#include "folly/io/IOBufQueue.h"
#include "folly/io/IOBuf.h"
#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/persistence/PersistenceManager.h"
using namespace facebook::cachelib::persistence;

class FileReader : public PersistenceStreamReader {
 public:
  FileReader(const std::string& filename) : if_(filename, std::ifstream::in) {
  }

  ~FileReader() {
  }

  folly::IOBuf read(size_t length) override {
    auto buf = folly::IOBuf::create(length);
    if_.read(reinterpret_cast<char*>(buf->writableData()), length);

    if (!if_.good()) {
      throw std::invalid_argument("can't read more data from file");
    }

    buf->append(length);
    return std::move(*buf);
  }

  char read() override {
    char c;
    if (!if_.get(c)) {
      throw std::invalid_argument("can't read more data from file");
    }
    return c;
  }
 private:
  std::ifstream if_;
};

class FileWriter : public PersistenceStreamWriter {
 public:
  FileWriter(const std::string& filename) : of_(filename, std::ofstream::out) {
  }

  ~FileWriter() {
    flush();
  }

  void write(folly::IOBuf buffer) override {
    queue_.append(buffer);
  }

  void write(char c) override {
    queue_.append(&c, sizeof(bool));
  }

  void flush() override {
    auto buf = queue_.move();
    for (auto& br : *buf) {
      of_.write(reinterpret_cast<const char*>(br.data()), br.size());
    }
    of_.flush();
  }
 private:
   std::ofstream of_;
   folly::IOBufQueue queue_;
};
```

### Persist Cache

```
#include "cachelib/persistence/PersistenceManager.h"
using namespace facebook::cachelib;

using Cache = cachelib::LruAllocator;
using CacheConfig = typename Cache::Config;
using CacheKey = typename Cache::Key;
using CacheItemHandle = typename Cache::ItemHandle;

// cache config
CacheConfig config;
config
  .setCacheSize(1024 * 1024)
  .setCacheName("test")
  .enableCachePersistence("cacheDir")
  .usePosixForShm()
  .validate();

// init cache
std::unique_ptr<Cache> cache;
try {
  cache = std::make_unique<Cache>(Cache::SharedMemAttach, config);
} catch (const std::exception& e) {
  cache = std::make_unique<Cache>(Cache::SharedMemNew, config);
}

// ... use cache ...

// shutdown cache
cache->shutDown();

// persist cache
persistence::PersistenceManager manager(config);
try {
  FileWriter writer("/nas/file/path");
  manager.saveCache(writer);
} catch (const std::exception& e) {
  std::cerr << "Couldn't persist cache: " << e.what() << std::endl;
}
```

### Restore Cache

```
#include "cachelib/persistence/PersistenceManager.h"
using namespace facebook::cachelib;

using Cache = cachelib::LruAllocator;
using CacheConfig = typename Cache::Config;
using CacheKey = typename Cache::Key;
using CacheItemHandle = typename Cache::ItemHandle;

// cache config
CacheConfig config;
config
  .setCacheSize(1024 * 1024)
  .setCacheName("test")
  .enableCachePersistence("cacheDir")
  .usePosixForShm()
  .validate();

// restore cache
persistence::PersistenceManager manager(config);
try {
  FileReader reader("/nas/file/path");
  manager.restoreCache(reader);
} catch (const std::exception& e) {
  std::cerr << "Couldn't restore cache: " << e.what() << std::endl;
}

// try to attach cache
std::unique_ptr<Cache> cache;
try {
  cache = std::make_unique<Cache>(Cache::SharedMemAttach, config);
} catch (const std::exception& e) {
  cache = std::make_unique<Cache>(Cache::SharedMemNew, config);
}

// ... use cache ...

// shutdown cache
cache->shutDown();
```

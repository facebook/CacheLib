# CacheLib: A Pluggable Caching Engine for High-Performance Services

<p align="center">
  <img width="500" height="140" alt="CacheLib" src="website/static/img/CacheLib-Logo-Large-transp.png">
</p>

CacheLib is a C++ library that provides a powerful, flexible, and high-performance in-process caching engine. It is designed to build and scale services that require efficient memory management, low-latency access to cached data, and the ability to leverage both DRAM and SSD storage transparently.

See [www.cachelib.org](https://cachelib.org) for comprehensive documentation and more information.

## Quick Start

Here is a minimal example of how to set up and use a simple CacheLib cache:

```cpp
#include <cachelib/allocator/CacheAllocator.h>
#include <iostream>

using Cache = facebook::cachelib::CacheAllocator;
using PoolId = facebook::cachelib::PoolId;

int main() {
    // 1. Configure the cache
    Cache::Config config;
    config.setCacheSize(1024 * 1024 * 1024); // 1GB
    config.setCacheName("MyFirstCache");
    config.setAccessConfig({25, 10}); // Use 25 buckets and 10 locks

    // 2. Create the cache instance
    std::unique_ptr<Cache> cache = std::make_unique<Cache>(config);
    PoolId defaultPool = cache->addPool("default", cache->getCacheMemoryStats().cacheSize);

    // 3. Allocate and write to an item
    auto handle = cache->allocate(defaultPool, "my_key", 100);
    if (handle) {
        std::strcpy(reinterpret_cast<char*>(handle->getMemory()), "Hello, CacheLib!");
        cache->insert(handle);
    }

    // 4. Read from the cache
    auto readHandle = cache->find("my_key");
    if (readHandle) {
        std::cout << reinterpret_cast<char*>(readHandle->getMemory()) << std::endl;
    }

    // 5. Remove from the cache
    cache->remove("my_key");

    return 0;
}
```

## Key Features

-   **High Performance**: Thread-safe, low-overhead API for concurrent access with zero-copy semantics.
-   **Hybrid Caching**: Transparently caches data across both DRAM and SSDs.
-   **Flexible Eviction Policies**: Includes a variety of caching algorithms like LRU, Segmented LRU, FIFO, 2Q, and TinyLFU.
-   **Memory Management**: Provides hard RSS memory constraints to prevent OOMs and efficiently caches variable-sized objects.
-   **Persistence**: Supports shared-memory based persistence of the cache across process restarts.
-   **Intelligent Tuning**: Automatically tunes the cache for dynamic changes in workload.
-   **Extensible**: Pluggable architecture for custom eviction policies and storage backends.

## Why Use CacheLib?

CacheLib is ideal for services that manage gigabytes of cached data and require fine-grained control over memory. Consider using CacheLib if your service needs:

-   **Efficient Memory Use**: Caching for variable-sized objects without fragmentation.
-   **High Concurrency**: Lock-free access patterns for high-throughput services.
-   **OOM Protection**: Strict memory limits to ensure cache usage does not impact service stability.
-   **Advanced Caching Algorithms**: Sophisticated eviction policies to maximize hit ratio.
-   **Fast Restarts**: The ability to restore cache state quickly after a process restart.

## Architecture Overview

CacheLib's architecture is designed for modularity and performance. It consists of three main components:

1.  **Access Container**: The indexing mechanism that maps keys to cached items. The default is a `ChainedHashTable` for O(1) lookups.
2.  **MM (Memory Management) Container**: Manages the eviction policy (e.g., LRU) and tracks item lifecycle.
3.  **Allocator**: Handles memory allocation from slabs, which are large chunks of memory carved from the total cache size.

These components work together to provide a highly concurrent and efficient caching system.

## Getting Started

### Building and Installation

We recommend using the provided `getdeps.py` script to build CacheLib and its dependencies.

```bash
# 1. Clone the repository
git clone https://github.com/facebook/CacheLib
cd CacheLib

# 2. Install system dependencies
sudo ./build/fbcode_builder/getdeps.py install-system-deps --recursive cachelib

# 3. Build CacheLib
python3 ./build/fbcode_builder/getdeps.py --allow-system-packages build cachelib

# 4. Run tests
python3 ./build/fbcode_builder/getdeps.py --allow-system-packages test cachelib
```

### Performance Benchmarking

CacheLib includes `CacheBench`, a powerful tool for evaluating cache performance against production workloads. See the [CacheBench documentation](https://cachelib.org/docs/Cache_Library_User_Guides/Cachebench_Overview) for more details.

## Contributing

We welcome contributions! If you're interested in helping make CacheLib better, please read our [Contributing Guide](CONTRIBUTING.md).

## Community

-   **GitHub Issues**: For bug reports and feature requests.
-   **Discussions**: For questions and community discussions.
-   **Stack Overflow**: Use the `cachelib` tag for questions.

## License

CacheLib is Apache-2.0 licensed, as found in the [LICENSE](LICENSE) file.

---
id: Cachebench_FB_HW_eval
title: Evaluating SSD hardware for Facebook workloads
---

`CacheBench` is a cache benchmark tool used to stress the storage and
memory subsystem in a comparable way that they are stressed
in production workloads. This doc describes how Facebook leverages CacheBench
to validate SSD performance.

## System requirements

To run CacheBench, first ensure that the machine has
sufficient free memory (50+GB) and SSD capacity (1TB).

* Memory: 64GB or more
* SSD Capacity: 100GB or more available capacity
* Internet connection capable of accessing github.com and installing packages

## Set up the SSD devices using mdraid

To gather SSD performance metrics, the SSD must be setup first. An example
below sets up a raid device to handle two ssds being used by CacheBench.

```sh
mdadm --create /dev/md0 --force --raid-devices=2 --level=0 --chunk=256 /dev/nvme1n1 /dev/nvme2n1
```

## Installing cachebench

1. If you have not already, clone the cachelib repository from github.com:

   ```sh
   git clone https://github.com/facebook/CacheLib.git
   ```

2. Build cachelib and `cachebench`:
    ```sh
    cd CacheLib
    ./contrib/build.sh -j
    ```
    Notes:
    * It will take several minutes to build and install all dependencies.
    * Remove `-j` flag to build using only a single CPU (build will take longer)
    * The script will automatically use `sudo` to install several OS packages (using `apt`, `dnf`, etc.)
    * The build script has been tested to work on stable Debian, Ubuntu, CentOS, RockyLinux.
      Other systems are possible but not officially supported.

3. The resulting binaries and libraries will be in `./opt/cachelib`:
    ```sh
    ./opt/cachelib/bin/cachebench --help
    ```
    or
    ```sh
    cd ./opt/cachelib/bin/
    ./cachebench --help
    ```

4. Sample test configurations are provided in `./opt/cachelib/test_configs/`.
    Example:

    ```sh
    cd ./opt/cachelib
    ./bin/cachebench --json_test_config ./test_configs/simple_test.json
    ```

<details>
<summary>Expected Output of test run</summary>

    $ cd ./opt/cachelib
    $ ./bin/cachebench --json_test_config ./test_configs/simple_test.json
    ===JSON Config===
    // @nolint instantiates a small cache and runs a quick run of basic operations.
    {
      "cache_config" : {
        "cacheSizeMB" : 512,
        "poolRebalanceIntervalSec" : 1,
        "moveOnSlabRelease" : false,

        "numPools" : 2,
        "poolSizes" : [0.3, 0.7]
      },
      "test_config" : {

          "numOps" : 100000,
          "numThreads" : 32,
          "numKeys" : 1000000,

          "keySizeRange" : [1, 8, 64],
          "keySizeRangeProbability" : [0.3, 0.7],

          "valSizeRange" : [1, 32, 10240, 409200],
          "valSizeRangeProbability" : [0.1, 0.2, 0.7],

          "getRatio" : 0.15,
          "setRatio" : 0.8,
          "delRatio" : 0.05,
          "keyPoolDistribution": [0.4, 0.6],
          "opPoolDistribution" : [0.5, 0.5]
        }
    }

    Welcome to OSS version of cachebench
    Created 897,355 keys in 0.00 mins
    Generating 1.60M sampled accesses
    Generating 1.60M sampled accesses
    Generated access patterns in 0.00 mins
    Total 3.20M ops to be run
    12:07:12       0.00M ops completed
    == Test Results ==
    == Allocator Stats ==
    Items in RAM  : 96,995
    Items in NVM  : 0
    Alloc Attempts: 2,559,176 Success: 100.00%
    RAM Evictions : 2,163,672
    Cache Gets    : 480,592
    Hit Ratio     :  10.97%
    NVM Gets      :               0, Coalesced : 100.00%
    NVM Puts      :               0, Success   :   0.00%, Clean   : 100.00%, AbortsFromDel   :        0, AbortsFromGet   :        0
    NVM Evicts    :               0, Clean     : 100.00%, Unclean :       0, Double          :        0
    NVM Deletes   :               0 Skipped Deletes: 100.00%
    Released 21 slabs
      Moves     : attempts:          0, success: 100.00%
      Evictions : attempts:      3,040, success:  99.57%

    == Throughput for  ==
    Total Ops : 3.20 million
    Total sets: 2,559,176
    get       :    49,453/s, success   :  10.97%
    set       :   263,344/s, success   : 100.00%
    del       :    16,488/s, found     :  10.83%

</details>


5. If fio is not installed, build it with:
    ```sh
    git clone https://github.com/axboe/fio.git
    cd fio
    ./configure
    make
    make install
    ```

See [build and installation](../installation/) for further details.

## Running the benchmark for SSD perf testing

Cachebench has three configs packaged for SSD validation. These are under `test_configs/ssd_perf/<service-domain>`. Currently, we have "graph_cache_leader", "kvcache_reg", and "kvcache_wc" which represent three distinct cache workloads from Facebook. Below, we show how the benchmarks can be run for two of these workloads. It is important to trim the ssds between the runs to ensure any interference is avoided.


1. Change to the path where you previously copied cachebench to.
    ```sh
    cd <your path>
    ```
2. If `/dev/md0` is not being used, edit workload files appropiately.
   Change all instances of `/dev/md0` to raw path of data SSD(s):
    ```sh
    vi ./test_configs/ssd_perf/graph_cache_leader/config.json
    vi ./test_configs/ssd_perf/kvcache_l2_wc/config.json
    ```
    See [configuring storage path](Configuring_cachebench_parameters#storage-filedevicedirectory-path-info)  for more details on how to configure the storage path.
3. Before each benchmark run, fully trim the drive with fio:
    ```sh
   fio --name=trim --filename=/dev/md0 --rw=trim --bs=3G
    ```
3. Execute social graph leader cache workload:
    ```sh
    ./cachebench -json_test_config test_configs/ssd_perf/graph_cache_leader/config.json --progress_stats_file=/tmp/graph_cache_leader.log
    ```
4. Fully trim the drive with fio again:
    ```sh
   fio --name=trim --filename=/dev/md0 --rw=trim --bs=3G
   ```
5. Execute the `kvcache` workload:
    ```sh
    ./cachebench -json_test_config test_configs/ssd_perf/kvcache_l2_wc/config.json —progress_stats_file=/tmp/mc-l2-wc.log
    ```

## Tuning the workload and cache parameters

For a full list of options that can be configured, see [configuring cachebench](Configuring_cachebench_parameters)

1. **Duration of Replay** - To run cachebench operation for longer,
   increase the `numOps` appropriately in the config file.
2. **Device Info** - Device info is configured in the config file
   using the `nvmCachePaths` option.  If you would rather use a
   filesystem based cache, pass the appropriate path through
   `nvmCachePaths`.  The benchmark will create a single file
   under that path corresponding to the configured `nvmCacheSizeMB`
3. **Watching Progress** -  While the benchmark runs, you can monitor the
   progress so far. The interval for progress update can be configured
   using the `--progress` and specifying a duration in seconds.
   If `--progress-stats-file` is also specified, on every progress
   interval, `cachebench` would log the internal stats to the specified file.

## Getting the Results

View results summary through the log file:

```sh
tail -n 50 /tmp/graph_cache_leader.log
tail -n 50 /tmp/mc-l2-wc.log
```
## Plotting latency stats
The stats output can be parsed to plot SSD latency information over time. To
do this, first ensure `gnuplot` is installed. For example, on CentOs:

```shell
yum install gnuplot
```

Then run this command to get the latency stats:

```shell
./vizualize/extract_latency.sh /tmp/graph_cache_leader.log
./vizualize/extract_latency.sh /tmp/mc-l2-wc.log
```

This should produce a tsv file for read latency, a tsv file for write latency, and the corresponding `png` files that have the graphs plotted.

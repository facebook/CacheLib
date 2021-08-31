# Benchmarking with CacheBench

## Introduction

`cachebench` is a cache benchmark tool used to stress the storage and
memory subsystem in a comparable way that they are stressed
in production workloads.

`cachebench` has two configs packaged for flash HW validation.
These are under `test_configs/ssd_perf/<service-domain>`.
Currently, we have 'tao-leader', 'memcache-wc' which represents two
distinct workloads in the cache space.

To run any of them, first ensure that the machine has
sufficient free memory (50+GB) and SSD capacity (1TB).

## System requirements

* Memory: 64GB
* SSD Capacity: 1TB available capacity
* Internet connection capable of accessing github.com and installing packages

## Mdraid (Default)

```sh
mdadm --create /dev/md0 --force --raid-devices=2 --level=0 --chunk=256 /dev/nvme1n1 /dev/nvme2n1
```

## Cachebench installion instructions

1. Clone the cachelib repository from github.com:
   `git clone git@github.com:facebookincubator/CacheLib.git`
2. Build cachelib and `cachebench`:
    ```sh
    cd CacheLib
   ./contrib/build.sh -d -j -v
   ```
    Note: it will take several minutes in install with all dependencies
3. Copy cachebench executable to cachebench folder:
    `cp build-cachelib/cachebench/cachebench cachelib/cachebench/`
4. If fio is not installed, build it with:
    ```sh
    git clone git@github.com:axboe/fio.git
    ./configure
    make
    make install
    ```

See [README.build.md](README.build.md) for further details.

## Running the benchmark for SSD perf testing

1. If `/dev/md0` is not being used, edit workload files appropiately.
   Change all instances of `/dev/md0` to raw path of data SSD(s):
    ```sh
    vi test_configs/ssd_perf/tao_leader/tao_leader_t10.json
    vi test_configs/ssd_perf/memcache_l2_wc/memcache_l2_wc.json
    ```
2. Before each benchmark run, fully trim the drive with fio:
   `fio --name=trim --filename=/dev/md0 --rw=trim --bs=3G`
3. Execute Tao Leader cachebench workload:
    ```sh
    cd cachelib/cachebench
    ./cachebench -json_test_config test_configs/ssd_perf/tao_leader/config.json --progress_stats_file=/tmp/tao_leader.log
    ```
4. Fully trim the drive with fio again:
   `fio --name=trim --filename=/dev/md0 --rw=trim --bs=3G`
5. Execute `memcache_l2_wc` cachebench workload:
    ```sh
    ./cachebench -json_test_config test_configs/ssd_perf/memcache_l2_wc/config.json —progress_stats_file=/tmp/mc-l2-wc.log
    ```

## Getting the Results

1.  View Results Summary:
    ```sh
    tail -n 50 /tmp/tao_leader.log
    tail -n 50 /tmp/mc-l2-wc.log
    ```
2.  Plotting Latency Stats. The stats output can be parsed
    to plot SSD latency information over time. To do this,
    first ensure `gnuplot` is installed:
    ```sh
    ./vizualize/extract_latency.sh /tmp/tao_leader.log
    ./vizualize/extract_latency.sh /tmp/mc-l2-wc.log
    ```
    Note: This should produce a tsv file each for read and write latency and
    corresponding png files that have the graphs plotted.


## Tuning the workload and cache parameters

1. **Duration of Replay** - To run cachebench operation for longer,
   increase the numOps appropriately in the config file.
2. **Device Info** - Device info is configured in the config file
   using the "nvmCachePaths" option.  If you would rather use a
   filesystem based cache, pass the appropriate path through
   `nvmCachePaths`.  The benchmark will create a single file
   under that path corresponding to the configured `nvmCacheSizeMB`
3. **Watching Progress** -  While the benchmark runs, you can monitor the
   progress so far. The interval for progress update can be configured
   using the `--progress` and specifying a duration in seconds.
   If `--progress-stats-file` is also specified, on every progress
   interval, `cachebench` would log the internal stats to the specified file.

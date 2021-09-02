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
   git clone git@github.com:facebook/CacheLib.git
   ```

2. Build cachelib and `cachebench`:
    ```sh
    cd CacheLib
   ./contrib/build.sh -d -j -v
   ```
    Note: it will take several minutes to install with all dependencies
3. Copy the cachebench executable and the configs to an appropriate location
   from which you want to run cachebench:
   ```sh
    cp ./opt/cachelib/bin/cachebench <your path>
    cp -r cachebench/test_configs <your path>
    cd <your path>
    ```

4. If fio is not installed, build it with:
    ``  ````sh
    git clone git@github.com:axboe/fio.git
    ./configure
    make
    make install
    ```

See [build and installation](../installation/installation) for further details.

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

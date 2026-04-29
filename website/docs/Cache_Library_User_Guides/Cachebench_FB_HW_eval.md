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

## Set up the SSD devices

To gather SSD performance metrics, the SSD must be setup first. Cachebench (and CacheLib) supports using various types of devices for NVM cache including a raw block device or a regular file. When one wants to use multiple SSDs as NVM cache, the CacheLib also provides a native support for RAID0 (i.e., striping).

Optionally, as an example, an user can setup and use md devices as follows. In this example, the md device is created from two ssd devices to be used as a raw block device in CacheBench.

```sh
mdadm --create /dev/md0 --force --raid-devices=2 --level=0 --chunk=256 /dev/nvme1n1 /dev/nvme2n1
```

## Installing cachebench

1. Clone the CacheLib repository and build:

    ```sh
    git clone https://github.com/facebook/CacheLib
    cd CacheLib
    sudo python3 ./build/fbcode_builder/getdeps.py install-system-deps --recursive cachelib
    python3 ./build/fbcode_builder/getdeps.py --allow-system-packages build cachelib
    ```

    Note: it will take several minutes to build and install all dependencies.

2. Locate the install directory and verify `cachebench` works:

    ```sh
    INST_DIR=$(python3 ./build/fbcode_builder/getdeps.py show-inst-dir cachelib)
    $INST_DIR/bin/cachebench --help
    ```

3. Sample test configurations are provided in the install directory.
    Example:

    ```sh
    $INST_DIR/bin/cachebench --json_test_config $INST_DIR/test_configs/simple_test.json
    ```

See [Installation](/docs/installation) for full build details and [Locating build output](/docs/installation#locating-build-output) for more on finding artifacts.

4. If fio is not installed, build it with:
    ```sh
    git clone https://github.com/axboe/fio.git
    cd fio
    ./configure
    make
    make install
    ```

## Running the benchmark for SSD perf testing

Cachebench has three configs packaged for SSD validation. These are under `test_configs/ssd_perf/<service-domain>`. Currently, we have "graph_cache_leader", "kvcache_reg", and "kvcache_wc" which represent three distinct cache workloads from Facebook. Below, we show how the benchmarks can be run for two of these workloads. It is important to trim the ssds between the runs to ensure any interference is avoided.


1. Set the install directory path:
    ```sh
    INST_DIR=$(python3 ./build/fbcode_builder/getdeps.py show-inst-dir cachelib)
    ```
2. If `/dev/md0` is not being used, edit workload files appropriately.
   Change all instances of `/dev/md0` to raw path of data SSD(s):
    ```sh
    vi $INST_DIR/test_configs/ssd_perf/graph_cache_leader/config.json
    vi $INST_DIR/test_configs/ssd_perf/kvcache_l2_wc/config.json
    ```
    See [configuring storage path](Configuring_cachebench_parameters#storage-filedevicedirectory-path-info) for more details on how to configure the storage path.
3. Before each benchmark run, fully trim the drive with fio:
    ```sh
    fio --name=trim --filename=/dev/md0 --rw=trim --bs=3G
    ```
4. Execute social graph leader cache workload:
    ```sh
    $INST_DIR/bin/cachebench -json_test_config $INST_DIR/test_configs/ssd_perf/graph_cache_leader/config.json --progress_stats_file=/tmp/graph_cache_leader.log
    ```
5. Fully trim the drive with fio again:
    ```sh
    fio --name=trim --filename=/dev/md0 --rw=trim --bs=3G
    ```
6. Execute the `kvcache` workload:
    ```sh
    $INST_DIR/bin/cachebench -json_test_config $INST_DIR/test_configs/ssd_perf/kvcache_l2_wc/config.json --progress_stats_file=/tmp/mc-l2-wc.log
    ```

### Tuning the workload and cache parameters

For a full list of options that can be configured, see [configuring cachebench](Configuring_cachebench_parameters)

1. **Target QPS** - The target QPS (Query/Transactions Per Second) can be configured
   using `opRatePerSec` and `opRateBurstSize` which controls the parameter of
   token-bucket
2. **Duration of Replay** - To run cachebench operation for longer,
   increase the `numOps` appropriately in the config file.
3. **Device Info** - Device info is configured in the config file
   using the `nvmCachePaths` option.  If you would rather use a
   filesystem based cache, pass the appropriate path through
   `nvmCachePaths`.  The benchmark will create a single file
   under that path corresponding to the configured `nvmCacheSizeMB`
4. **Watching Progress** -  While the benchmark runs, you can monitor the
   progress so far. The interval for progress update can be configured
   using the `--progress` and specifying a duration in seconds.
   If `--progress-stats-file` is also specified, on every progress
   interval, `cachebench` would log the internal stats to the specified file.
## Running cachebench with the trace workload

Meta is sharing anonymized traces captured from large scale production cache services. These traces are licensed under the same license as CacheLib. They are meant to help academic and industry researchers to optimize for our caching workloads. One can freely download it from our AWS S3 bucket and run the CacheBench to replay the trace with varying configuration as follows.

1. Install and setup the AWS CLI
2. Download the tracedata
   ```sh
   $ aws s3 ls --no-sign-request s3://cachelib-workload-sharing/pub/kvcache/202206/
   2023-02-09 13:37:50       1374 config_kvcache.json
   2023-02-09 13:38:58 4892102575 kvcache_traces_1.csv
   2023-02-09 13:38:58 4814294537 kvcache_traces_2.csv
   2023-02-09 13:38:58 4678364393 kvcache_traces_3.csv
   2023-02-09 13:38:58 4734675702 kvcache_traces_4.csv
   2023-02-09 13:38:58 4810857756 kvcache_traces_5.csv
   $ aws s3 cp --no-sign-request --recursive s3://cachelib-workload-sharing/pub/kvcache/202206/ ./
   ```
3. Modify the test config as needed (see following section)
   ```sh
   $ vi ./config_kvcache.json
   ```

4. Execute the trace workload
    ```sh
    $INST_DIR/bin/cachebench -json_test_config ./config_kvcache.json --progress_stats_file=/tmp/kvcache-trace.log
    ```

### List of traces
The list of traces uploaded are as follows

* `kvcache/202206`
   * Those are traces captured for 5 consecutive days from a Meta's key-value cache cluster consisting of 500 hosts
   * Each host uses (roughly) 42 GB of DRAM and 930 GB of SSD for caching
   * The traffic factor is 1/100

* `kvcache/202401`
   * Those are traces captured for 5 consecutive days from a Meta's key-value cache cluster consisting of 8000 hosts
   * Each host uses (roughly) 42 GB of DRAM and 930 GB of SSD for caching
   * The traffic factor is 1/125
   * This trace provides usecase and sub-usecase columns additionally; usecase identifies the tenant (i.e., application using distributed key-value cache). The sub-usecase is meant to further categorize the different traffics from the same usecase, but should be considered neither complete nor accurate

* `cdn/202303/`
   * Those are traces captured from Meta's 3 selected CDN cache clusters (named nha, prn, eag) respectively for 7 days on Mar 2023
   * Each cluster consists of 1000's of hosts
   * Each host uses (roughly) 40 GB of DRAM and 1.8TB of SSD for caching
   * Traffic factor and scaled cache sizes are:
      * nha: 1/6.37, DRAM 6006 MB, NVM 272314 MB
      * prn: 1/4.58, DRAM 8357 MB, NVM 375956 MB
      * eag: 1/13.4, DRAM 2857 MB, NVM 129619 MB

* `storage/202312`
   * Those are traces captured for 5 consecutive days from a Meta's block storage cluster consisting of 3000 hosts at the sampling ratio of 1/4000
   * Each host uses (roughly) 10 GB of DRAM and 380 GB of SSD for caching
   * The traffic factor is ~1
   
* `cdn/202504`
   * Those are traces captured for 7 consecurive days from a selected CDN edge cluster.
   * The cluster contains around 300 hosts.
   * Each host uses roughly 105GB DRAM and 3577 GB SSD for caching.
   * Traffic factor is 1/7.08. Scaled cache size DRAM 15150MB, SSD 1032294 MB.

### Resource Scaling or Trace Amplifcation

The trace is captured at a certain sampling ratio and the traffic factor is defined as the ratio of the captured traffic normalized to those received from a single host in the production. For example, the `kvcache/202206` is sampled at the ratio of 1/50'000 from 500 hosts, meaning the traffic factor is 1/100. This means that the trace data contains the samples amount to 1/100 of those handled by each host on average. So, in order to get the similar results from the cachebench simulation, either the cache resource or the trace data needs to be scaled accordingly.

The resource scaling is useful when the required resources (e.g., NVM cache 1TB) are not available in the simulation system or when one is interested in iterating fast with sacrificing accuracy of some metrics. The resource scaling can be done by modifying the `cache_config` in the cachebench test config.

1. **cacheSizeMB** - This controls the DRAM cache size
2. **nvmCacheSizeMB** - This controls the size of NVM cache

Amplifying the trace data is useful to achieve the highest accuracy of the simulation. The cachebench supports amplifying the trace data by amplifying the key population; specifically, when cachebench replays the trace entry, it duplicates each trace entry by specified number of times (i.e., amplification factor) by appending suffixes ranging from `0` to `k - 1` to the key for the amplification factor of `k`. The trace amplifcation factor can be specified in `test_config` section as part of `replayGeneratorConfig`.

1. **ampFactor** - specifies the amplification factor for replay generator


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

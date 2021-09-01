---
id: Cachebench_Overview
title: Overview
---

CacheBench is a benchmark suite that can read a workload configuration file, simulate cache behavior as stipulated in the config, and produce performance summary for the simulated cache. Results include metrics such as hit rate, evictions, write rate to flash cache, latency, etc. The workload configs can be hand-written by a human, produced by a workload analyzer, or backed by raw production cachelib traces. The main customization points into CacheBench are through writing workload configs or custom workload generators.

![](cachebench.png)

### Build the latest cachebench

Follow instructions in [Installation](../installation/installation) to build
cachebench. This should install cachebench in your local machine under
```opt/cachelib/bin/cachebench```

### Running cachebench for Facebook hardware validation

Cachebench has three configs packaged for SSD validation. These are under `test_configs/ssd_perf/<service-domain>`. Currently, we have "tao-leader", "memcache-reg", and "memcache-wc" which represent three distinct cache workloads from Facebook.

To run any of them, first ensure that the machine has sufficient free memory (50+GB). Next, modify the config.json file appropriately to reflect the SSD device setu;p. See [configuring storage path](Configuring_cachebench_parameters#storage-filedevicedirectory-path-info) for details. Then invoke the following.

```shell
./cachebench --json_test_config test_configs/ssd_perf/<service-domain>/config.json --progress_stats_file=/tmp/cachebench.log
```

This will stream the benchmark progress to the terminal and also log detailed stats to the specified file. The log file would contain a periodic dump of the latency  and cache stats, which  can then be plotted using some of the scripts.  If the stats are not intended to be collected, then skip the option `--progress_stats_file`.

### Analyzing the performance metrics

While cachebench runs, it will report some stats through the fb303 port. In addition, one can monitor the flash metrics from ODS on IOPS, nand writes etc. cachebench also periodically dumps some stats to the stdout, that can be processed later on.

### Plotting latency stats

The stats output can be parsed to plot NVM latency information over time. To do this, first ensure `gnuplot` is installed:

```shell
yum install gnuplot
```

Then run this command to get the latency stats:

```shell
./vizualize/extract_latency.sh /tmp/cachebench.log
```

This should produce a tsv file for read latency, a tsv file for write latency, and the corresponding `png` files that have the graphs plotted.

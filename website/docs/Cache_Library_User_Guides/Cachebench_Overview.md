---
id: Cachebench_Overview
title: Cachebench Overview
---

# What is cachebench?

CacheBench is a benchmark suite that can read a workload configuration file, simulate cache behavior as stipulated in the config, and produce performance summary for the simulated cache. Results include metrics such as hit rate, evictions, write rate to flash cache, latency, etc. The workload configs can be hand-written by a human, produced by a workload analyzer, or backed by raw production cachelib traces. The main customization points into CacheBench are through writing workload configs or custom workload generators.

![](cachebench.png)

# Downloading the latest package

```shell
mkdir cachebench; cd cachebench;
fbpkg.fetch cachelib.cachebench:latest
```

# Running cachebench on T10 HW

Cachebench has three configs packaged for flash HW validation. These are under `test_configs/hw_test_configs/<service-domain>`. Currently, we have "tao-leader", "memcache-reg", and "memcache-wc" which represent three distinct workloads in the cache space.

To run any of them, first ensure that the machine has sufficient free memory (50+GB).

## Tao Leader

```shell
./cachebench --json_test_config test_configs/ssd_perf/tao_leader/config.json --progress_stats_file=/tmp/tao_leader.log
```

## Memcache WC

```shell
./cachebench --json_test_config test_configs/ssd_perf/memcache_l2_wc/config.json  --progress_stats_file=/tmp/mc-l2-wc.log
```

## Memcache reg

```shell
./cachebench --json_test_config test_configs/ssd_perf/memcache_l2_reg/config.json  --progress_stats_file=/tmp/mc-l2-reg.log
```

This will stream the benchmark progress to the terminal and also log detailed stats to the specified file. The log file would contain a periodic dump of the latency  and cache stats, which  can then be plotted using some of the scripts.  If the stats are not intended to be collected, then skip the option `--progress_stats_file`.

# Analyzing the performance metrics

While cachebench runs, it will report some stats through the fb303 port. In addition, one can monitor the flash metrics from ODS on IOPS, nand writes etc. cachebench also periodically dumps some stats to the stdout, that can be processed later on.

## Plotting latency stats

The stats output can be parsed to plot NVM latency information over time. To do this, first ensure `gnuplot` is installed:

```shell
yum install gnuplot
```

Then run this command to get the latency stats:

```shell
./vizualize/extract_latency.sh /tmp/tao_leader.log
```

This should produce a tsv file for read latency, a tsv file for write latency, and the corresponding `png` files that have the graphs plotted.

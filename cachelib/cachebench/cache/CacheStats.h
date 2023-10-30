/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
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

#pragma once
#include <folly/Benchmark.h>
#include <gflags/gflags.h>

#include "cachelib/common/PercentileStats.h"

DECLARE_bool(report_api_latency);
DECLARE_string(report_ac_memory_usage_stats);

namespace facebook {
namespace cachelib {
namespace cachebench {

struct BackgroundEvictionStats {
  // the number of items this worker evicted by looking at pools/classes stats
  uint64_t nEvictedItems{0};

  // number of times we went executed the thread //TODO: is this def correct?
  uint64_t nTraversals{0};

  // number of classes
  uint64_t nClasses{0};

  // size of evicted items
  uint64_t evictionSize{0};
};

struct BackgroundPromotionStats {
  // the number of items this worker evicted by looking at pools/classes stats
  uint64_t nPromotedItems{0};

  // number of times we went executed the thread //TODO: is this def correct?
  uint64_t nTraversals{0};
};

struct Stats {
  BackgroundEvictionStats backgndEvicStats;
  BackgroundPromotionStats backgndPromoStats;

  uint64_t numEvictions{0};
  uint64_t numItems{0};

  uint64_t evictAttempts{0};
  uint64_t allocAttempts{0};
  uint64_t allocFailures{0};

  std::vector<double> poolUsageFraction;

  uint64_t numCacheGets{0};
  uint64_t numCacheGetMiss{0};
  uint64_t numCacheEvictions{0};
  uint64_t numRamDestructorCalls{0};
  uint64_t numNvmGets{0};
  uint64_t numNvmGetMiss{0};
  uint64_t numNvmGetCoalesced{0};

  uint64_t numNvmItems{0};
  uint64_t numNvmPuts{0};
  uint64_t numNvmPutErrs{0};
  uint64_t numNvmAbortedPutOnTombstone{0};
  uint64_t numNvmAbortedPutOnInflightGet{0};
  uint64_t numNvmPutFromClean{0};
  uint64_t numNvmUncleanEvict{0};
  uint64_t numNvmCleanEvict{0};
  uint64_t numNvmCleanDoubleEvict{0};
  uint64_t numNvmDestructorCalls{0};
  uint64_t numNvmEvictions{0};
  uint64_t numNvmBytesWritten{0};
  uint64_t numNvmNandBytesWritten{0};
  uint64_t numNvmLogicalBytesWritten{0};

  uint64_t numNvmItemRemovedSetSize{0};

  util::PercentileStats::Estimates cacheAllocateLatencyNs;
  util::PercentileStats::Estimates cacheFindLatencyNs;

  util::PercentileStats::Estimates nvmInsertLatencyNs;

  double nvmReadLatencyMicrosP50{0};
  double nvmReadLatencyMicrosP90{0};
  double nvmReadLatencyMicrosP99{0};
  double nvmReadLatencyMicrosP999{0};
  double nvmReadLatencyMicrosP9999{0};
  double nvmReadLatencyMicrosP99999{0};
  double nvmReadLatencyMicrosP999999{0};
  double nvmReadLatencyMicrosP100{0};
  double nvmWriteLatencyMicrosP50{0};
  double nvmWriteLatencyMicrosP90{0};
  double nvmWriteLatencyMicrosP99{0};
  double nvmWriteLatencyMicrosP999{0};
  double nvmWriteLatencyMicrosP9999{0};
  double nvmWriteLatencyMicrosP99999{0};
  double nvmWriteLatencyMicrosP999999{0};
  double nvmWriteLatencyMicrosP100{0};

  uint64_t numNvmExceededMaxRetry{0};

  uint64_t numNvmDeletes{0};
  uint64_t numNvmSkippedDeletes{0};

  uint64_t slabsReleased{0};
  uint64_t numAbortedSlabReleases{0};
  uint64_t numReaperSkippedSlabs{0};
  uint64_t moveAttemptsForSlabRelease{0};
  uint64_t moveSuccessesForSlabRelease{0};
  uint64_t evictionAttemptsForSlabRelease{0};
  uint64_t evictionSuccessesForSlabRelease{0};
  uint64_t numNvmRejectsByExpiry{0};
  uint64_t numNvmRejectsByClean{0};

  uint64_t inconsistencyCount{0};
  bool isNvmCacheDisabled{false};
  uint64_t invalidDestructorCount{0};
  int64_t unDestructedItemCount{0};

  std::map<PoolId, std::map<ClassId, ACStats>> allocationClassStats;

  // populate the counters related to nvm usage. Cache implementation can decide
  // what to populate since not all of those are interesting when running
  // cachebench.
  std::unordered_map<std::string, double> nvmCounters;

  std::map<PoolId, std::map<ClassId, uint64_t>> backgroundEvictionClasses;
  std::map<PoolId, std::map<ClassId, uint64_t>> backgroundPromotionClasses;

  // errors from the nvm engine.
  std::unordered_map<std::string, double> nvmErrors;

  void render(std::ostream& out) const {
    auto totalMisses = getTotalMisses();
    const double overallHitRatio = invertPctFn(totalMisses, numCacheGets);
    out << folly::sformat("Items in RAM  : {:,}", numItems) << std::endl;
    out << folly::sformat("Items in NVM  : {:,}", numNvmItems) << std::endl;

    out << folly::sformat("Alloc Attempts: {:,} Success: {:.2f}%",
                          allocAttempts,
                          invertPctFn(allocFailures, allocAttempts))
        << std::endl;
    out << folly::sformat("Evict Attempts: {:,} Success: {:.2f}%",
                          evictAttempts,
                          pctFn(numEvictions, evictAttempts))
        << std::endl;
    out << folly::sformat("RAM Evictions : {:,}", numEvictions) << std::endl;

    auto foreachAC = [](const auto& map, auto cb) {
      for (auto& pidStat : map) {
        for (auto& cidStat : pidStat.second) {
          cb(pidStat.first, cidStat.first, cidStat.second);
        }
      }
    };

    for (auto pid = 0U; pid < poolUsageFraction.size(); pid++) {
      out << folly::sformat("Fraction of pool {:,} used : {:.2f}", pid,
                            poolUsageFraction[pid])
          << std::endl;
    }

    if (FLAGS_report_ac_memory_usage_stats != "") {
      auto formatMemory = [&](size_t bytes) -> std::tuple<std::string, double> {
        if (FLAGS_report_ac_memory_usage_stats == "raw") {
          return {"B", bytes};
        }

        constexpr double KB = 1024.0;
        constexpr double MB = 1024.0 * 1024;
        constexpr double GB = 1024.0 * 1024 * 1024;

        if (bytes >= GB) {
          return {"GB", static_cast<double>(bytes) / GB};
        } else if (bytes >= MB) {
          return {"MB", static_cast<double>(bytes) / MB};
        } else if (bytes >= KB) {
          return {"KB", static_cast<double>(bytes) / KB};
        } else {
          return {"B", bytes};
        }
      };

      foreachAC(allocationClassStats, [&](auto pid, auto cid, auto stats) {
        auto [allocSizeSuffix, allocSize] = formatMemory(stats.allocSize);
        auto [memorySizeSuffix, memorySize] =
            formatMemory(stats.activeAllocs * stats.allocSize);
        out << folly::sformat("pid{:2} cid{:4} {:8.2f}{} memorySize: {:8.2f}{}",
                              pid, cid, allocSize, allocSizeSuffix, memorySize,
                              memorySizeSuffix)
            << std::endl;
      });

      foreachAC(allocationClassStats, [&](auto pid, auto cid, auto stats) {
        auto [allocSizeSuffix, allocSize] = formatMemory(stats.allocSize);

        // If the pool is not full, extrapolate usageFraction for AC assuming it
        // will grow at the same rate. This value will be the same for all ACs.
        double acUsageFraction;
        if (poolUsageFraction[pid] < 1.0) {
          acUsageFraction = poolUsageFraction[pid];
        } else if (stats.usedSlabs == 0) {
          acUsageFraction = 0.0;
        } else {
          acUsageFraction =
              stats.activeAllocs / (stats.usedSlabs * stats.allocsPerSlab);
        }

        out << folly::sformat(
                   "pid{:2} cid{:4} {:8.2f}{} usageFraction: {:4.2f}", pid, cid,
                   allocSize, allocSizeSuffix, acUsageFraction)
            << std::endl;
      });
    }

    if (numCacheGets > 0) {
      out << folly::sformat("Cache Gets    : {:,}", numCacheGets) << std::endl;
      out << folly::sformat("Hit Ratio     : {:6.2f}%", overallHitRatio)
          << std::endl;

      if (FLAGS_report_api_latency) {
        auto printLatencies =
            [&out](folly::StringPiece cat,
                   const util::PercentileStats::Estimates& latency) {
              auto fmtLatency = [&out, &cat](folly::StringPiece pct,
                                             double val) {
                out << folly::sformat("{:20} {:8} : {:>10.2f} ns\n", cat, pct,
                                      val);
              };

              fmtLatency("p50", latency.p50);
              fmtLatency("p90", latency.p90);
              fmtLatency("p99", latency.p99);
              fmtLatency("p999", latency.p999);
              fmtLatency("p9999", latency.p9999);
              fmtLatency("p99999", latency.p99999);
              fmtLatency("p999999", latency.p999999);
              fmtLatency("p100", latency.p100);
            };

        printLatencies("Cache Find API latency", cacheFindLatencyNs);
        printLatencies("Cache Allocate API latency", cacheAllocateLatencyNs);
      }
    }

    if (!backgroundEvictionClasses.empty() &&
        backgndEvicStats.nEvictedItems > 0) {
      out << "== Class Background Eviction Counters Map ==" << std::endl;
      foreachAC(backgroundEvictionClasses,
                [&](auto pid, auto cid, auto evicted) {
                  out << folly::sformat("pid{:2} cid{:4} evicted: {:4}", pid,
                                        cid, evicted)
                      << std::endl;
                });

      out << folly::sformat("Background Evicted Items : {:,}",
                            backgndEvicStats.nEvictedItems)
          << std::endl;
      out << folly::sformat("Background Evictor Traversals : {:,}",
                            backgndEvicStats.nTraversals)
          << std::endl;
    }

    if (!backgroundPromotionClasses.empty() &&
        backgndPromoStats.nPromotedItems > 0) {
      out << "== Class Background Promotion Counters Map ==" << std::endl;
      foreachAC(backgroundPromotionClasses,
                [&](auto pid, auto cid, auto promoted) {
                  out << folly::sformat("pid{:2} cid{:4} promoted: {:4}", pid,
                                        cid, promoted)
                      << std::endl;
                });

      out << folly::sformat("Background Promoted Items : {:,}",
                            backgndPromoStats.nPromotedItems)
          << std::endl;
      out << folly::sformat("Background Promoter Traversals : {:,}",
                            backgndPromoStats.nTraversals)
          << std::endl;
    }

    if (numNvmGets > 0 || numNvmDeletes > 0 || numNvmPuts > 0) {
      const double ramHitRatio = invertPctFn(numCacheGetMiss, numCacheGets);
      const double nvmHitRatio = invertPctFn(numNvmGetMiss, numNvmGets);

      out << folly::sformat(
          "RAM Hit Ratio : {:6.2f}%\n"
          "NVM Hit Ratio : {:6.2f}%\n",
          ramHitRatio, nvmHitRatio);

      out << folly::sformat(
          "RAM eviction rejects expiry : {:,}\nRAM eviction rejects clean : "
          "{:,}\n",
          numNvmRejectsByExpiry, numNvmRejectsByClean);

      folly::StringPiece readCat = "NVM Read  Latency";
      folly::StringPiece writeCat = "NVM Write Latency";
      folly::StringPiece nvmInsertCat = "NVM Insert Latency";
      auto fmtLatency = [&](folly::StringPiece cat, folly::StringPiece pct,
                            double val) {
        out << folly::sformat("{:20} {:8} : {:>10.2f} us\n", cat, pct, val);
      };

      fmtLatency(readCat, "p50", nvmReadLatencyMicrosP50);
      fmtLatency(readCat, "p90", nvmReadLatencyMicrosP90);
      fmtLatency(readCat, "p99", nvmReadLatencyMicrosP99);
      fmtLatency(readCat, "p999", nvmReadLatencyMicrosP999);
      fmtLatency(readCat, "p9999", nvmReadLatencyMicrosP9999);
      fmtLatency(readCat, "p99999", nvmReadLatencyMicrosP99999);
      fmtLatency(readCat, "p999999", nvmReadLatencyMicrosP999999);
      fmtLatency(readCat, "p100", nvmReadLatencyMicrosP100);

      fmtLatency(writeCat, "p50", nvmWriteLatencyMicrosP50);
      fmtLatency(writeCat, "p90", nvmWriteLatencyMicrosP90);
      fmtLatency(writeCat, "p99", nvmWriteLatencyMicrosP99);
      fmtLatency(writeCat, "p999", nvmWriteLatencyMicrosP999);
      fmtLatency(writeCat, "p9999", nvmWriteLatencyMicrosP9999);
      fmtLatency(writeCat, "p99999", nvmWriteLatencyMicrosP99999);
      fmtLatency(writeCat, "p999999", nvmWriteLatencyMicrosP999999);
      fmtLatency(writeCat, "p100", nvmWriteLatencyMicrosP100);

      fmtLatency(nvmInsertCat, "p50", nvmInsertLatencyNs.p50 / 1000.0);
      fmtLatency(nvmInsertCat, "p90", nvmInsertLatencyNs.p90 / 1000.0);
      fmtLatency(nvmInsertCat, "p99", nvmInsertLatencyNs.p99 / 1000.0);
      fmtLatency(nvmInsertCat, "p999", nvmInsertLatencyNs.p999 / 1000.0);
      fmtLatency(nvmInsertCat, "p9999", nvmInsertLatencyNs.p9999 / 1000.0);
      fmtLatency(nvmInsertCat, "p99999", nvmInsertLatencyNs.p99999 / 1000.0);
      fmtLatency(nvmInsertCat, "p999999", nvmInsertLatencyNs.p999999 / 1000.0);
      fmtLatency(nvmInsertCat, "p100", nvmInsertLatencyNs.p100 / 1000.0);

      constexpr double GB = 1024.0 * 1024 * 1024;
      double appWriteAmp =
          pctFn(numNvmBytesWritten, numNvmLogicalBytesWritten) / 100.0;

      double devWriteAmp =
          pctFn(numNvmNandBytesWritten, numNvmBytesWritten) / 100.0;
      out << folly::sformat("NVM bytes written (physical)  : {:6.2f} GB\n",
                            numNvmBytesWritten / GB);
      out << folly::sformat("NVM bytes written (logical)   : {:6.2f} GB\n",
                            numNvmLogicalBytesWritten / GB);
      out << folly::sformat("NVM bytes written (nand)      : {:6.2f} GB\n",
                            numNvmNandBytesWritten / GB);
      out << folly::sformat("NVM app write amplification   : {:6.2f}\n",
                            appWriteAmp);
      out << folly::sformat("NVM dev write amplification   : {:6.2f}\n",
                            devWriteAmp);
    }
    const double putSuccessPct =
        invertPctFn(numNvmPutErrs + numNvmAbortedPutOnInflightGet +
                        numNvmAbortedPutOnTombstone,
                    numNvmPuts);
    const double cleanEvictPct = pctFn(numNvmCleanEvict, numNvmEvictions);
    const double getCoalescedPct = pctFn(numNvmGetCoalesced, numNvmGets);
    out << folly::sformat("{:14}: {:15,}, {:10}: {:6.2f}%",
                          "NVM Gets",
                          numNvmGets,
                          "Coalesced",
                          getCoalescedPct)
        << std::endl;
    out << folly::sformat(
               "{:14}: {:15,}, {:10}: {:6.2f}%, {:8}: {:6.2f}%, {:16}: "
               "{:8,}, {:16}: {:8,}",
               "NVM Puts",
               numNvmPuts,
               "Success",
               putSuccessPct,
               "Clean",
               pctFn(numNvmPutFromClean, numNvmPuts),
               "AbortsFromDel",
               numNvmAbortedPutOnTombstone,
               "AbortsFromGet",
               numNvmAbortedPutOnInflightGet)
        << std::endl;
    out << folly::sformat(
               "{:14}: {:15,}, {:10}: {:6.2f}%, {:8}: {:7,},"
               " {:16}: {:8,}",
               "NVM Evicts",
               numNvmEvictions,
               "Clean",
               cleanEvictPct,
               "Unclean",
               numNvmUncleanEvict,
               "Double",
               numNvmCleanDoubleEvict)
        << std::endl;
    const double skippedDeletesPct = pctFn(numNvmSkippedDeletes, numNvmDeletes);
    out << folly::sformat("{:14}: {:15,} {:14}: {:6.2f}%",
                          "NVM Deletes",
                          numNvmDeletes,
                          "Skipped Deletes",
                          skippedDeletesPct)
        << std::endl;
    if (numNvmExceededMaxRetry > 0) {
      out << folly::sformat("{}: {}", "NVM max read retry reached",
                            numNvmExceededMaxRetry)
          << std::endl;
    }

    if (slabsReleased > 0) {
      out << folly::sformat(
                 "Released {:,} slabs\n"
                 "  Moves     : attempts: {:10,}, success: {:6.2f}%\n"
                 "  Evictions : attempts: {:10,}, success: {:6.2f}%",
                 slabsReleased,
                 moveAttemptsForSlabRelease,
                 pctFn(moveSuccessesForSlabRelease, moveAttemptsForSlabRelease),
                 evictionAttemptsForSlabRelease,
                 pctFn(evictionSuccessesForSlabRelease,
                       evictionAttemptsForSlabRelease))
          << std::endl;
    }

    if (!nvmCounters.empty()) {
      out << "== NVM Counters Map ==" << std::endl;
      for (const auto& it : nvmCounters) {
        out << it.first << "  :  " << it.second << std::endl;
      }
    }

    if (numRamDestructorCalls > 0 || numNvmDestructorCalls > 0) {
      out << folly::sformat("Destructor executed from RAM {}, from NVM {}",
                            numRamDestructorCalls, numNvmDestructorCalls)
          << std::endl;
    }

    if (numCacheEvictions > 0) {
      out << folly::sformat("Total eviction executed {}", numCacheEvictions)
          << std::endl;
    }
  }

  uint64_t getTotalMisses() const {
    return numNvmGets > 0 ? numNvmGetMiss : numCacheGetMiss;
  }

  std::tuple<double, double, double> getHitRatios(
      const Stats& prevStats) const {
    double overallHitRatio = 0.0;
    double ramHitRatio = 0.0;
    double nvmHitRatio = 0.0;

    if (numCacheGets > prevStats.numCacheGets) {
      auto totalMisses = getTotalMisses();
      auto prevTotalMisses = prevStats.getTotalMisses();

      overallHitRatio = invertPctFn(totalMisses - prevTotalMisses,
                                    numCacheGets - prevStats.numCacheGets);

      ramHitRatio = invertPctFn(numCacheGetMiss - prevStats.numCacheGetMiss,
                                numCacheGets - prevStats.numCacheGets);
    }

    if (numNvmGets > prevStats.numNvmGets) {
      nvmHitRatio = invertPctFn(numNvmGetMiss - prevStats.numNvmGetMiss,
                                numNvmGets - prevStats.numNvmGets);
    }

    return std::make_tuple(overallHitRatio, ramHitRatio, nvmHitRatio);
  }

  // Render the stats based on the delta between overall stats and previous
  // stats. It can be used to render the stats in the last time period.
  void render(const Stats& prevStats, std::ostream& out) const {
    if (numCacheGets > prevStats.numCacheGets) {
      auto [overallHitRatio, ramHitRatio, nvmHitRatio] =
          getHitRatios(prevStats);
      out << folly::sformat("Cache Gets    : {:,}",
                            numCacheGets - prevStats.numCacheGets)
          << std::endl;
      out << folly::sformat("Hit Ratio     : {:6.2f}%", overallHitRatio)
          << std::endl;

      out << folly::sformat(
          "RAM Hit Ratio : {:6.2f}%\n"
          "NVM Hit Ratio : {:6.2f}%\n",
          ramHitRatio, nvmHitRatio);
    }
  }

  void render(folly::UserCounters& counters) {
    auto calcInvertPctFn = [](uint64_t ops, uint64_t total) {
      return static_cast<int64_t>(invertPctFn(ops, total) * 100);
    };

    auto totalMisses = getTotalMisses();
    counters["num_items"] = numItems;
    counters["num_nvm_items"] = numNvmItems;
    counters["hit_rate"] = calcInvertPctFn(totalMisses, numCacheGets);

    counters["find_latency_p99"] = cacheFindLatencyNs.p99;
    counters["alloc_latency_p99"] = cacheAllocateLatencyNs.p99;

    counters["ram_hit_rate"] = calcInvertPctFn(numCacheGetMiss, numCacheGets);
    counters["nvm_hit_rate"] = calcInvertPctFn(numCacheGetMiss, numCacheGets);

    counters["nvm_read_latency_p99"] =
        static_cast<int64_t>(nvmReadLatencyMicrosP99);
    counters["nvm_write_latency_p99"] =
        static_cast<int64_t>(nvmWriteLatencyMicrosP99);

    constexpr double MB = 1024.0 * 1024;
    double appWriteAmp =
        pctFn(numNvmBytesWritten, numNvmLogicalBytesWritten) / 100.0;

    double devWriteAmp =
        pctFn(numNvmNandBytesWritten, numNvmBytesWritten) / 100.0;

    counters["nvm_bytes_written_physical_mb"] =
        static_cast<int64_t>(numNvmBytesWritten / MB);
    counters["nvm_bytes_written_logical_mb"] =
        static_cast<int64_t>(numNvmLogicalBytesWritten / MB);
    counters["nvm_bytes_written_nand_mb"] =
        static_cast<int64_t>(numNvmNandBytesWritten / MB);
    counters["nvm_app_write_amp"] = static_cast<int64_t>(appWriteAmp);
    counters["nvm_dev_write_amp"] = static_cast<int64_t>(devWriteAmp);
  }

  bool renderIsTestPassed(std::ostream& out) {
    bool pass = true;

    if (isNvmCacheDisabled) {
      out << "NVM Cache was disabled during test!" << std::endl;
      pass = false;
    }

    if (inconsistencyCount) {
      out << "Found " << inconsistencyCount << " inconsistent cases"
          << std::endl;
      pass = false;
    }

    if (invalidDestructorCount) {
      out << "Found " << invalidDestructorCount
          << " invalid item destructor cases" << std::endl;
      pass = false;
    }

    if (unDestructedItemCount) {
      out << "Found " << unDestructedItemCount
          << " items missing destructor cases" << std::endl;
      pass = false;
    }

    for (const auto& kv : nvmErrors) {
      std::cout << "NVM error. " << kv.first << " : " << kv.second << std::endl;
      pass = false;
    }

    if (numNvmItemRemovedSetSize != 0) {
      std::cout << "NVM error. ItemRemoved not empty" << std::endl;
      pass = false;
    }

    return pass;
  }

 private:
  static double pctFn(uint64_t ops, uint64_t total) {
    return total == 0
               ? 0
               : 100.0 * static_cast<double>(ops) / static_cast<double>(total);
  }

  static double invertPctFn(uint64_t ops, uint64_t total) {
    return 100 - pctFn(ops, total);
  }

}; // namespace cachebench

} // namespace cachebench
} // namespace cachelib
} // namespace facebook

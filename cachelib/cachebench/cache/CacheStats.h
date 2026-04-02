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

#include "cachelib/allocator/memory/MemoryAllocatorStats.h"
#include "cachelib/allocator/memory/Slab.h"
#include "cachelib/cachebench/cache/StatsBase.h"
#include "cachelib/common/PercentileStats.h"
#include "cachelib/interface/Stats.h"

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

class Stats : public StatsBase {
 public:
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

  double bcInsertLatencyMicrosP50{0};
  double bcInsertLatencyMicrosP90{0};
  double bcInsertLatencyMicrosP99{0};
  double bcInsertLatencyMicrosP999{0};
  double bcInsertLatencyMicrosP9999{0};
  double bcInsertLatencyMicrosP99999{0};
  double bcInsertLatencyMicrosP999999{0};
  double bcInsertLatencyMicrosP100{0};
  double bcLookupLatencyMicrosP50{0};
  double bcLookupLatencyMicrosP90{0};
  double bcLookupLatencyMicrosP99{0};
  double bcLookupLatencyMicrosP999{0};
  double bcLookupLatencyMicrosP9999{0};
  double bcLookupLatencyMicrosP99999{0};
  double bcLookupLatencyMicrosP999999{0};
  double bcLookupLatencyMicrosP100{0};
  double bcRemoveLatencyMicrosP50{0};
  double bcRemoveLatencyMicrosP90{0};
  double bcRemoveLatencyMicrosP99{0};
  double bcRemoveLatencyMicrosP999{0};
  double bcRemoveLatencyMicrosP9999{0};
  double bcRemoveLatencyMicrosP99999{0};
  double bcRemoveLatencyMicrosP999999{0};
  double bcRemoveLatencyMicrosP100{0};

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

  // Aggregate throughput stats from another instance. DOES NOT HANDLE
  // LATENCY STATS!
  Stats& operator+=(const StatsBase& otherBase) override {
    aggregated_ = true;
    auto& other = otherBase.as<Stats>();

    backgndEvicStats.nEvictedItems += other.backgndEvicStats.nEvictedItems;
    backgndEvicStats.nTraversals += other.backgndEvicStats.nTraversals;
    backgndEvicStats.nClasses += other.backgndEvicStats.nClasses;
    backgndEvicStats.evictionSize += other.backgndEvicStats.evictionSize;
    backgndPromoStats.nPromotedItems += other.backgndPromoStats.nPromotedItems;
    backgndPromoStats.nTraversals += other.backgndPromoStats.nTraversals;

    numEvictions += other.numEvictions;
    numItems += other.numItems;
    evictAttempts += other.evictAttempts;
    allocAttempts += other.allocAttempts;
    allocFailures += other.allocFailures;

    numCacheGets += other.numCacheGets;
    numCacheGetMiss += other.numCacheGetMiss;
    numCacheEvictions += other.numCacheEvictions;
    numRamDestructorCalls += other.numRamDestructorCalls;
    numNvmGets += other.numNvmGets;
    numNvmGetMiss += other.numNvmGetMiss;
    numNvmGetCoalesced += other.numNvmGetCoalesced;

    numNvmItems += other.numNvmItems;
    numNvmPuts += other.numNvmPuts;
    numNvmPutErrs += other.numNvmPutErrs;
    numNvmAbortedPutOnTombstone += other.numNvmAbortedPutOnTombstone;
    numNvmAbortedPutOnInflightGet += other.numNvmAbortedPutOnInflightGet;
    numNvmPutFromClean += other.numNvmPutFromClean;
    numNvmUncleanEvict += other.numNvmUncleanEvict;
    numNvmCleanEvict += other.numNvmCleanEvict;
    numNvmCleanDoubleEvict += other.numNvmCleanDoubleEvict;
    numNvmDestructorCalls += other.numNvmDestructorCalls;
    numNvmEvictions += other.numNvmEvictions;
    numNvmBytesWritten += other.numNvmBytesWritten;
    numNvmNandBytesWritten += other.numNvmNandBytesWritten;
    numNvmLogicalBytesWritten += other.numNvmLogicalBytesWritten;

    numNvmItemRemovedSetSize += other.numNvmItemRemovedSetSize;
    numNvmExceededMaxRetry += other.numNvmExceededMaxRetry;
    numNvmDeletes += other.numNvmDeletes;
    numNvmSkippedDeletes += other.numNvmSkippedDeletes;

    slabsReleased += other.slabsReleased;
    numAbortedSlabReleases += other.numAbortedSlabReleases;
    numReaperSkippedSlabs += other.numReaperSkippedSlabs;
    moveAttemptsForSlabRelease += other.moveAttemptsForSlabRelease;
    moveSuccessesForSlabRelease += other.moveSuccessesForSlabRelease;
    evictionAttemptsForSlabRelease += other.evictionAttemptsForSlabRelease;
    evictionSuccessesForSlabRelease += other.evictionSuccessesForSlabRelease;
    numNvmRejectsByExpiry += other.numNvmRejectsByExpiry;
    numNvmRejectsByClean += other.numNvmRejectsByClean;

    inconsistencyCount += other.inconsistencyCount;
    // doesn't make sense to combine isNvmCacheDisabled since different test
    // configs may or may not have NVM disabled
    invalidDestructorCount += other.invalidDestructorCount;
    unDestructedItemCount += other.unDestructedItemCount;

    // doesn't make sense to combine pool/allocation class stats since different
    // test setups may have different pool/allocation class configs

    auto accumulateMap = [](auto& mapA, const auto& mapB) {
      for (const auto& [key, otherVal] : mapB) {
        mapA[key] += otherVal;
      }
    };
    accumulateMap(nvmCounters, other.nvmCounters);
    accumulateMap(nvmErrors, other.nvmErrors);

    return *this;
  }

  std::string progress(const StatsBase& prevStatsBase) const override {
    const auto& prevStats = prevStatsBase.as<Stats>();
    auto hitRates = getHitRatios(prevStats);
    return folly::sformat(
        "{} items in cache. {} items in nvm cache. {} items evicted from nvm "
        "cache. Hit Ratio {:6.2f}% (RAM {:6.2f}%, NVM {:6.2f}%).",
        numItems,
        numNvmItems,
        numNvmEvictions,
        hitRates["overall"],
        hitRates["ram"],
        hitRates["nvm"]);
  }

  void render(std::ostream& out) const override {
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

    if (!aggregated_) {
      for (auto pid = 0U; pid < poolUsageFraction.size(); pid++) {
        out << folly::sformat("Fraction of pool {:,} used : {:.2f}", pid,
                              poolUsageFraction[pid])
            << std::endl;
      }

      if (FLAGS_report_ac_memory_usage_stats != "") {
        auto formatMemory =
            [&](size_t bytes) -> std::tuple<std::string, double> {
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
          out << folly::sformat(
                     "pid{:2} cid{:4} {:8.2f}{} memorySize: {:8.2f}{}", pid,
                     cid, allocSize, allocSizeSuffix, memorySize,
                     memorySizeSuffix)
              << std::endl;
        });

        foreachAC(allocationClassStats, [&](auto pid, auto cid, auto stats) {
          auto [allocSizeSuffix, allocSize] = formatMemory(stats.allocSize);

          // If the pool is not full, extrapolate usageFraction for AC assuming
          // it will grow at the same rate. This value will be the same for all
          // ACs.
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
                     "pid{:2} cid{:4} {:8.2f}{} usageFraction: {:4.2f}", pid,
                     cid, allocSize, allocSizeSuffix, acUsageFraction)
              << std::endl;
        });
      }
    }

    if (numCacheGets > 0) {
      out << folly::sformat("Cache Gets    : {:,}", numCacheGets) << std::endl;
      out << folly::sformat("Hit Ratio     : {:6.2f}%", overallHitRatio)
          << std::endl;

      if (FLAGS_report_api_latency && !aggregated_) {
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
      if (!aggregated_) {
        foreachAC(backgroundEvictionClasses,
                  [&](auto pid, auto cid, auto evicted) {
                    out << folly::sformat("pid{:2} cid{:4} evicted: {:4}", pid,
                                          cid, evicted)
                        << std::endl;
                  });
      }

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
      if (!aggregated_) {
        foreachAC(backgroundPromotionClasses,
                  [&](auto pid, auto cid, auto promoted) {
                    out << folly::sformat("pid{:2} cid{:4} promoted: {:4}", pid,
                                          cid, promoted)
                        << std::endl;
                  });
      }

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

      if (!aggregated_) {
        folly::StringPiece readCat = "NVM Read  Latency";
        folly::StringPiece writeCat = "NVM Write Latency";
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

        folly::StringPiece insertCat = "BlockCache Insert Latency";
        fmtLatency(insertCat, "p50", bcInsertLatencyMicrosP50);
        fmtLatency(insertCat, "p90", bcInsertLatencyMicrosP90);
        fmtLatency(insertCat, "p99", bcInsertLatencyMicrosP99);
        fmtLatency(insertCat, "p999", bcInsertLatencyMicrosP999);
        fmtLatency(insertCat, "p9999", bcInsertLatencyMicrosP9999);
        fmtLatency(insertCat, "p99999", bcInsertLatencyMicrosP99999);
        fmtLatency(insertCat, "p999999", bcInsertLatencyMicrosP999999);
        fmtLatency(insertCat, "p100", bcInsertLatencyMicrosP100);

        folly::StringPiece lookupCat = "BlockCache Lookup Latency";
        fmtLatency(lookupCat, "p50", bcLookupLatencyMicrosP50);
        fmtLatency(lookupCat, "p90", bcLookupLatencyMicrosP90);
        fmtLatency(lookupCat, "p99", bcLookupLatencyMicrosP99);
        fmtLatency(lookupCat, "p999", bcLookupLatencyMicrosP999);
        fmtLatency(lookupCat, "p9999", bcLookupLatencyMicrosP9999);
        fmtLatency(lookupCat, "p99999", bcLookupLatencyMicrosP99999);
        fmtLatency(lookupCat, "p999999", bcLookupLatencyMicrosP999999);
        fmtLatency(lookupCat, "p100", bcLookupLatencyMicrosP100);

        folly::StringPiece removeCat = "BlockCache Remove Latency";
        fmtLatency(removeCat, "p50", bcRemoveLatencyMicrosP50);
        fmtLatency(removeCat, "p90", bcRemoveLatencyMicrosP90);
        fmtLatency(removeCat, "p99", bcRemoveLatencyMicrosP99);
        fmtLatency(removeCat, "p999", bcRemoveLatencyMicrosP999);
        fmtLatency(removeCat, "p9999", bcRemoveLatencyMicrosP9999);
        fmtLatency(removeCat, "p99999", bcRemoveLatencyMicrosP99999);
        fmtLatency(removeCat, "p999999", bcRemoveLatencyMicrosP999999);
        fmtLatency(removeCat, "p100", bcRemoveLatencyMicrosP100);
      }

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

  std::map<std::string, double> getHitRatios(
      const StatsBase& prevStatsBase) const override {
    auto& prevStats = prevStatsBase.as<Stats>();
    std::map<std::string, double> rates;
    rates["overall"] = 0.0;
    rates["ram"] = 0.0;
    rates["nvm"] = 0.0;

    if (numCacheGets > prevStats.numCacheGets) {
      auto totalMisses = getTotalMisses();
      auto prevTotalMisses = prevStats.getTotalMisses();

      rates["overall"] = invertPctFn(totalMisses - prevTotalMisses,
                                     numCacheGets - prevStats.numCacheGets);

      rates["ram"] = invertPctFn(numCacheGetMiss - prevStats.numCacheGetMiss,
                                 numCacheGets - prevStats.numCacheGets);
    }

    if (numNvmGets > prevStats.numNvmGets) {
      rates["nvm"] = invertPctFn(numNvmGetMiss - prevStats.numNvmGetMiss,
                                 numNvmGets - prevStats.numNvmGets);
    }

    return rates;
  }

  // Render the stats based on the delta between overall stats and previous
  // stats. It can be used to render the stats in the last time period.
  void render(const StatsBase& prevStatsBase,
              std::ostream& out) const override {
    auto& prevStats = prevStatsBase.as<Stats>();

    if (numCacheGets > prevStats.numCacheGets) {
      auto rates = getHitRatios(prevStatsBase);
      out << folly::sformat("Cache Gets    : {:,}",
                            numCacheGets - prevStats.numCacheGets)
          << std::endl;
      out << folly::sformat("Hit Ratio     : {:6.2f}%", rates["overall"])
          << std::endl;

      out << folly::sformat(
          "RAM Hit Ratio : {:6.2f}%\n"
          "NVM Hit Ratio : {:6.2f}%\n",
          rates["ram"], rates["nvm"]);
    }
  }

  void render(folly::UserCounters& counters) const override {
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

  bool renderIsTestPassed(std::ostream& out) const override {
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
  bool aggregated_{false};

  static double pctFn(uint64_t ops, uint64_t total) {
    return total == 0
               ? 0
               : 100.0 * static_cast<double>(ops) / static_cast<double>(total);
  }

  static double invertPctFn(uint64_t ops, uint64_t total) {
    return 100 - pctFn(ops, total);
  }
};

class ComponentStats : public StatsBase {
 public:
  interface::CacheComponentStats stats_;

  explicit ComponentStats(interface::CacheComponentStats&& stats)
      : stats_(std::move(stats)) {}

  // Aggregate throughput stats from another instance. DOES NOT HANDLE
  // LATENCY STATS!
  ComponentStats& operator+=(const StatsBase& otherBase) override {
    auto& other = otherBase.as<ComponentStats>();

    auto addOpCounters = [](auto& dst, const auto& src) {
      dst.throughput_.calls_ += src.throughput_.calls_;
      dst.throughput_.successes_ += src.throughput_.successes_;
      dst.throughput_.errors_ += src.throughput_.errors_;
    };

    auto addFindOpCounters = [&addOpCounters](auto& dst, const auto& src) {
      addOpCounters(dst, src);
      dst.throughput_.hits_ += src.throughput_.hits_;
      dst.throughput_.misses_ += src.throughput_.misses_;
    };

    addOpCounters(stats_.allocate_, other.stats_.allocate_);
    addOpCounters(stats_.insert_, other.stats_.insert_);
    addOpCounters(stats_.insertOrReplace_, other.stats_.insertOrReplace_);
    addFindOpCounters(stats_.find_, other.stats_.find_);
    addFindOpCounters(stats_.findToWrite_, other.stats_.findToWrite_);
    addFindOpCounters(stats_.removeByKey_, other.stats_.removeByKey_);
    addOpCounters(stats_.removeByHandle_, other.stats_.removeByHandle_);
    addOpCounters(stats_.writeBack_, other.stats_.writeBack_);
    addOpCounters(stats_.release_, other.stats_.release_);

    stats_.numItems += other.stats_.numItems;

    return *this;
  }

  std::string progress(const StatsBase& prevStatsBase) const override {
    auto& prevStats = prevStatsBase.as<ComponentStats>();
    auto hitRates = getHitRatios(prevStats);
    uint64_t totalOps = getTotalCalls();
    return folly::sformat(
        "{} items in cache. "
        "{:>5} total cache operations (including support ops, e.g., "
        "writeBack()). Hit Ratio {:6.2f}%.",
        stats_.numItems,
        prettyPrintOps(totalOps),
        hitRates["overall"]);
  }

  void render(std::ostream& out) const override {
    out << "== Cache Component Stats ==" << std::endl << stats_;
  }

  void render(const StatsBase& prevStatsBase,
              std::ostream& out) const override {
    auto& prevStats = prevStatsBase.as<ComponentStats>();

    auto printOpDelta = [&out](const char* name, const auto& curr,
                               const auto& prev) {
      auto dCalls = curr.throughput_.calls_ - prev.throughput_.calls_;
      if (dCalls == 0) {
        return;
      }
      auto dSucc = curr.throughput_.successes_ - prev.throughput_.successes_;
      auto dErr = curr.throughput_.errors_ - prev.throughput_.errors_;
      out << folly::sformat("{:15}: calls={:<8} succ={:<8} err={}\n", name,
                            dCalls, dSucc, dErr);
    };

    auto printFindOpDelta = [&out](const char* name, const auto& curr,
                                   const auto& prev) {
      auto dCalls = curr.throughput_.calls_ - prev.throughput_.calls_;
      if (dCalls == 0) {
        return;
      }
      auto dSucc = curr.throughput_.successes_ - prev.throughput_.successes_;
      auto dErr = curr.throughput_.errors_ - prev.throughput_.errors_;
      auto dHits = curr.throughput_.hits_ - prev.throughput_.hits_;
      auto dMiss = curr.throughput_.misses_ - prev.throughput_.misses_;
      out << folly::sformat(
          "{:15}: calls={:<8} succ={:<8} err={:<6} hits={:<8} miss={}\n", name,
          dCalls, dSucc, dErr, dHits, dMiss);
    };

    auto rates = getHitRatios(prevStatsBase);
    out << folly::sformat("Hit Ratio: {:6.2f}%\n", rates["overall"]);
    printOpDelta("allocate", stats_.allocate_, prevStats.stats_.allocate_);
    printOpDelta("insert", stats_.insert_, prevStats.stats_.insert_);
    printOpDelta("insertOrReplace", stats_.insertOrReplace_,
                 prevStats.stats_.insertOrReplace_);
    printFindOpDelta("find", stats_.find_, prevStats.stats_.find_);
    printFindOpDelta("findToWrite", stats_.findToWrite_,
                     prevStats.stats_.findToWrite_);
    printFindOpDelta("removeByKey", stats_.removeByKey_,
                     prevStats.stats_.removeByKey_);
    printOpDelta("removeByHandle", stats_.removeByHandle_,
                 prevStats.stats_.removeByHandle_);
    printOpDelta("writeBack", stats_.writeBack_, prevStats.stats_.writeBack_);
    printOpDelta("release", stats_.release_, prevStats.stats_.release_);
  }

  void render(folly::UserCounters& counters) const override {
    counters["total_calls"] = static_cast<int64_t>(getTotalCalls());
    counters["find_calls"] =
        static_cast<int64_t>(stats_.find_.throughput_.calls_);

    auto hits =
        stats_.find_.throughput_.hits_ + stats_.findToWrite_.throughput_.hits_;
    auto calls = stats_.find_.throughput_.calls_ +
                 stats_.findToWrite_.throughput_.calls_;
    counters["hit_rate"] =
        calls > 0 ? static_cast<int64_t>(hits * 10000 / calls) : 0;
    counters["num_items"] = static_cast<int64_t>(stats_.numItems);
  }

  std::map<std::string, double> getHitRatios(
      const StatsBase& prevStatsBase) const override {
    auto& prevStats = prevStatsBase.as<ComponentStats>();
    std::map<std::string, double> rates;
    rates["overall"] = 0.0;

    uint64_t hits =
        stats_.find_.throughput_.hits_ + stats_.findToWrite_.throughput_.hits_;
    uint64_t calls = stats_.find_.throughput_.calls_ +
                     stats_.findToWrite_.throughput_.calls_;

    uint64_t prevHits = prevStats.stats_.find_.throughput_.hits_ +
                        prevStats.stats_.findToWrite_.throughput_.hits_;
    uint64_t prevCalls = prevStats.stats_.find_.throughput_.calls_ +
                         prevStats.stats_.findToWrite_.throughput_.calls_;

    uint64_t deltaHits = hits - prevHits;
    uint64_t deltaTotal = calls - prevCalls;
    if (deltaTotal > 0) {
      rates["overall"] = 100.0 * static_cast<double>(deltaHits) /
                         static_cast<double>(deltaTotal);
    }

    return rates;
  }

  bool renderIsTestPassed(std::ostream& /* out */) const override {
    // ComponentStats doesn't track error conditions that would fail a test
    return true;
  }

 private:
  // Pretty-print a number with K/M/G/T suffix, keeping to ~3 characters
  static std::string prettyPrintOps(uint64_t ops) {
    if (ops < 1000) {
      return std::to_string(ops);
    } else if (ops < 1000000) {
      return std::to_string(ops / 1000) + "K";
    } else if (ops < 1000000000) {
      return std::to_string(ops / 1000000) + "M";
    } else if (ops < 1000000000000ULL) {
      return std::to_string(ops / 1000000000) + "G";
    } else {
      return std::to_string(ops / 1000000000000ULL) + "T";
    }
  }

  uint64_t getTotalCalls() const {
    return stats_.allocate_.throughput_.calls_ +
           stats_.insert_.throughput_.calls_ +
           stats_.insertOrReplace_.throughput_.calls_ +
           stats_.find_.throughput_.calls_ +
           stats_.findToWrite_.throughput_.calls_ +
           stats_.removeByKey_.throughput_.calls_ +
           stats_.removeByHandle_.throughput_.calls_ +
           stats_.writeBack_.throughput_.calls_ +
           stats_.release_.throughput_.calls_;
  }
};

} // namespace cachebench
} // namespace cachelib
} // namespace facebook

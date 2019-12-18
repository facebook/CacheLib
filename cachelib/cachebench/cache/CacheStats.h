#pragma once
namespace facebook {
namespace cachelib {
namespace cachebench {
struct Stats {
  uint64_t numEvictions{0};
  uint64_t numItems{0};

  uint64_t allocAttempts{0};
  uint64_t allocFailures{0};

  uint64_t numCacheGets{0};
  uint64_t numCacheGetMiss{0};
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
  uint64_t numNvmEvictions{0};
  uint64_t numNvmBytesWritten{0};
  uint64_t numNvmNandBytesWritten{0};
  uint64_t numNvmLogicalBytesWritten{0};
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

  uint64_t slabsReleased{0};
  uint64_t moveAttemptsForSlabRelease{0};
  uint64_t moveSuccessesForSlabRelease{0};
  uint64_t evictionAttemptsForSlabRelease{0};
  uint64_t evictionSuccessesForSlabRelease{0};

  uint64_t inconsistencyCount{0};
  bool isNvmCacheDisabled{false};

  void render(std::ostream& out) const {
    auto pctFn = [](uint64_t ops, uint64_t total) {
      return total == 0 ? 100.0
                        : 100.0 * static_cast<double>(ops) /
                              static_cast<double>(total);
    };

    auto invertPctFn = [&pctFn](uint64_t ops, uint64_t total) {
      return 100 - pctFn(ops, total);
    };

    const uint64_t totalMisses =
        numNvmGets > 0 ? numNvmGetMiss : numCacheGetMiss;
    const double overallHitRatio = invertPctFn(totalMisses, numCacheGets);
    out << folly::sformat("Items in RAM  : {:,}", numItems) << std::endl;
    out << folly::sformat("Items in NVM  : {:,}", numNvmItems) << std::endl;

    out << folly::sformat("Alloc Attempts: {:,} Success: {:.2f}%",
                          allocAttempts,
                          invertPctFn(allocFailures, allocAttempts))
        << std::endl;
    out << folly::sformat("RAM Evictions : {:,}", numEvictions) << std::endl;

    if (numCacheGets > 0) {
      out << folly::sformat("Cache Gets    : {:,}", numCacheGets) << std::endl;
      out << folly::sformat("Hit Ratio     : {:6.2f}%", overallHitRatio)
          << std::endl;
    }

    if (numNvmGets > 0 || numNvmDeletes > 0 || numNvmPuts > 0) {
      const double ramHitRatio = invertPctFn(numCacheGetMiss, numCacheGets);
      const double nvmHitRatio = invertPctFn(numNvmGetMiss, numNvmGets);

      out << folly::sformat(
          "RAM Hit Ratio : {:6.2f}%\n"
          "NVM Hit Ratio : {:6.2f}%\n",
          ramHitRatio, nvmHitRatio);

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
    out << folly::sformat("{:14}: {:15,}", "NVM Deletes", numNvmDeletes)
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
  }
}; // namespace cachebench

} // namespace cachebench
} // namespace cachelib
} // namespace facebook

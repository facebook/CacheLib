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

#include <folly/io/async/EventBase.h>
#include <folly/logging/LoggerDB.h>
#include <gflags/gflags.h>

#include <filesystem>
#include <memory>
#include <thread>
#include <vector>

#include "cachelib/cachebench/runner/Runner.h"
#include "cachelib/cachebench/util/AggregateStats.h"
#include "cachelib/cachebench/util/Sleep.h"
#include "cachelib/common/Utils.h"

#ifdef CACHEBENCH_FB_ENV
#include "cachelib/cachebench/facebook/FbDep.h"
#include "cachelib/cachebench/facebook/fb303/FB303ThriftServer.h"
#include "cachelib/facebook/odsl_exporter/OdslExporter.h"
#include "common/init/Init.h"
#else
#include <folly/init/Init.h>
#include <gflags/gflags.h>
#endif

#ifdef CACHEBENCH_FB_ENV
DEFINE_bool(export_to_ods, false, "Upload cachelib stats to ODS");
DEFINE_int32(fb303_port,
             0,
             "Port for cachebench fb303 service. If 0, do not export to fb303. "
             "If valid, this will disable ODSL export.");
#endif
DEFINE_string(json_test_config,
              "",
              "path to test config. If empty, use default setting");
DEFINE_uint64(
    progress,
    60,
    "if set, prints progress every X seconds as configured, 0 to disable");
DEFINE_string(progress_stats_file,
              "",
              "Print detailed stats at each progress interval to this file");
DEFINE_int32(timeout_seconds,
             0,
             "Maximum allowed seconds for running test. 0 means no timeout");

struct sigaction act;
std::vector<facebook::cachelib::cachebench::Runner> runnerInstances;
std::unique_ptr<std::thread> stopperThread;

void sigint_handler(int sig_num) {
  switch (sig_num) {
  case SIGINT:
  case SIGTERM: {
    for (auto& runner : runnerInstances) {
      runner.abort();
    }
    break;
  }
  }
}

void setupSignalHandler() {
  memset(&act, 0, sizeof(struct sigaction));
  act.sa_handler = &sigint_handler;
  act.sa_flags = SA_RESETHAND;
  if (sigaction(SIGINT, &act, nullptr) == -1 ||
      sigaction(SIGTERM, &act, nullptr) == -1) {
    std::cout << "Failed to register SIGINT&SIGTERM handler" << std::endl;
    std::exit(1);
  }
}

void setupTimeoutHandler() {
  if (FLAGS_timeout_seconds > 0) {
    stopperThread = std::make_unique<std::thread>([] {
      folly::EventBase eb;
      eb.runAfterDelay(
          [&eb]() {
            XLOGF(INFO,
                  "Stopping due to timeout {} seconds",
                  FLAGS_timeout_seconds);
            for (auto& runner : runnerInstances) {
              runner.abort();
            }
            eb.terminateLoopSoon();
          },
          FLAGS_timeout_seconds * 1000);
      eb.loopForever();
      // We give another few seconds for the graceful shutdown to complete
      eb.runAfterDelay([]() { XCHECK(false); }, 30 * 1000);
      eb.loopForever();
    });
    stopperThread->detach();
  }
}

std::optional<facebook::cachelib::cachebench::CacheBenchConfig>
checkArgsValidity(
    const facebook::cachelib::cachebench::CacheConfigCustomizer& c = {},
    const facebook::cachelib::cachebench::StressorConfigCustomizer& s = {}) {
  if (FLAGS_json_test_config.empty() ||
      !facebook::cachelib::util::pathExists(FLAGS_json_test_config)) {
    std::cout << "Invalid config file: " << FLAGS_json_test_config
              << ". pass a valid --json_test_config for cachebench."
              << std::endl;
    return std::nullopt;
  }
  facebook::cachelib::cachebench::CacheBenchConfig config(
      FLAGS_json_test_config, c, s);
  // Enforce non-empty progress_stats_file when running multiple instances to
  // avoid garbled output
  if (config.getNumInstances() > 1 && FLAGS_progress_stats_file.empty()) {
    std::cout << "Error: --progress_stats_file must be specified when "
                 "running multiple instances (numInstances="
              << config.getNumInstances() << ")" << std::endl;
    return std::nullopt;
  }

  return config;
}

// Generate a unique progress stats file path for a given instance.
// Appends "_N" before the file extension.
std::string getProgressStatsFileForInstance(size_t instanceIdx) {
  namespace fs = std::filesystem;
  fs::path p(FLAGS_progress_stats_file);
  fs::path stem = p.stem();
  fs::path ext = p.extension();
  fs::path parent = p.parent_path();

  std::string newFilename =
      stem.string() + "_" + std::to_string(instanceIdx) + ext.string();
  return (parent / newFilename).string();
}

bool runAllRunners(std::chrono::seconds progress) {
  std::cout << "Running " << runnerInstances.size() << " cachebench instance(s)"
            << std::endl;

  // Start each Runner in a separate thread since run() is blocking
  std::vector<std::thread> threads;
  std::vector<bool> results(runnerInstances.size(), false);
  threads.reserve(runnerInstances.size());

  for (uint32_t i = 0; i < runnerInstances.size(); ++i) {
    threads.emplace_back([=, &results]() {
      std::string progressFile = getProgressStatsFileForInstance(i);
      results[i] = runnerInstances[i].run(
          progress, progressFile, /* alsoPrintResultsToConsole */ false);
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  std::cout << facebook::cachelib::cachebench::AggregatedStats(runnerInstances);

  return std::ranges::all_of(results, [](bool result) { return result; });
}

int main(int argc, char** argv) {
  using namespace facebook::cachelib;
  using namespace facebook::cachelib::cachebench;

  calibrateSleep();
#ifdef CACHEBENCH_FB_ENV
  facebook::initFacebook(&argc, &argv);
  auto config = checkArgsValidity(customizeCacheConfigForFacebook,
                                  customizeStressorConfigForFacebook);
  if (!config) {
    return 1;
  }

  std::unique_ptr<util::OdslExporter> odslExporter_;
  std::unique_ptr<FB303ThriftService> fb303_;
  if (FLAGS_fb303_port == 0 && FLAGS_export_to_ods) {
    odslExporter_ = std::make_unique<util::OdslExporter>(kCachebenchCacheName);
  } else if (FLAGS_fb303_port > 0) {
    fb303_ = std::make_unique<FB303ThriftService>(FLAGS_fb303_port);
  }

  std::cout << "Welcome to FB-internal version of cachebench" << std::endl;
#else
  const folly::Init init(&argc, &argv, true);
  auto config = checkArgsValidity();
  if (!config) {
    return 1;
  }

  std::cout << "Welcome to OSS version of cachebench" << std::endl;
#endif

  try {
    const size_t numInstances = config->getNumInstances();
    runnerInstances.reserve(numInstances);
    for (size_t i = 0; i < numInstances; ++i) {
      runnerInstances.emplace_back(i, *config);
    }

    setupSignalHandler();
    setupTimeoutHandler();

    std::chrono::seconds progress(FLAGS_progress);
    auto allPassed =
        numInstances == 1
            ? runnerInstances[0].run(progress, FLAGS_progress_stats_file)
            : runAllRunners(progress);

    std::cout << (allPassed ? "Finished!" : "Failed.") << std::endl;
    return allPassed ? 0 : 1;
  } catch (const std::exception& e) {
    std::cout << "Invalid configuration. Exception: " << e.what() << std::endl;
    return 1;
  }
}

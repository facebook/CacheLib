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

#include <memory>
#include <thread>

#include "cachelib/cachebench/workload/KVReplayGenerator.h"
#include "cachelib/common/Utils.h"

#include <folly/init/Init.h>
#include <gflags/gflags.h>

DEFINE_string(json_test_config,
              "",
              "path to test config. If empty, use default setting");
DEFINE_uint64(
    progress,
    60,
    "if set, prints progress every X seconds as configured, 0 to disable");


bool checkArgsValidity() {
  if (FLAGS_json_test_config.empty() ||
      !facebook::cachelib::util::pathExists(FLAGS_json_test_config)) {
    std::cout << "Invalid config file: " << FLAGS_json_test_config
              << ". pass a valid --json_test_config for trace generation."
              << std::endl;
    return false;
  }

  return true;
}

int main(int argc, char** argv) {
  using namespace facebook::cachelib;
  using namespace facebook::cachelib::cachebench;

  folly::init(&argc, &argv, true);
  if (!checkArgsValidity()) {
    return 1;
  }

  CacheBenchConfig config(FLAGS_json_test_config);
  std::cout << "Binary Trace Generator" << std::endl;

  try {
    auto generator = 
        std::make_unique<KVReplayGenerator>(config.getStressorConfig());
  } catch (const std::exception& e) {
    std::cout << "Invalid configuration. Exception: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}

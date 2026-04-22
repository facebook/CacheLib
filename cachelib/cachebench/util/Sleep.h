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

#include <chrono>

namespace facebook::cachelib::cachebench {

/**
 * Calibrate the high-precision sleep function. This makes busy-waiting for
 * short intervals more precise.
 *
 * NOTE: users should call this function before using highPrecisionSleep()!
 */
void calibrateSleep();

/**
 * Sleep for the given duration.  This function is more precise than traditional
 * sleep methods for small (sub-millisecond) durations because it busy waits
 * rather than calling into the OS to sleep.  This is good for benchmarks but
 * shouldn't be used in prod.  Be warned: this will consume CPU!
 *
 * NOTE: users should call calibrateSleep() before using this function!
 *
 * @param duration the duration to sleep
 */
void highPrecisionSleep(std::chrono::nanoseconds ns);

} // namespace facebook::cachelib::cachebench

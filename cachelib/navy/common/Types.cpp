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

#include "cachelib/navy/common/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {
const char* toString(Status status) {
  switch (status) {
  case Status::Ok:
    return "Ok";
  case Status::NotFound:
    return "NotFound";
  case Status::Rejected:
    return "Rejected";
  case Status::Retry:
    return "Retry";
  case Status::DeviceError:
    return "DeviceError";
  case Status::BadState:
    return "BadState";
  }
  return "Unknown";
}

const char* toString(DestructorEvent e) {
  switch (e) {
  case DestructorEvent::Recycled:
    return "Recycled";
  case DestructorEvent::Removed:
    return "Removed";
  case DestructorEvent::PutFailed:
    return "PutFailed";
  }
  return "Unknown";
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# - Find liburing
#
# uring_INCLUDE_DIR - Where to find liburing.h
# uring_LIBRARIES - List of libraries when using uring.
# uring_FOUND - True if uring found.

find_path(uring_INCLUDE_DIR
  NAMES liburing.h)
find_library(uring_LIBRARIES
  NAMES liburing.a liburing)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(uring
  DEFAULT_MSG uring_LIBRARIES uring_INCLUDE_DIR)

mark_as_advanced(
  uring_INCLUDE_DIR
  uring_LIBRARIES)

if(uring_FOUND AND NOT TARGET uring::uring)
  add_library(uring::uring UNKNOWN IMPORTED)
  set_target_properties(uring::uring PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${uring_INCLUDE_DIR}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${uring_LIBRARIES}")
endif()

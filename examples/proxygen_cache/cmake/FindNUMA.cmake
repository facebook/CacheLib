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

# - Find NUMA
# Find the NUMA library and includes
#
# NUMA_INCLUDE_DIRS - where to find numa.h, etc.
# NUMA_LIBRARIES - List of libraries when using NUMA.
# NUMA_FOUND - True if NUMA found.

message(STATUS "looking numa in dir: : ${NUMA_INCLUDE_DIRS}")
message(STATUS "root: : ${NUMA_ROOT_DIR}")

find_path(NUMA_INCLUDE_DIRS
  NAMES numa.h numaif.h
  HINTS ${NUMA_ROOT_DIR}/include)

message(STATUS "root: : ${NUMA_ROOT_DIR}")

message(STATUS "looking numa in dir again : ${NUMA_INCLUDE_DIRS}")

find_library(NUMA_LIBRARIES
  NAMES numa
  HINTS ${NUMA_ROOT_DIR}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(NUMA DEFAULT_MSG NUMA_LIBRARIES NUMA_INCLUDE_DIRS)

mark_as_advanced(
  NUMA_LIBRARIES
  NUMA_INCLUDE_DIRS)

if(NUMA_FOUND AND NOT (TARGET NUMA::NUMA))
  add_library (NUMA::NUMA UNKNOWN IMPORTED)
  set_target_properties(NUMA::NUMA
    PROPERTIES
      IMPORTED_LOCATION ${NUMA_LIBRARIES}
      INTERFACE_INCLUDE_DIRECTORIES ${NUMA_INCLUDE_DIRS})
endif()

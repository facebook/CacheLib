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

# - Try to find Glog
# Once done, this will define
#
# GLOG_FOUND - system has Glog
# GLOG_INCLUDE_DIRS - the Glog include directories
# GLOG_LIBRARIES - link these to use Glog

include(FindPackageHandleStandardArgs)
find_path(
  ZBD_INCLUDE_DIRS zbd.h
  HINTS
      $ENV{ZBD_ROOT}/include
      ${ZBD_ROOT}/include
)

find_library(
    ZBD_LIBRARIES zbd
    HINTS
        $ENV{ZBD_ROOT}/lib
        ${ZBD_ROOT}/lib
)

# For some reason ZBD_FOUND is never marked as TRUE
set(ZBD_FOUND TRUE)
mark_as_advanced(ZBD_INCLUDE_DIRS ZBD_LIBRARIES)

find_package_handle_standard_args(zbd ZBD_INCLUDE_DIRS ZBD_LIBRARIES)

if(ZBD_FOUND AND NOT ZBD_FIND_QUIETLY)
    message(STATUS "ZBD: ${ZBD_INCLUDE_DIRS}")
endif()

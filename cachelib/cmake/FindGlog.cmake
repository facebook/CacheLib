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
# glog::glog - imported target, preferred over the variables above

include(FindPackageHandleStandardArgs)

# glog >= 0.7 only compiles when consumers see its exported target, which
# carries the GLOG_USE_GLOG_EXPORT definition. Prefer the upstream config and
# derive this module's documented variables from it; GLOG_LIBRARIES names the
# target so that linking through the variable still picks up the definition.
find_package(glog CONFIG QUIET)
if (TARGET glog::glog)
  get_target_property(GLOG_INCLUDE_DIR glog::glog INTERFACE_INCLUDE_DIRECTORIES)
  set(GLOG_INCLUDE_DIRS ${GLOG_INCLUDE_DIR})
  set(GLOG_LIBRARIES glog::glog)
  set(GLOG_FOUND TRUE)
  return()
endif()

find_library(GLOG_LIBRARY glog
  PATHS ${GLOG_LIBRARYDIR})

find_path(GLOG_INCLUDE_DIR glog/logging.h
  PATHS ${GLOG_INCLUDEDIR})

find_package_handle_standard_args(glog DEFAULT_MSG
  GLOG_LIBRARY
  GLOG_INCLUDE_DIR)

mark_as_advanced(
  GLOG_LIBRARY
  GLOG_INCLUDE_DIR)

set(GLOG_LIBRARIES ${GLOG_LIBRARY})
set(GLOG_INCLUDE_DIRS ${GLOG_INCLUDE_DIR})

if (NOT TARGET glog::glog)
  add_library(glog::glog UNKNOWN IMPORTED)
  set_target_properties(glog::glog PROPERTIES INTERFACE_INCLUDE_DIRECTORIES "${GLOG_INCLUDE_DIRS}")
  set_target_properties(glog::glog PROPERTIES IMPORTED_LINK_INTERFACE_LANGUAGES "C" IMPORTED_LOCATION "${GLOG_LIBRARIES}")
endif()

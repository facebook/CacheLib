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

cmake_minimum_required (VERSION 3.12)

project(proxygen_cache VERSION 0.1)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake/")

find_package(cachelib CONFIG REQUIRED)


find_package(proxygen CONFIG REQUIRED)
find_package(GTest  CONFIG REQUIRED)         # GTest::gtest, GTest::gmock
find_package(NUMA   REQUIRED)         # NUMA::NUMA
find_package(glog REQUIRED)         # Glog::glog

add_executable(proxygen_cache main.cpp)

if (TARGET cachelib)
  get_target_property(_cachelib_inc cachelib INTERFACE_INCLUDE_DIRECTORIES)
  # If it ends in /include/cachelib, step up one level
  foreach(dir IN LISTS _cachelib_inc)
    if(dir MATCHES ".*/include/cachelib$")
      get_filename_component(PARENT_DIR "${dir}" DIRECTORY)
      target_include_directories(proxygen_cache PRIVATE "${PARENT_DIR}")
    else()
      target_include_directories(proxygen_cache PRIVATE "${dir}")
    endif()
  endforeach()
endif()

target_link_libraries(proxygen_cache PUBLIC
  cachelib
  GTest::gmock
  glog::glog
  NUMA::NUMA
  proxygen::proxygen
  proxygen::proxygenhttpserver
)

get_target_property(_inc cachelib INTERFACE_INCLUDE_DIRECTORIES)
message(STATUS "cachelib include dirs: ${_inc}")

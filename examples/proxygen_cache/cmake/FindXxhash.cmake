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


find_path(Xxhash_INCLUDE_DIR
  NAMES xxhash.h)
find_library(Xxhash_LIBRARIES
  NAMES libxxhash.a libxxhash)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Xxhash
  DEFAULT_MSG Xxhash_LIBRARIES Xxhash_INCLUDE_DIR)

mark_as_advanced(
  Xxhash_INCLUDE_DIR
  Xxhash_LIBRARIES)

if(Xxhash_FOUND AND NOT TARGET Xxhash::Xxhash)
  add_library(Xxhash::Xxhash UNKNOWN IMPORTED)
  set_target_properties(Xxhash::Xxhash PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${Xxhash_INCLUDE_DIR}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${Xxhash_LIBRARIES}")
endif()

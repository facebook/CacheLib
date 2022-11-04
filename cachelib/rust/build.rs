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

// Need to replace this with a CMake build, using https://github.com/XiangpengHao/cxx-cmake-example as inspiration

fn main() {
    cxx_build::bridge("src/lib.rs") // returns a cc::Build
        .file("src/cachelib.cpp")
        .flag_if_supported("-std=c++11")
        .compile("cachelib");

    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=src/cachelib.cpp");
    println!("cargo:rerun-if-changed=src/cachelib.h");
}

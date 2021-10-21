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

cmake_minimum_required(VERSION 3.4)

find_package(cachelib REQUIRED)
add_library(rocksdb_cachelib STATIC CachelibWrapper.cpp)
target_link_libraries(rocksdb_cachelib PRIVATE cachelib)
target_include_directories(rocksdb_cachelib PRIVATE ${CACHELIB_INCLUDE_DIR})

# Suppresses errors from folly exceptions
target_compile_options(rocksdb_cachelib PRIVATE -Wno-error=class-memaccess)
set(cachelib_LIBS rocksdb_cachelib PARENT_SCOPE)
set(cachelib_TARGETS rocksdb_cachelib PARENT_SCOPE)
set(cachelib_FUNC register_CachelibObjects PARENT_SCOPE)

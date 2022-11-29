cmake_minimum_required(VERSION 3.4)

find_package(cachelib REQUIRED)

# Suppresses errors from folly exceptions
set(cachelib_SOURCES CachelibWrapper.cpp PARENT_SCOPE)
set(cachelib_INCLUDE_PATHS ${CACHELIB_INCLUDE_DIR}/.. PARENT_SCOPE)
set(cachelib_COMPILE_FLAGS -Wno-error=class-memaccess -Wno-error=sign-compare -Wno-error=deprecated-declarationsPARENT_SCOPE)
set(cachelib_LIBS "cachelib_allocator" "cachelib_shm" "cachelib_navy" "cachelib_common" "thriftcpp2" "thriftfrozen2" "thriftmetadata" "thriftanyrep" "thrifttype" "thrifttyperep" "thriftannotation" "thriftprotocol" "async" "wangle" "fizz" "sodium" "rt" PARENT_SCOPE)
set(cachelib_FUNC register_CachelibObjects PARENT_SCOPE)
set(cachelib_LINK_PATHS ${CMAKE_PREFIX_PATH}/lib PARENT_SCOPE)
set(cachelib_TESTS tests/CachelibWrapperTest.cpp PARENT_SCOPE)

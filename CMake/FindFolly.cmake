# https://github.com/bro/cmake/blob/master/FindJeMalloc.cmake
# - Try to find google test headers and libraries.
#
# Usage of this module as follows:
#
#     find_package(Folly)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  FOLLY_ROOT_DIR Set this variable to the root installation of
#                    folly if the module has problems finding
#                    the proper installation path.
#
# Variables defined by this module:
#
#  FOLLY_FOUND             System has folly libs/headers
#  FOLLY_LIBRARIES         The folly library/libraries
#  FOLLY_INCLUDE_DIR       The location of folly headers

find_path(FOLLY_ROOT_DIR
    NAMES include/folly/folly-config.h
)

find_library(FOLLY_LIBRARIES
    NAMES folly
    HINTS ${FOLLY_ROOT_DIR}/lib
)

find_library(FOLLY_BENCHMARK_LIBRARIES
    NAMES follybenchmark
    HINTS ${FOLLY_ROOT_DIR}/lib
)

find_path(FOLLY_INCLUDE_DIR
    NAMES folly/folly-config.h
    HINTS ${FOLLY_ROOT_DIR}/include
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Folly DEFAULT_MSG
    FOLLY_LIBRARIES
    FOLLY_INCLUDE_DIR
)

mark_as_advanced(
    FOLLY_ROOT_DIR
    FOLLY_LIBRARIES
    FOLLY_BENCHMARK_LIBRARIES
    FOLLY_INCLUDE_DIR
)

#
# - Try to find Facebook folly library
# This will define
# FOLLY_FOUND
# FOLLY_INCLUDE_DIR
# FOLLY_LIBRARIES
#

find_package(DoubleConversion REQUIRED)
find_package(Glog REQUIRED)
find_package(Boost REQUIRED filesystem regex context program_options)
if(Boost_FOUND)
      include_directories(${Boost_INCLUDE_DIRS})
endif()

find_path(
        FOLLY_INCLUDE_DIR
        NAMES "folly/String.h"
        HINTS
        "/usr/local/facebook/include"
)

find_library(
        FOLLY_LIBRARY
        NAMES folly
        HINTS
        "/usr/local/facebook/lib"
)

set(FOLLY_LIBRARIES ${FOLLY_LIBRARY} ${DOUBLE_CONVERSION_LIBRARY} ${LIBGLOG_LIBRARIES} ${LIBIBERTY_LIBRARY} ${CMAKE_DL_LIBS} Threads::Threads ${LIBGFLAGS_LIBRARY} Boost::filesystem Boost::regex Boost::context Boost::system Boost::program_options ${fmt_LIBRARY} ${LIBEVENT_LIB})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
        FOLLY DEFAULT_MSG FOLLY_INCLUDE_DIR FOLLY_LIBRARIES)

mark_as_advanced(FOLLY_INCLUDE_DIR FOLLY_LIBRARIES FOLLY_FOUND)

if(FOLLY_FOUND AND NOT FOLLY_FIND_QUIETLY)
    message(STATUS "FOLLY: ${FOLLY_INCLUDE_DIR}")
endif()

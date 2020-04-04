#
# - Try to find Facebook fbthrift library
# This will define
# FBTHRIFT_FOUND
# FBTHRIFT_INCLUDE_DIR
# FBTHRIFT_LIBRARIES
#

find_package(OpenSSL REQUIRED)

find_path(
    FBTHRIFT_INCLUDE_DIR
    NAMES "thrift/lib/cpp2/Thrift.h"
    HINTS
        "/usr/local/include"
)
find_file(
    FBTHRIFT_LIBRARY_FILE
    NAMES "ThriftLibrary.cmake"
    HINTS
        "/usr/local/include/thrift"
)

find_file(
    THRIFT1
    NAMES "thrift1"
    HINTS
        "/usr/local/bin"
)

find_library(
    FBTHRIFT_CORE_LIBRARY
    NAMES thrift-core
    HINTS
        "/usr/local/lib"
)

find_library(
    THRIFTCPP2 
    NAMES thriftcpp2
    HINTS
        "/usr/local/lib"
)

find_library(
    FBTHRIFT_PROTOCOL_LIBRARY
    NAMES thriftprotocol
    HINTS
        "/usr/local/lib"
)
find_library(
    FBTHRIFT_PROTOCOL_LIBRARY_IMPL
    NAMES protocol
    HINTS
        "/usr/local/lib"
)

find_library(
    FBTHRIFT_FROZEN2_LIBRARY
    NAMES thriftfrozen2
    HINTS
        "/usr/local/lib"
)

find_library(
    FBTHRIFT_METADATA_LIBRARY
    NAMES thriftmetadata
    HINTS
        "/usr/local/lib"
)

find_library(
    FBTHRIFT_TRANSPORT_LIBRARY
    NAMES transport
    HINTS
        "/usr/local/lib"
)
set(FBTHRIFT_LIBRARIES
    ${FBTHRIFT_CORE_LIBRARY}
    ${THRIFTCPP2}
    ${FBTHRIFT_FROZEN2_LIBRARY}
    ${OPENSSL_LIBRARIES}
    ${FBTHRIFT_METADATA_LIBRARY}
    ${FBTHRIFT_PROTOCOL_LIBRARY}
    ${FBTHRIFT_PROTOCOL_LIBRARY_IMPL}
    ${FBTHRIFT_TRANSPORT_LIBRARY}


)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
    FBTHRIFT DEFAULT_MSG FBTHRIFT_INCLUDE_DIR FBTHRIFT_LIBRARIES)
include(${FBTHRIFT_LIBRARY_FILE})
mark_as_advanced(FBTHRIFT_INCLUDE_DIR FBTHRIFT_LIBRARIES FBTHRIFT_FOUND FBTHRIFT_LIBRARY_FILE)

if(FBTHRIFT_FOUND AND NOT FBTHRIFT_FIND_QUIETLY)
    message(STATUS "FBTHRIFT: ${FBTHRIFT_INCLUDE_DIR}")
    message(STATUS "FBTHRIFT: ${THRIFTCPP2}")

endif()

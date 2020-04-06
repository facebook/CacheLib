project (cachelot_benchmarks)

cmake_minimum_required (VERSION 2.8.3)
set (CMAKE_CXX_STANDARD 17)

set (BOOST_MIN_VERSION, "1.53.0")
set (USED_COMPONENTS unit_test_framework system program_options)
set (Boost_USE_MULTITHREADED ON)
set (Boost_USE_STATIC_LIBS ON)
add_definitions (-DBOOST_ALL_NO_LIB) # disable boost auto-linking feature as it does not work with all compilers
find_package (Boost ${BOOST_MIN_VERSION} COMPONENTS ${USED_COMPONENTS} REQUIRED)
include_directories (${INCLUDE_DIRECTORIES} ${Boost_INCLUDE_DIRS})



find_library(CACHELOT_LIB cachelot HINTS /home/daniel/cachelot/lib/Release)


###########################################################################
#       Platform setup -- required!
###########################################################################
if (CMAKE_SIZEOF_VOID_P EQUAL 4)
  set (CACHELOT_PLATFORM_BITS 32)
  add_definitions ("-DCACHELOT_PLATFORM_BITS=32")
elseif (CMAKE_SIZEOF_VOID_P EQUAL 8)
  set (CACHELOT_PLATFORM_BITS 64)
  add_definitions ("-DCACHELOT_PLATFORM_BITS=64")
else ()
  message (FATAL_ERROR "CMake reports unsupported pointer size: \"${CMAKE_SIZEOF_VOID_P}\"")
endif ()
message (STATUS "Platform: ${CMAKE_SYSTEM_NAME} x${CACHELOT_PLATFORM_BITS}")
if (${CMAKE_SYSTEM_NAME} MATCHES "Windows")
    add_definition (-D_WIN32_WINNT=0x05010000) # set NTDDI_WINXP as a target win version
endif ()
string (REGEX MATCH "arm.*" ARM_CPU "${CMAKE_SYSTEM_PROCESSOR}")
string (REGEX MATCH "x86_64" x86_64_CPU "${CMAKE_SYSTEM_PROCESSOR}")


###########################################################################
#       config.h -- required!
###########################################################################
include (CheckCXXSymbolExists)
check_cxx_symbol_exists (aligned_alloc stdlib.h HAVE_ALIGNED_ALLOC)
check_cxx_symbol_exists (posix_memalign stdlib.h HAVE_POSIX_MEMALIGN)
configure_file ("/home/daniel/cachelot/src/cachelot/config.h.in" "/home/daniel/cachelot/src/cachelot/config.h")
add_definitions (-DHAVE_CONFIG_H=1)


### Cache benchmark
add_executable(cachelot_test cachelot.cpp)

include_directories("/home/daniel/cachelot/src/")
#target_link_directories("/home/daniel/cachelot/lib/Release")



target_link_libraries (cachelot_test ${Boost_LIBRARIES} ${CACHELOT_LIB})

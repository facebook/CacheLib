add_library(
  cachelib_common
  ApproxSplitSet.h
  AtomicCounter.h
  BytesEqual.h
  Cohort.h
  CompilerUtils.h
  CountMinSketch.h
  Exceptions.h
  FastStats.h
  Hash.h
  Iterators.h
  MurmurHash.h
  Mutex.h
  PercentileStats.h
  PeriodicWorker.h
  Serialization.h
  TestUtils.h
  Throttler.h
  Time.h
  Utils.h
  Cohort.cpp
  CountMinSketch.cpp
  FurcHash.cpp
  PercentileStats.cpp
  PeriodicWorker.cpp
  Serialization.cpp
  TestUtils.cpp
  Utils.cpp
)

target_link_libraries(
  cachelib_common
  PUBLIC
    ${FOLLY_LIBRARIES}
)

#https://github.com/facebook/fbthrift/blob/master/ThriftLibrary.cmake lines 88 to 121


add_subdirectory(tests)

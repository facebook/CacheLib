#!/bin/bash
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

if [[ -z "$GITHUB_STEP_SUMMARY" ]]; then
  echo "run_tests.sh is for use by CI (selected tests, timeout)."
  echo "Users should go to opt/cachelib/tests and run make."
  echo
fi

# Optional (e.g., flaky) tests. Issues a warning instead of an error.
OPTIONAL=()
OPTIONAL+=("allocator-test-AllocationClassTest")  # Ubuntu 18 (segfault)
OPTIONAL+=("allocator-test-AllocatorResizeTypeTest")  # Rocky 8.6
OPTIONAL+=("allocator-test-AllocatorTypeTest")  # CentOS 8.5, Rocky 8.6
OPTIONAL+=("allocator-test-BlockCacheTest")  # Rocky 8.6
OPTIONAL+=("allocator-test-MemoryAllocatorTest")  # Ubuntu 18 (segfault)
OPTIONAL+=("allocator-test-MM2QTest")  # Ubuntu 18 (segfault)
# CentOS 8.1, CentOS 8.5, Debian, Fedora 36, Rocky 9, Rocky 8.6, Ubuntu 18
OPTIONAL+=("allocator-test-NavySetupTest")
OPTIONAL+=("allocator-test-SlabAllocatorTest")  # Ubuntu 18
# CentOS 8.1, CentOS 8.5, Debian, Fedora 36, Rocky 9, Rocky 8.6, Ubuntu 18
OPTIONAL+=("common-test-UtilTests")
OPTIONAL+=("datatype-test-MapTest")  # Ubuntu 20
# CentOS 8.1, Rocky 9, Ubuntu 18
OPTIONAL+=("navy-test-BlockCacheTest")
# CentOS 8.1, CentOS 8.5, Debian, Fedora 36, Rocky 9, Ubuntu 18, Ubuntu 20, Ubuntu 22
OPTIONAL+=("navy-test-DriverTest")  
# CentOS 8.5, Rocky 9, Ubuntu 20
OPTIONAL+=("navy-test-MockJobSchedulerTest")
# CentOS 8.1, CentOS 8.5, Debian, Fedora 36, Rocky 9, Rocky 8.6, Ubuntu 18, Ubuntu 20, Ubuntu 22
# Large pages need to be enabled
OPTIONAL+=("shm-test-test_page_size")  

# Skip long-running benchmarks.
TO_SKIP=()
# TO_SKIP+=("allocator-test-AllocatorTypeTest")  # 12 mins.
TO_SKIP+=("benchmark-test-CompactCacheBench")  # 26 mins.
TO_SKIP+=("benchmark-test-MutexBench")  # 60 mins.

TEST_TIMEOUT=30m
BENCHMARK_TIMEOUT=20m
PARALLELISM=10

OPTIONAL_LIST=$(printf  -- '%s\n' ${OPTIONAL[@]})
TO_SKIP_LIST=$(printf  -- '%s\n' ${TO_SKIP[@]})

MD_OUT=${GITHUB_STEP_SUMMARY:-$PWD/summary.md}
if [[ "$MD_OUT" != "$GITHUB_STEP_SUMMARY" ]]; then
  echo "Markdown summary will be saved in $MD_OUT. Truncating it."
  echo
  echo "Time started: $(date)" > $MD_OUT
fi

echo "See Summary page of job for a table of test results and log excerpts"
echo 


dir=$(dirname "$0")
cd "$dir/.." || die "failed to change-dir into $dir/.."
test -d cachelib || die "failed to change-dir to expected root directory"

PREFIX="$PWD/opt/cachelib"
LD_LIBRARY_PATH="$PREFIX/lib:${LD_LIBRARY_PATH:-}"
export LD_LIBRARY_PATH

echo "LD_LIBRARY_PATH: $LD_LIBRARY_PATH"

cd opt/cachelib/tests || die "failed to change-dir into opt/cachelib/tests"

TESTS_TO_RUN=$(find * -type f -not -name "*bench*" -executable \
  | grep -vF "$TO_SKIP_LIST" \
  | awk ' { print $1 ".log" } ')
N_TESTS=$(echo $TESTS_TO_RUN | wc -w)

echo
echo "::group::Running tests for CI (total: $N_TESTS, max: $TEST_TIMEOUT)"
timeout --preserve-status $TEST_TIMEOUT make -j $PARALLELISM -s $TESTS_TO_RUN
echo "::endgroup::"
echo "Successful tests: $(find -name '*.ok' | wc -l)"
echo "Failed tests: $(find -name '*.fail' | wc -l)"
echo

BENCHMARKS_TO_RUN=$(find * -type f -name "*bench*" -executable \
  | grep -vF "$TO_SKIP_LIST" \
  | awk ' { print $1 ".log" } ')
N_BENCHMARKS=$(echo $BENCHMARKS_TO_RUN | wc -w)

echo "::group::Running benchmarks for CI (total: $N_BENCHMARKS, max: $BENCHMARK_TIMEOUT)"
timeout --preserve-status $BENCHMARK_TIMEOUT make -j $PARALLELISM -s $BENCHMARKS_TO_RUN
echo "::endgroup::"
echo "Successful benchmarks: $(find -name '*bench*.ok' | wc -l)"
echo "Failed benchmarks: $(find -name '*bench*.fail' | wc -l)"

TESTS_PASSED=$(find * -name '*.log.ok' | sed 's/\.log\.ok$//')
TESTS_FAILED=$(find * -name '*.log.fail' | sed 's/\.log\.fail$//')
TESTS_TIMEOUT=$(find * -type f -executable \
  | grep -vF "$TESTS_PASSED" \
  | grep -vF "$TESTS_FAILED" \
  | grep -vF "$TO_SKIP_LIST")
TESTS_IGNORED=$(echo "$TESTS_FAILED" | grep -F "$OPTIONAL_LIST")
FAILURES_UNIGNORED=$(echo "$TESTS_FAILED" | grep -vF "$OPTIONAL_LIST")

N_TIMEOUT=$(echo $TESTS_TIMEOUT | wc -w)
N_PASSED=$(echo $TESTS_PASSED | wc -w)
N_FAILED=$(echo $TESTS_FAILED | wc -w)
N_IGNORED=$(echo $TESTS_IGNORED | wc -w)
N_FAILURES_UNIGNORED=$(echo $FAILURES_UNIGNORED | wc -w)
N_SKIPPED=$(echo $TO_SKIP_LIST | wc -w)

echo "## Test summary" >> $MD_OUT
echo "|Workflow| Passed | Failed | Ignored | Timeout | Skipped" >> $MD_OUT
echo "|--------|--------|--------|---------|---------|---------|" >> $MD_OUT
echo "| $GITHUB_JOB | $N_PASSED | $N_FAILED | $N_IGNORED | $N_TIMEOUT | $N_SKIPPED |" >> $MD_OUT

STATUS=0

if [[ $N_FAILED -ne 0 ]]; then
  if [[ $N_IGNORED -ne 0 ]]; then
    echo
    echo "::group::Ignored test failures "
    echo "$TESTS_IGNORED"
    echo "::endgroup::"
    echo "::warning ::$N_IGNORED tests/benchmarks failed and ignored."

    echo >> $MD_OUT
    echo "## Ignored test failures" >> $MD_OUT
    echo "$TESTS_IGNORED" | awk ' { print "1. " $1 } ' >> $MD_OUT
  fi 

  if [ $N_FAILURES_UNIGNORED -eq 0 ]; then
    echo "Only ignored tests failed."
  else
    STATUS=1
    echo
    echo "== Failing tests =="
    echo "$FAILURES_UNIGNORED"

    echo >> $MD_OUT
    echo "## Failing tests" >> $MD_OUT
    echo "$FAILURES_UNIGNORED" | awk ' { print "1. " $1 } ' >> $MD_OUT

    echo "::error ::$N_FAILURES_UNIGNORED tests/benchmarks failed."
  fi

  echo
  echo "::group::Failed tests"
  grep "Segmentation fault" *.log || true
  grep "FAILED.*ms" *.log || true
  echo "::endgroup::"

  echo >> $MD_OUT
  echo "## Failures summary" >> $MD_OUT
  echo "\`\`\`" >> $MD_OUT
  grep "Segmentation fault" *.log >> $MD_OUT || true
  grep "FAILED.*ms" *.log >> $MD_OUT || true
  echo "\`\`\`" >> $MD_OUT

  echo
  echo "=== Failure logs with context ==="
  echo >> $MD_OUT
  echo "### Failure logs with context" >> $MD_OUT
  for faillog in *.log.fail; do
    logfile="${faillog/\.fail/}"
    echo
    echo "::group::Logs:$logfile"
    grep -Pazo "(?s)\[ RUN[^\[]+\[  FAILED[^\n]+ms\)\n" $logfile
    grep "Segmentation fault" -B 3 $logfile
    echo "::endgroup::"

    echo "#### $logfile" >> $MD_OUT
    echo "\`\`\`" >> $MD_OUT
    grep -Pazo "(?s)\[ RUN[^\[]+\[  FAILED[^\n]+ms\)\n" $logfile \
      | sed 's/\x0/---------------\n/g' >> $MD_OUT
    grep "Segmentation fault" -B 3 $logfile >> $MD_OUT
    echo "\`\`\`" >> $MD_OUT
    echo >> $MD_OUT
  done    
else
  echo
  echo "All tests passed."
fi

echo
echo "::group::Skipped tests"
echo "$TO_SKIP_LIST"
echo "::endgroup::"

echo >> $MD_OUT
echo "## Skipped tests" >> $MD_OUT
echo "$TO_SKIP_LIST" | awk ' { print "1. " $1 } ' >> $MD_OUT

if [[ $N_TIMEOUT -ne 0 ]]; then
  echo
  echo "::error ::$N_TIMEOUT tests exceeded time limit." \
    " Consider adding them to TO_SKIP or increasing TEST_TIMEOUT/BENCHMARK_TIMEOUT."
  echo "::group::Timed out tests"
  echo "$TESTS_TIMEOUT"
  echo "::endgroup::"

  echo "## Tests timed out" >> $MD_OUT
  echo "$TESTS_TIMEOUT" | awk ' { print "1. " $1 } ' >> $MD_OUT
fi

# Comment out if you do not want (unignored) failing tests to fail the build
exit $STATUS

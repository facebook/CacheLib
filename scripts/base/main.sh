#!/usr/bin/env bash

workloadName=$1
tracePath=$2;
numOps=$(< "$tracePath" wc -l)

startSize=256
stepSize=256
endSize=8192

for (( CACHE_SIZE=$startSize; CACHE_SIZE<=$endSize; CACHE_SIZE+=$stepSize )); do
  output_path="./config/${workloadName}_${CACHE_SIZE}.json"
  python3 make_config.py $tracePath $CACHE_SIZE $numOps $output_path
  ../../opt/cachelib/bin/cachebench --json_test_config $output_path
done

#!/usr/bin/env bash

workloadName=$1 # name used to store results 
tracePath=$2; # the path to the block trace to replay 
numOps=$(< "$tracePath" wc -l) # number of operations cannot be determined by the block trace ? 

startSize=$3 # start cache size 
stepSize=$4 # step size
endSize=$5 # end cache size 

for (( CACHE_SIZE=$startSize; CACHE_SIZE<=$endSize; CACHE_SIZE+=$stepSize )); do
  config_dir="./config/base/${workloadName}"
  mkdir -p $config_dir
  config_path="${config_dir}/${CACHE_SIZE}.json"

  out_dir="./out/base/${workloadName}"
  mkdir -p $out_dir
  output_path="${out_dir}/${CACHE_SIZE}"
  
  python3 make_config.py $tracePath $CACHE_SIZE $numOps $config_path
  ../../opt/cachelib/bin/cachebench --json_test_config $config_path >> $output_path
done

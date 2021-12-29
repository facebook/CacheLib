#!/bin/bash

workload_name=$1 
t1_size=$2
t2_size=$3 
it=$4

csv_trace_dir=/home/pranav/csv_traces
csv_trace_path=$csv_trace_dir/${workload_name}.csv

output_path=./output 
output_dir=$output_path/$workload_name
mkdir -p $output_dir 
json_config_path=cur-config.json 

python3 make_config.py --p $csv_trace_path --s1 $t1_size --s2 $t2_size --lba 0 

log_output_path=$output_dir/${workload_name}_${t1_size}_${t2_size}_${it}.log
stat_output_path=$output_dir/${workload_name}_${t1_size}_${t2_size}_${it}.stat
err_output_path=$output_dir/${workload_name}_${t1_size}_${t2_size}_${it}.err
mem_output_path=$output_dir/${workload_name}_${t1_size}_${t2_size}_${it}.mem

# echo $json_config_path, $log_output_path, $stat_output_path, $err_output_path

../../../opt/cachelib/bin/cachebench --json_test_config $json_config_path --progress_stats_file $stat_output_path 2>> $err_output_path 1>> $log_output_path &
pid=$! 

while true;
do
    mem=$(ps -p "$pid" -o %mem)
    set -- $mem
    memory="$2"
    echo "$memory" >> $mem_output_path
    sleep 60
done


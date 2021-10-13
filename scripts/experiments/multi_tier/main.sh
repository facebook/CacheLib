workload_name=$1
trace_path=$2
experiment_name=$3
OUTPUT_DIR="./output/"

num_ops=$(cat ../../data/block_read_write_stats.csv | grep w54 | awk -F "\"*,\"*" '{ print $3 }')

total_ws_4kb=$(cat ../../data/block_read_write_stats.csv | grep w54 | awk -F "\"*,\"*" '{ print $6 }')
total_ws_mb=$(($total_ws_4kb/256))

T1_START_SIZE=1000 # 1 GB
T1_STEP_SIZE=1000 # 1 GB
T1_END_SIZE=12000 # 12 GB

T2_STEP_SIZE=2000 # 2 GB
T2_END_SIZE=0


for (( t1_cache_size=T1_START_SIZE; t1_cache_size<=T1_END_SIZE; t1_cache_size+=T1_STEP_SIZE ))
do  
  for (( t2_cache_size=0; t2_cache_size<=T2_END_SIZE; t2_cache_size+=T2_STEP_SIZE ))
  do  
    echo "T1 $t1_cache_size MB - T2 $t2_cache_size MB"

    # create the necessary config file 
    python3 make_config.py --p $trace_path --s1 $t1_cache_size --s2 $t2_cache_size --n $num_ops

    # create the output directory 


    # run the cache and store the output in the output directory 

  done
done




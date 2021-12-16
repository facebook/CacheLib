experiment_name=$1
workload_name=$2
trace_path=$3
disk_file_path=$4
cache_list_file=$5

OUTPUT_DIR="./output/$experiment_name"
mkdir -p $OUTPUT_DIR

min_lba=$(cat ../../data/workload_min_lba.csv | grep $workload_name | awk -F "\"*,\"*" '{ print $2 }')

while read c; do
  t1_size=$(echo $c | awk -F, '{print $1}')
  t2_size=$(echo $c | awk -F, '{print $2}')
  OUTPUT_FILE="${workload_name}_${t1_size}_${t2_size}"
  python3 make_config.py --p $trace_path --d $disk_file_path --s1 $t1_size --s2 $t2_size --lba $min_lba 
  ../../../opt/cachelib/bin/cachebench --json_test_config cur-config.json >> $OUTPUT_DIR/$OUTPUT_FILE
done <$cache_list_file

t1_size=$(echo $c | awk -F, '{print $1}')
t2_size=$(echo $c | awk -F, '{print $2}')
OUTPUT_FILE="${workload_name}_${t1_size}_${t2_size}"
python3 make_config.py --p $trace_path --d $disk_file_path --s1 $t1_size --s2 $t2_size --lba $min_lba
../../../opt/cachelib/bin/cachebench --json_test_config cur-config.json >> $OUTPUT_DIR/$OUTPUT_FILE






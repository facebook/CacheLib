#! /bin/bash
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


input_logfile=$1
if [[ $# -ne 1 ]] || [[ ! -s "$input_logfile" ]] || [[ ! -f "$input_logfile" ]]; then
    echo "USAGE: $0 LOGFILE"
    echo "produces a tsv containing nvm read/write latencies from LOGFILE"
    echo "if gnuplot is installed, will produce a plot of these latencies"
    echo "LOGFILE should be logging output of cachebench with --progress-stats_file"
    exit 1
fi


percentiles="p50us p90us p99us p999us p9999us p99999us p999999us p100us"
# grep for the lines, convert sequential lines into columns
#   NVM Read Latency p50     : 201.39 us
#   NVM Read Latency p90     : 449.96 us
#   NVM Read Latency p99     : 1054.32 us
#   NVM Read Latency p999    : 2426.05 us
#   NVM Read Latency p9999   : 4632.00 us
#   NVM Read Latency p99999  : 5533.74 us
#   NVM Read Latency p999999 : 5533.74 us
#   NVM Read Latency p100    : 9111.00 us
#
#   becomes
#   1 201.39   449.96   1054.32 .... 9111.00
extract_latency() {
    local in=$3
    local out=$2
    local search=$1

    {
        echo "time $percentiles"
        # get the latency and translate from one per line into 8 columns
        # use sequence number of line as time stamp
        grep -e "$search" "$in"             | \
            awk '{print $6}'                | \
            perl -pe 's/\n/ / if $. % 8'    | \
            awk '{print NR" "$0}'
    }  | column -t > "$out"

    # check if the file actually produced valid content
    [[ "$(wc -l < "$out")" -eq "1" ]] &&                        \
        echo "Incorrect log file. No latecny records found" ||  \
        echo "$search latency written to $out"
}

bandwidth="egress_gb_s ingress_gb_s"
# grep for the last line, convert egressBytesPerSec/ingressBytesPerSec into columns
# egressBytes : 4573.78 GB, ingressBytes: 771.18 GB, egressBytesPerSec :   9.53 GB/s, ingressBytesPerSec:   1.61 GB/s, ingressEgressratio:  83.14%
#
#   becomes
#   9.53   1.61
extract_bandwidth() {
    local in=$3
    local out=$2
    local search=$1

    {
        echo "$bandwidth"
        # get the bandwidth
        grep -e "$search" "$in"             | \
            tail -1                         | \
            awk '{print $10 " " $13}'
    }  | column -t > "$out"

    # check if the file actually produced valid content
    [[ "$(wc -l < "$out")" -eq "1" ]] &&                        \
        echo "Incorrect log file. No bandwidth records found" ||  \
        echo "Bandwidth written to $out"
}

extract_median() {
    local in="$1"
    local column="$2"
    local cnt
    cnt=$(wc -l "$in" | cut -f 1 -d " ")
    local median=$(("$cnt"/2))
    # get the median line's value
    awk -v col="$column" '{print $col}' "$in" | grep -E  '^[0-9]' | sort -n |\
        head -n "$median" | tail -n 1
}

print_median_percentile() {
    local tsv=$1
    local i=2
    local type="$2"
    echo -n "$type "
    for _ in $percentiles; do
        pct_val=$(extract_median "$tsv" $i)
        echo -n "$pct_val "
        i=$((i+1))
    done
    echo
}


out_dir=$(dirname "$input_logfile")
filename=$(basename "$input_logfile")

# location of the intermediate tsv
read_tsv="$out_dir/${filename%%.*}_read_latency.tsv"
write_tsv="$out_dir/${filename%%.*}_write_latency.tsv"
bandwidth_tsv="$out_dir/${filename%%.*}_bandwidth.tsv"

extract_latency "NVM Write Latency" "$write_tsv"  "$input_logfile"
extract_latency "NVM Read  Latency" "$read_tsv"  "$input_logfile"
extract_bandwidth "egressBytesPerSec" "$bandwidth_tsv"  "$input_logfile"

# location of the final latency charts
read_png="$out_dir/${filename%%.*}_read_latency.png"
write_png="$out_dir/${filename%%.*}_write_latency.png"
plot_file="$(dirname "$0")/gnuplot_latency.plt"

plot_png() {
    gnuplot                     \
        -e "tsv='$1'"           \
        -e "out_file='$2'"      \
        -e "chart_title='$3'"   \
        "$4"                &&  \
            echo "Plotted $3 in $2" || \
            echo "Failed to plot $3"
}

plot_png "$read_tsv" "$read_png" "Read Latency" "$plot_file"
plot_png "$write_tsv" "$write_png" "Write Latency" "$plot_file"

echo -e "\n\n======= Median Latency ======= "
{
    echo "OpType $percentiles"
    print_median_percentile "$read_tsv" "read"
    print_median_percentile "$write_tsv" "write"
} | column -t

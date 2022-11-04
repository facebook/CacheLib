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

set terminal png size 4000, 2000

# out_file is provided as input
set output out_file

# chart_title is provided as input
set multiplot layout 4,2 title chart_title font ",20

set grid
set autoscale
set format x ''
set key font ",16"

# pick up column names from first line
set key center top autotitle columnhead

# tsv is the input file  containing data
# p50
plot tsv using 1:2 with lines

# p90
plot tsv using 1:3 with lines

# p99
plot tsv using 1:4 with lines

# p999
plot tsv using 1:5 with lines

# p9994
plot tsv using 1:6 with lines

# p99999
plot tsv using 1:7 with lines

# p999999
plot tsv using 1:8 with lines

# p100
plot tsv using 1:9 with lines

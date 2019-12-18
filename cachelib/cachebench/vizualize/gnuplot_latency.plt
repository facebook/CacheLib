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

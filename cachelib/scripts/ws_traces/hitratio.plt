set terminal png size 2000, 2000

# out_file is provided as input
set output "ws_hit_ratios.png"

# chart_title is provided as input
set multiplot layout 4,3 title "%HDD-IOPS saved vs Cache Size" font ",20"

set grid
set autoscale

set key font ",10"

# pick up column names from first line
set key bottom right autotitle columnhead

list = "cerium cesium chlorine chromium cobalt curium radium samarium scandium selenium silicon strontium"
do for [file in list] {
    set title file font ",16"

    # use the arrow to mark the current HW config
    set arrow from 600, graph 0 to 600, graph 1 nohead filled back lw 3 lc rgb "coral"

    # plot the three graphs. The last one adds a legend for the arrow we drew
    # above
    plot file.".txt" every ::0::7 using 1:2 with linespoints pointtype 7 linewidth 3 title "LIRS",      \
        file.".txt" every ::0::7 using 1:3 with linespoints pointtype 7 linewidth 3 title "Prob-LRU",   \
        file.".txt" every ::0::7 using 1:4 with linespoints pointtype 7 linewidth 3 title "Rej1-LRU", \
        1/0 with lines linewidth 3 linecolor rgb "coral" title "Current HW"
}

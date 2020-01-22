from __future__ import absolute_import, division, print_function, unicode_literals

import sys

import cachelib.scripts.ws_traces.cache as cache
import cachelib.scripts.ws_traces.utils as utils
import matplotlib.pyplot as plt
import numpy


def plot_graph(bins, v, out_file_name):
    print("Plotting to ", out_file_name)
    plt.xlabel("Reuse Distance in Secs")
    plt.ylabel("% of Reuse")
    plt.xscale("log")
    name = out_file_name.split("/")[-1].split(".")[0]
    plt.title("Distribution of reuse distance for {}".format(name))
    plt.plot(
        bins,
        v,
        alpha=1.0,
        aa=True,
        marker="*",
        drawstyle="default",
        linewidth=2,
        markevery=None,
    )
    plt.savefig(out_file_name, dpi=400, quality=95)

    return


def compute_reuse_distance(a):
    # filter out keys that had no reuse
    reused = dict(filter(lambda v: len(v[1].accesses) > 1, a.items()))
    pct_filtered = int(round((len(a) - len(reused)) * 100.0 / len(a)))
    print("{}% keys out of {} had no reuse".format(pct_filtered, len(a)))

    stats = {}
    for k, v in reused.items():
        stats[k] = cache.CacheStats()
        stats[k].sim_cache(v.accesses)

    reuse = []
    for _, s in stats.items():
        reuse.extend(s.reuse_dist)

    return reuse


tracefile = sys.argv[1]
input_file_name = tracefile[: -len(".trace")].split("/")[-1]
out_file_name = sys.argv[2] + input_file_name + "_reuse_hist.png"
a, _, _ = utils.read_processed_file(tracefile)
reuse_dist = compute_reuse_distance(a)

b = [1, 10, 60, 300, 600, 3600, 7200, max(reuse_dist)]

hist, bins = numpy.histogram(reuse_dist, bins=b, density=False)
print(hist)

sum_access = sum(hist)
v = [x / sum_access for x in numpy.cumsum(hist)]

print(v, bins[1:])

plot_graph(bins[1:], v, out_file_name)

from __future__ import absolute_import, division, print_function, unicode_literals

import sys

import cachelib.scripts.ws_traces.cache as cache
import cachelib.scripts.ws_traces.utils as utils
import matplotlib.pyplot as plt


def plot_graph(key_cum, hit_cum, out_file_name):
    print("Plotting to ", out_file_name)
    plt.xlabel("% of keys")
    plt.ylabel("% of hits")
    name = out_file_name.split("/")[-1].split(".")[0]
    plt.title("keys vs hits for {}".format(name))
    # plt.xscale('log')
    plt.plot(
        key_cum,
        hit_cum,
        alpha=1.0,
        aa=True,
        marker="*",
        drawstyle="default",
        linewidth=2,
        markevery=None,
    )
    plt.savefig(out_file_name, dpi=400, quality=95)


def compute_hits(a):
    # filter out keys that had no reuse
    stats = {}
    for k, v in a.items():
        stats[k] = cache.CacheStats()
        stats[k].sim_cache(v.accesses)

    hit_cnts = []
    for _, v in stats.items():
        hit_cnts.append(v.numHits())

    return hit_cnts


tracefile = sys.argv[1]
input_file_name = tracefile[: -len(".trace")].split("/")[-1]
out_file_name = sys.argv[2] + input_file_name + "_hits_pct.png"

a, _, _ = utils.read_processed_file(tracefile)


hit_cnts = compute_hits(a)
hit_cnts.sort(reverse=True)

all_hits = sum(hit_cnts)
all_keys = len(hit_cnts)

every = 0.05
hit_cum = []
key_cum = []
key_cnt = 0
hit_cnt = 0
prev_pct = 0

for h in hit_cnts:
    key_cnt += 1
    hit_cnt += h

    key_pct = key_cnt / all_keys
    if key_pct < prev_pct + every:
        continue

    key_cum.append(key_pct)
    hit_cum.append(hit_cnt / all_hits)
    prev_pct = key_pct

print(hit_cum)
print(key_cum)

plot_graph(key_cum, hit_cum, out_file_name)

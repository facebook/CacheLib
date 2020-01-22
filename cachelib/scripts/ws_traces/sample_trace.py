from __future__ import absolute_import, division, print_function, unicode_literals

import sys

import cachelib.scripts.ws_traces.utils as utils


tracefile = sys.argv[1]
sample_rate = float(sys.argv[2])
print("sampling {} at {}%".format(tracefile, sample_rate))
out_file = (
    tracefile[: -len(".trace")].replace("raw", "processed", 1)
    + "_"
    + str(sample_rate)
    + ".trace"
)

users_out_file = (
    tracefile[: -len(".trace")].replace("raw", "processed", 1)
    + "_"
    + str(sample_rate)
    + ".users"
)

namespaces_out_file = (
    tracefile[: -len(".trace")].replace("raw", "processed", 1)
    + "_"
    + str(sample_rate)
    + ".namespaces"
)

pipelines_out_file = (
    tracefile[: -len(".trace")].replace("raw", "processed", 1)
    + "_"
    + str(sample_rate)
    + ".pipelines"
)


accesses, users, namespaces, pipelines = utils.read_tracefile(tracefile, sample_rate)
utils.write_access_to_file(out_file, accesses)
if users and len(users):
    utils.write_feature_encoding_to_file(users_out_file, users)

if namespaces and len(namespaces):
    utils.write_feature_encoding_to_file(namespaces_out_file, namespaces)

if pipelines and len(pipelines):
    utils.write_feature_encoding_to_file(pipelines_out_file, pipelines)

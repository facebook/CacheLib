#!/usr/bin/python3

import json
import subprocess
import re
import io
import csv

config = {
  "cache_config": {
    "cacheSizeMB": 8192, 
    "poolRebalanceIntervalSec": 0
  }, 
  "test_config": 
    {
      "addChainedRatio": 0.0, 
      "delRatio": 0.0, 
      "enableLookaside": True, 
      "getRatio": 0.9746082749830369, 
      "keySizeRange": [
        1, 
        8, 
        64
      ], 
      "keySizeRangeProbability": [
        0.3, 
        0.7
      ], 
      "loneGetRatio": 0.025391725016963105, 
      "numKeys": 23458726,
      "numOps": 5000000, 
      "numThreads": 1,
      "popDistFile": "assoc_altoona1_follower_pop.json", 
      "prepopulateCache": True, 
      "setRatio": 0.0, 
      "valSizeDistFile": "assoc_altoona1_follower_sizes.json"
    }
 
}

outfile = open('run.csv', 'w')
writer = csv.writer(outfile)


# need repetitions
# different cache configs
# different working set sizes

for rep in range(0,10):
    for sys in ["assoc","fbobj"]:
        for num_keys in [2345872,23458726,234587260]:
            for threads in [1,2,4,8,16]:
                config["test_config"]["popDistFile"] = sys+"_altoona1_follower_pop.json"
                config["test_config"]["valSizeDistFile"] = sys+"_altoona1_follower_sizes.json"
                config["test_config"]["numThreads"] = threads
                config["test_config"]["numKeys"] = num_keys
                config["test_config"]["numOps"] = 300000000

                res = [
                    sys,
                    config["test_config"]["numThreads"],
                    config["test_config"]["numKeys"],
                    config["test_config"]["numOps"],
                    rep
                ]
                
                with open("run.json","w") as jfile:
                    json.dump(config, jfile, indent=2)
                
                #sudo /home/daniel/cachelib/build/bin/cachebench_bin --json_test_config ./fbobj_altoona2_follower_model_hr.json
                
                p = subprocess.Popen(["sudo","/home/daniel/cachelib/build/bin/cachebench_bin","--json_test_config","./run.json"], stdout=subprocess.PIPE)
            
                warmup = True
                for line in io.TextIOWrapper(p.stdout, encoding="utf-8"):
                    if "Test Results" in line:
                        warmup = False
                    if not warmup:
                        if "Hit Ratio" in line:
                            match = re.search("[0-9.]+(?=%)", line)
                            if match:
                                res.append(float(match.group().replace(',', '')))
                        if re.search("get|set|del", line):
                            match = re.search("[0-9,]+(?=/)", line)
                            if match:
                                res.append(int(match.group().replace(',', '')))
                print(res)
                writer.writerow(res)


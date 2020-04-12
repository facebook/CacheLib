#!/usr/bin/python3

import json
import subprocess
import re
import io
import csv
import socket

def parseRegex(regex, line):
    match = re.search(regex, line)
    if match:
        return match.group().replace(',', '')


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
        "numOps": 20000,
      "numThreads": 1,
      "popDistFile": "assoc_altoona1_follower_pop.json", 
      "prepopulateCache": True, 
      "setRatio": 0.0, 
      "valSizeDistFile": "assoc_altoona1_follower_sizes.json"
    }
 
}



fn_log = "run_"+socket.gethostname()+".csv"
fn_conf = "run_"+socket.gethostname()+".json"
err_log = "run_"+socket.gethostname()+"err.log"
print("outputfiles:",fn_log,fn_conf,err_log)
outfile = open(fn_log, 'w')
writer = csv.writer(outfile)
errfile = open(err_log, 'w')


# need repetitions
# different cache configs
# different working set sizes

for rep in range(0,10):
    for num_keys in [23458726]: #2345872,
        for threads in [32,64]: #1,2,4,8,16,64]:
            for sys in ["assoc"]:  #,"fbobj"
                for cache in ["memcached"]: #"cachelib"
                    config["test_config"]["popDistFile"] = sys+"_altoona1_follower_pop.json"
                    config["test_config"]["valSizeDistFile"] = sys+"_altoona1_follower_sizes.json"
                    config["test_config"]["numThreads"] = threads
                    config["test_config"]["numKeys"] = num_keys
                    config["test_config"]["numOps"] = 250000000
                    if cache=="memcached":
                        config["test_config"]["mode"] = "micro"
                    else:
                        if "mode" in config["test_config"]:
                            del config["test_config"]["mode"]
    
                    res = [
                        sys,
                        cache,
                        config["test_config"]["numThreads"],
                        config["test_config"]["numKeys"],
                        config["test_config"]["numOps"],
                        rep,
                        -1, #6 hr
                        -1, #7 gets
                        -1, #8 sets
                        -1  #9 dels
                    ]
                    
                    with open(fn_conf,"w") as jfile:
                        json.dump(config, jfile, indent=2)
                    
                    #sudo /home/daniel/cachelib/build/bin/cachebench_bin --json_test_config ./fbobj_altoona2_follower_model_hr.json
                    
                    p = subprocess.Popen(["/proj/robinhood-PG0/cachelib/build/bin/cachebench_bin","--json_test_config","./"+fn_conf], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                
                    warmup = True
                    resultsout = ""
                    for line in io.TextIOWrapper(p.stdout, encoding="utf-8"):
                        if "Test Results" in line:
                            warmup = False
                        if not warmup:
                            resultsout += line
                            if "get" in line:
                                # hr
                                hr = parseRegex("[0-9.]+(?=%)", line)
                                if hr:
                                    res[6] = float(hr)
                                # gets
                                gets = parseRegex("[0-9,]+(?=/)", line)
                                if gets:
                                    res[7] = int(gets)
                            if "set" in line:
                                # sets
                                sets = parseRegex("[0-9,]+(?=/)", line)
                                if sets:
                                    res[8] = int(sets)
                            if "del" in line:
                                # dels
                                dels = parseRegex("[0-9,]+(?=/)", line)
                                if dels:
                                    res[9] = int(dels)

                    if res[6]==-1 or res[7]==-1:
                        # wasn't able to parse
                        errfile.write(",".join(str(item) for item in res))
                        errfile.write(resultsout)
                        errfile.write("n".join(str(line) for line in p.stderr))
                        errfile.flush()

                    print(res)
                    writer.writerow(res)
                    outfile.flush()
outfile.close()
errfile.close()

import argparse 
import json 
import pathlib

NUM_THREADS = 1
TRACE_BLOCK_SIZE = 512 # bytes 
PAGE_SIZE = 4096
OUTPUT_JSON_FILE = "cur-config.json"

def main(trace_path, disk_file_path, t1_size, t2_size, min_lba):
  config_json = {
    "cache_config": {
      "cacheSizeMB": t1_size
    },
    "test_config":
      {
        "enableLookaside": "true",
        "generator": "block-replay",
        "numThreads": NUM_THREADS,
        "traceFilePath": str(trace_path.resolve()),
        "traceBlockSize": TRACE_BLOCK_SIZE,
        "diskFilePath": str(disk_file_path.resolve()),
        "pageSize": PAGE_SIZE,
        "minLBA": min_lba
      }
  }

  if t2_size > 0:
    config_json["cache_config"]["nvmCacheSizeMB"] = t2_size
    config_json["cache_config"]["nvmCachePaths"] = ["/dev/nvme0n1"] 
    
  with open(OUTPUT_JSON_FILE, 'w+') as fp:
      json.dump(config_json, fp)


if __name__ == "__main__":
  argparse = argparse.ArgumentParser(description="Generate a cachelib config file from input.")
  argparse.add_argument("--p", type=pathlib.Path, help="path to trace")
  argparse.add_argument("--d", type=pathlib.Path, help="path to disk file")
  argparse.add_argument("--s1", type=int, help="size of DRAM cache")
  argparse.add_argument("--s2", type=int, help="size of NVM cache")
  argparse.add_argument("--lba", type=int, help="minimum value of lba")
  args = argparse.parse_args()

  main(args.p, args.d, args.s1, args.s2, args.lba)

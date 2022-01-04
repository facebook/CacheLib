import argparse 
import json 
import pathlib

NUM_THREADS = 1 # only single threaded to read trace file for replay 
DEFAULT_TRACE_BLOCK_SIZE = 512 # bytes 
DEFAULT_PAGE_SIZE = 4096 # bytes 
DEFAULT_OUTPUT_PATH = "cur-config.json"
DEFAULT_FLASH_PATH = "/flash/cache.file"
DEFAULT_DISK_PATH = "/disk/disk.file"
ALLOC_SIZE = DEFAULT_PAGE_SIZE + 39 + 1 # 8B key+31B metadata=39 bytes per item

def main(trace_path, disk_file_path, t1_size, t2_size, min_lba):
  config_json = {
    "cache_config": {
      "cacheSizeMB": t1_size,
      "allocSizes": [ALLOC_SIZE],
    },
    "test_config":
      {
        "enableLookaside": "true",
        "generator": "block-replay",
        "numThreads": NUM_THREADS,
        "traceFilePath": str(trace_path.resolve()),
        "traceBlockSize": DEFAULT_TRACE_BLOCK_SIZE,
        "diskFilePath": str(disk_file_path.resolve()),
        "pageSize": DEFAULT_PAGE_SIZE,
        "minLBA": min_lba
      }
  }

  if t2_size > 0:
    config_json["cache_config"]["nvmCacheSizeMB"] = t2_size
    config_json["cache_config"]["nvmCachePaths"] = [DEFAULT_FLASH_PATH]
    config_json["cache_config"]["navyBigHashSizePct"] = 0
    config_json["cache_config"]["navySizeClasses"] = []
    config_json["cache_config"]["truncateItemToOriginalAllocSizeInNvm"] = True 
    
  with open(DEFAULT_OUTPUT_PATH, 'w+') as fp:
      json.dump(config_json, fp, indent=4)


if __name__ == "__main__":
  argparse = argparse.ArgumentParser(description="Generate a cachelib config file from input.")
  argparse.add_argument("--p", type=pathlib.Path, help="path to trace")
  argparse.add_argument("--d", type=pathlib.Path, default=DEFAULT_DISK_PATH,
    help="path to disk file")
  argparse.add_argument("--s1", type=int, help="size of DRAM cache")
  argparse.add_argument("--s2", type=int, help="size of NVM cache")
  argparse.add_argument("--lba", type=int, help="minimum value of lba")
  args = argparse.parse_args()

  main(args.p, args.d, args.s1, args.s2, args.lba)

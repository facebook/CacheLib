import argparse 
import json 
import pathlib


def main(trace_path, t1_size, t2_size, num_ops, output_path):
  config_json = {
    "cache_config": {
      "cacheSizeMB": t1_size
    },
    "test_config":
      {
        "enableLookaside": "true",
        "generator": "twitter-replay",
        "numOps": num_ops,
        "numThreads": 1,
        "traceFileName": str(trace_path.resolve())
      }
  }

  if t2_size > 0:
    config_json["cache_config"]["nvmCacheSizeMB"] = t2_size
    config_json["cache_config"]["nvmCachePaths"] = ["/dev/nvme0n1"] 
    config_json["cache_config"]["navyBlockSize"] = 4096
    config_json["cache_config"]["navySizeClasses"] =  []
    

  with open(output_path, 'w+') as fp:
      json.dump(config_json, fp)


if __name__ == "__main__":
  argparse = argparse.ArgumentParser(description="Generate a cachelib config file from input.")
  argparse.add_argument("--p", type=pathlib.Path, help="path to twitter trace")
  argparse.add_argument("--s1", type=int, help="size of DRAM cache")
  argparse.add_argument("--s2", type=int, help="size of NVM cache")
  argparse.add_argument("--n", type=int, help="number of operations to replay")
  argparse.add_argument("--o", help="output path")
  args = argparse.parse_args()

  main(args.p, args.s1, args.s2, args.n, args.o)

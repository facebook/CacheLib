import argparse 
import json 
import pathlib

def main(trace_path, size, num_ops, output_path):
  config_json = {
    "cache_config": {
      "cacheSizeMB": size
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

  with open(output_path, 'w+') as fp:
      json.dump(config_json, fp)



if __name__ == "__main__":
  argparse = argparse.ArgumentParser(description="Generate a cachelib config file from input.")
  argparse.add_argument("t", type=pathlib.Path, help="path to the twitter trace file")
  argparse.add_argument("s", help="size of the cache")
  argparse.add_argument("n", help="number of operations to replay")
  argparse.add_argument("o", help="output path")
  args = argparse.parse_args()

  main(args.t, args.s, args.n, args.o)

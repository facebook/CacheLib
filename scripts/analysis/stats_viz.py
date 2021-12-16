import pathlib 
import pandas as pd 

STATS_FILE_PATH = "../data/block_read_write_stats.csv"

if __name__ == "__main__":
  df = pd.read_csv(STATS_FILE_PATH)

  print(df.sort_values(by='total_count', ascending=True))

  print(df.sort_values(by='total_ws', ascending=False))

  print(df.sort_values(by='read_ws', ascending=False))

  


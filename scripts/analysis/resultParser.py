import json 

class ResultParser:
  def __init__(self, result_file_path):
    self.path = result_file_path 
    self.handle = open(result_file_path, "r")
    self.data = {}
    self.process()

  def process(self):
    for line in self.handle:
      line = line.rstrip()
      split_line = line.split(":")
      val = split_line[-1]

      if "Items in RAM" in line:
        self.data["items_ram"] = int(val)
      elif "Items in NVM" in line:
        self.data["items_nvm"] = int(val)
      elif "RAM Evictions" in line:
        self.data["evictions_ram"] = int(val)
      elif "Cache Gets" in line:
        self.data["get_count"] = int(val)
      elif "NVM Gets" in line:
        val = line.split(", ")[0].split(":")[-1]
        self.data["get_nvm_count"] = int(val)
      elif "NVM Puts" in line:
        val = line.split(", ")[0].split(":")[-1]
        self.data["put_nvm_count"] = int(val)
      elif "NVM Evicts" in line:
        val = line.split(", ")[0].split(":")[-1]
        self.data["evict_nvm_count"] = int(val)
      elif "NVM Deletes" in line:
        val = line.split("Skipped")[0].split(":")[-1]
        self.data["delete_nvm_count"] = int(val)
      elif "Hit Ratio" in line:
        self.data["hit_ratio"] = float(val.rstrip("%"))
      elif "get" in line and "success" in line:
        split_line = line.split("/s,")
        self.data["get_bw"] = int(split_line[0].split(":")[-1].rstrip("/s").replace(',', ''))
        self.data["get_success"] = float(split_line[1].split(":")[-1].rstrip("/s").replace(',', '').rstrip("%"))
      elif "set" in line and "success" in line:
        split_line = line.split("/s,")
        self.data["set_bw"] = int(split_line[0].split(":")[-1].rstrip("/s").replace(',', ''))
        self.data["set_success"] = float(split_line[1].split(":")[-1].rstrip("/s").replace(',', '').rstrip("%"))
      elif "del" in line and "success" in line:
        split_line = line.split("/s,")
        self.data["del_bw"] = int(split_line[0].split(":")[-1].rstrip("/s").replace(',', ''))
        self.data["del_success"] = float(split_line[1].split(":")[-1].rstrip("/s").replace(',', '').rstrip("%"))


if __name__ == "__main__":
  parser = ResultParser("sample_result")

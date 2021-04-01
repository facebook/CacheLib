namespace cpp2 facebook.cachelib.benchmark

struct Vector {
  1: list<i32> m;
}

struct Map {
  1: map<i32, string> m;
}

struct UMap {
  1: map<i32, string> (
    cpp.template = "std::unordered_map",
  ) m;
}

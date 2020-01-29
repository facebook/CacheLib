namespace cpp2 facebook.cachelib.datatypebench

struct StdMap {
  1: required map<i32, string> m,
}

struct StdUnorderedMap {
  1: required map<i32, string>
  (cpp.template = "std::unordered_map")
  m,
}

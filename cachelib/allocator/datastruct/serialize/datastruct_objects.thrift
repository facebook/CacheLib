namespace cpp2 facebook.cachelib.serialization

// Adding a new "required" field will cause the cache to be dropped
// in the next release for our users. If the field needs to be required,
// make sure to communicate that with our users.

// Saved state for an SList
struct SListObject {
  2: required i64 size,
  3: required i64 compressedHead, // Pointer to the head element
  // TODO(bwatling): remove the default value and clean up SList::SList() once
  // we can rely on 'compressedTail' always being valid.
  4: i64 compressedTail = -1, // Pointer to the tail element
}

struct DListObject {
  1: required i64 compressedHead,
  2: required i64 compressedTail,
  3: required i64 size,
}

struct MultiDListObject {
  1: required list<DListObject> lists;
}

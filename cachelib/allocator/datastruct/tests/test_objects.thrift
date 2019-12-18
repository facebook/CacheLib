namespace cpp2 facebook.cachelib.test_serialization

// An older version of SListObject with no 'compressedTail'. Only used for
// testing warm rolls from the old format to the new format.
// TODO(bwatling): remove this when 'compressedTail' is always present.
struct SListObjectNoCompressedTail {
  2: required i64 size,
  3: required i64 compressedHead, // Pointer to the head element
}

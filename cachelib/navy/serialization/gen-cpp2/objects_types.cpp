/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
#include "/h/bsberg/CacheLib2/cachelib/navy/serialization/gen-cpp2/objects_types.h"
#include "/h/bsberg/CacheLib2/cachelib/navy/serialization/gen-cpp2/objects_types.tcc"

#include <thrift/lib/cpp2/gen/module_types_cpp.h>

#include "/h/bsberg/CacheLib2/cachelib/navy/serialization/gen-cpp2/objects_data.h"


namespace apache {
namespace thrift {
namespace detail {

void TccStructTraits<::facebook::cachelib::navy::serialization::IndexEntry>::translateFieldName(
    FOLLY_MAYBE_UNUSED folly::StringPiece _fname,
    FOLLY_MAYBE_UNUSED int16_t& fid,
    FOLLY_MAYBE_UNUSED apache::thrift::protocol::TType& _ftype) {
  if (false) {}
  else if (_fname == "key") {
    fid = 1;
    _ftype = apache::thrift::protocol::T_I32;
  }
  else if (_fname == "value") {
    fid = 2;
    _ftype = apache::thrift::protocol::T_I32;
  }
}
void TccStructTraits<::facebook::cachelib::navy::serialization::IndexBucket>::translateFieldName(
    FOLLY_MAYBE_UNUSED folly::StringPiece _fname,
    FOLLY_MAYBE_UNUSED int16_t& fid,
    FOLLY_MAYBE_UNUSED apache::thrift::protocol::TType& _ftype) {
  if (false) {}
  else if (_fname == "bucketId") {
    fid = 1;
    _ftype = apache::thrift::protocol::T_I32;
  }
  else if (_fname == "entries") {
    fid = 2;
    _ftype = apache::thrift::protocol::T_LIST;
  }
}
void TccStructTraits<::facebook::cachelib::navy::serialization::Region>::translateFieldName(
    FOLLY_MAYBE_UNUSED folly::StringPiece _fname,
    FOLLY_MAYBE_UNUSED int16_t& fid,
    FOLLY_MAYBE_UNUSED apache::thrift::protocol::TType& _ftype) {
  if (false) {}
  else if (_fname == "regionId") {
    fid = 1;
    _ftype = apache::thrift::protocol::T_I32;
  }
  else if (_fname == "lastEntryEndOffset") {
    fid = 2;
    _ftype = apache::thrift::protocol::T_I32;
  }
  else if (_fname == "classId") {
    fid = 3;
    _ftype = apache::thrift::protocol::T_I32;
  }
  else if (_fname == "numItems") {
    fid = 4;
    _ftype = apache::thrift::protocol::T_I32;
  }
  else if (_fname == "pinned") {
    fid = 5;
    _ftype = apache::thrift::protocol::T_BOOL;
  }
}
void TccStructTraits<::facebook::cachelib::navy::serialization::RegionData>::translateFieldName(
    FOLLY_MAYBE_UNUSED folly::StringPiece _fname,
    FOLLY_MAYBE_UNUSED int16_t& fid,
    FOLLY_MAYBE_UNUSED apache::thrift::protocol::TType& _ftype) {
  if (false) {}
  else if (_fname == "regions") {
    fid = 1;
    _ftype = apache::thrift::protocol::T_LIST;
  }
  else if (_fname == "regionSize") {
    fid = 2;
    _ftype = apache::thrift::protocol::T_I32;
  }
}
void TccStructTraits<::facebook::cachelib::navy::serialization::AccessStats>::translateFieldName(
    FOLLY_MAYBE_UNUSED folly::StringPiece _fname,
    FOLLY_MAYBE_UNUSED int16_t& fid,
    FOLLY_MAYBE_UNUSED apache::thrift::protocol::TType& _ftype) {
  if (false) {}
  else if (_fname == "totalHits") {
    fid = 1;
    _ftype = apache::thrift::protocol::T_BYTE;
  }
  else if (_fname == "currHits") {
    fid = 2;
    _ftype = apache::thrift::protocol::T_BYTE;
  }
  else if (_fname == "numReinsertions") {
    fid = 3;
    _ftype = apache::thrift::protocol::T_BYTE;
  }
}
void TccStructTraits<::facebook::cachelib::navy::serialization::AccessTracker>::translateFieldName(
    FOLLY_MAYBE_UNUSED folly::StringPiece _fname,
    FOLLY_MAYBE_UNUSED int16_t& fid,
    FOLLY_MAYBE_UNUSED apache::thrift::protocol::TType& _ftype) {
  if (false) {}
  else if (_fname == "data") {
    fid = 1;
    _ftype = apache::thrift::protocol::T_MAP;
  }
}
void TccStructTraits<::facebook::cachelib::navy::serialization::BlockCacheConfig>::translateFieldName(
    FOLLY_MAYBE_UNUSED folly::StringPiece _fname,
    FOLLY_MAYBE_UNUSED int16_t& fid,
    FOLLY_MAYBE_UNUSED apache::thrift::protocol::TType& _ftype) {
  if (false) {}
  else if (_fname == "version") {
    fid = 1;
    _ftype = apache::thrift::protocol::T_I64;
  }
  else if (_fname == "cacheBaseOffset") {
    fid = 2;
    _ftype = apache::thrift::protocol::T_I64;
  }
  else if (_fname == "cacheSize") {
    fid = 3;
    _ftype = apache::thrift::protocol::T_I64;
  }
  else if (_fname == "blockSize") {
    fid = 4;
    _ftype = apache::thrift::protocol::T_I32;
  }
  else if (_fname == "sizeClasses") {
    fid = 5;
    _ftype = apache::thrift::protocol::T_SET;
  }
  else if (_fname == "checksum") {
    fid = 6;
    _ftype = apache::thrift::protocol::T_BOOL;
  }
  else if (_fname == "sizeDist") {
    fid = 7;
    _ftype = apache::thrift::protocol::T_MAP;
  }
  else if (_fname == "holeCount") {
    fid = 8;
    _ftype = apache::thrift::protocol::T_I64;
  }
  else if (_fname == "holeSizeTotal") {
    fid = 9;
    _ftype = apache::thrift::protocol::T_I64;
  }
  else if (_fname == "reinsertionPolicyEnabled") {
    fid = 10;
    _ftype = apache::thrift::protocol::T_BOOL;
  }
}
void TccStructTraits<::facebook::cachelib::navy::serialization::BigHashPersistentData>::translateFieldName(
    FOLLY_MAYBE_UNUSED folly::StringPiece _fname,
    FOLLY_MAYBE_UNUSED int16_t& fid,
    FOLLY_MAYBE_UNUSED apache::thrift::protocol::TType& _ftype) {
  if (false) {}
  else if (_fname == "version") {
    fid = 1;
    _ftype = apache::thrift::protocol::T_I32;
  }
  else if (_fname == "generationTime") {
    fid = 2;
    _ftype = apache::thrift::protocol::T_I64;
  }
  else if (_fname == "itemCount") {
    fid = 3;
    _ftype = apache::thrift::protocol::T_I64;
  }
  else if (_fname == "bucketSize") {
    fid = 4;
    _ftype = apache::thrift::protocol::T_I64;
  }
  else if (_fname == "cacheBaseOffset") {
    fid = 5;
    _ftype = apache::thrift::protocol::T_I64;
  }
  else if (_fname == "numBuckets") {
    fid = 6;
    _ftype = apache::thrift::protocol::T_I64;
  }
  else if (_fname == "sizeDist") {
    fid = 7;
    _ftype = apache::thrift::protocol::T_MAP;
  }
}
void TccStructTraits<::facebook::cachelib::navy::serialization::BloomFilterPersistentData>::translateFieldName(
    FOLLY_MAYBE_UNUSED folly::StringPiece _fname,
    FOLLY_MAYBE_UNUSED int16_t& fid,
    FOLLY_MAYBE_UNUSED apache::thrift::protocol::TType& _ftype) {
  if (false) {}
  else if (_fname == "numFilters") {
    fid = 1;
    _ftype = apache::thrift::protocol::T_I32;
  }
  else if (_fname == "hashTableBitSize") {
    fid = 2;
    _ftype = apache::thrift::protocol::T_I64;
  }
  else if (_fname == "filterByteSize") {
    fid = 3;
    _ftype = apache::thrift::protocol::T_I64;
  }
  else if (_fname == "fragmentSize") {
    fid = 4;
    _ftype = apache::thrift::protocol::T_I32;
  }
  else if (_fname == "seeds") {
    fid = 5;
    _ftype = apache::thrift::protocol::T_LIST;
  }
}

} // namespace detail
} // namespace thrift
} // namespace apache

namespace facebook { namespace cachelib { namespace navy { namespace serialization {

IndexEntry::IndexEntry(apache::thrift::FragileConstructor, int32_t key__arg, int32_t value__arg) :
    key(std::move(key__arg)),
    value(std::move(value__arg)) {}

void IndexEntry::__clear() {
  // clear all fields
  key = 0;
  value = 0;
}

bool IndexEntry::operator==(const IndexEntry& rhs) const {
  (void)rhs;
  auto& lhs = *this;
  (void)lhs;
  if (!(lhs.key == rhs.key)) {
    return false;
  }
  if (!(lhs.value == rhs.value)) {
    return false;
  }
  return true;
}

bool IndexEntry::operator<(const IndexEntry& rhs) const {
  (void)rhs;
  auto& lhs = *this;
  (void)lhs;
  if (!(lhs.key == rhs.key)) {
    return lhs.key < rhs.key;
  }
  if (!(lhs.value == rhs.value)) {
    return lhs.value < rhs.value;
  }
  return false;
}


void swap(IndexEntry& a, IndexEntry& b) {
  using ::std::swap;
  swap(a.key, b.key);
  swap(a.value, b.value);
}

template void IndexEntry::readNoXfer<>(apache::thrift::BinaryProtocolReader*);
template uint32_t IndexEntry::write<>(apache::thrift::BinaryProtocolWriter*) const;
template uint32_t IndexEntry::serializedSize<>(apache::thrift::BinaryProtocolWriter const*) const;
template uint32_t IndexEntry::serializedSizeZC<>(apache::thrift::BinaryProtocolWriter const*) const;
template void IndexEntry::readNoXfer<>(apache::thrift::CompactProtocolReader*);
template uint32_t IndexEntry::write<>(apache::thrift::CompactProtocolWriter*) const;
template uint32_t IndexEntry::serializedSize<>(apache::thrift::CompactProtocolWriter const*) const;
template uint32_t IndexEntry::serializedSizeZC<>(apache::thrift::CompactProtocolWriter const*) const;

}}}} // facebook::cachelib::navy::serialization
namespace facebook { namespace cachelib { namespace navy { namespace serialization {

IndexBucket::IndexBucket(apache::thrift::FragileConstructor, int32_t bucketId__arg, ::std::vector< ::facebook::cachelib::navy::serialization::IndexEntry> entries__arg) :
    bucketId(std::move(bucketId__arg)),
    entries(std::move(entries__arg)) {}

void IndexBucket::__clear() {
  // clear all fields
  bucketId = 0;
  entries.clear();
}

bool IndexBucket::operator==(const IndexBucket& rhs) const {
  (void)rhs;
  auto& lhs = *this;
  (void)lhs;
  if (!(lhs.bucketId == rhs.bucketId)) {
    return false;
  }
  if (!(lhs.entries == rhs.entries)) {
    return false;
  }
  return true;
}

bool IndexBucket::operator<(const IndexBucket& rhs) const {
  (void)rhs;
  auto& lhs = *this;
  (void)lhs;
  if (!(lhs.bucketId == rhs.bucketId)) {
    return lhs.bucketId < rhs.bucketId;
  }
  if (!(lhs.entries == rhs.entries)) {
    return lhs.entries < rhs.entries;
  }
  return false;
}

const ::std::vector< ::facebook::cachelib::navy::serialization::IndexEntry>& IndexBucket::get_entries() const& {
  return entries;
}

::std::vector< ::facebook::cachelib::navy::serialization::IndexEntry> IndexBucket::get_entries() && {
  return std::move(entries);
}


void swap(IndexBucket& a, IndexBucket& b) {
  using ::std::swap;
  swap(a.bucketId, b.bucketId);
  swap(a.entries, b.entries);
}

template void IndexBucket::readNoXfer<>(apache::thrift::BinaryProtocolReader*);
template uint32_t IndexBucket::write<>(apache::thrift::BinaryProtocolWriter*) const;
template uint32_t IndexBucket::serializedSize<>(apache::thrift::BinaryProtocolWriter const*) const;
template uint32_t IndexBucket::serializedSizeZC<>(apache::thrift::BinaryProtocolWriter const*) const;
template void IndexBucket::readNoXfer<>(apache::thrift::CompactProtocolReader*);
template uint32_t IndexBucket::write<>(apache::thrift::CompactProtocolWriter*) const;
template uint32_t IndexBucket::serializedSize<>(apache::thrift::CompactProtocolWriter const*) const;
template uint32_t IndexBucket::serializedSizeZC<>(apache::thrift::CompactProtocolWriter const*) const;

}}}} // facebook::cachelib::navy::serialization
namespace facebook { namespace cachelib { namespace navy { namespace serialization {

Region::Region(apache::thrift::FragileConstructor, int32_t regionId__arg, int32_t lastEntryEndOffset__arg, int32_t classId__arg, int32_t numItems__arg, bool pinned__arg) :
    regionId(std::move(regionId__arg)),
    lastEntryEndOffset(std::move(lastEntryEndOffset__arg)),
    classId(std::move(classId__arg)),
    numItems(std::move(numItems__arg)),
    pinned(std::move(pinned__arg)) {}

void Region::__clear() {
  // clear all fields
  regionId = 0;
  lastEntryEndOffset = 0;
  classId = 0;
  numItems = 0;
  pinned = false;
}

bool Region::operator==(const Region& rhs) const {
  (void)rhs;
  auto& lhs = *this;
  (void)lhs;
  if (!(lhs.regionId == rhs.regionId)) {
    return false;
  }
  if (!(lhs.lastEntryEndOffset == rhs.lastEntryEndOffset)) {
    return false;
  }
  if (!(lhs.classId == rhs.classId)) {
    return false;
  }
  if (!(lhs.numItems == rhs.numItems)) {
    return false;
  }
  if (!(lhs.pinned == rhs.pinned)) {
    return false;
  }
  return true;
}

bool Region::operator<(const Region& rhs) const {
  (void)rhs;
  auto& lhs = *this;
  (void)lhs;
  if (!(lhs.regionId == rhs.regionId)) {
    return lhs.regionId < rhs.regionId;
  }
  if (!(lhs.lastEntryEndOffset == rhs.lastEntryEndOffset)) {
    return lhs.lastEntryEndOffset < rhs.lastEntryEndOffset;
  }
  if (!(lhs.classId == rhs.classId)) {
    return lhs.classId < rhs.classId;
  }
  if (!(lhs.numItems == rhs.numItems)) {
    return lhs.numItems < rhs.numItems;
  }
  if (!(lhs.pinned == rhs.pinned)) {
    return lhs.pinned < rhs.pinned;
  }
  return false;
}


void swap(Region& a, Region& b) {
  using ::std::swap;
  swap(a.regionId, b.regionId);
  swap(a.lastEntryEndOffset, b.lastEntryEndOffset);
  swap(a.classId, b.classId);
  swap(a.numItems, b.numItems);
  swap(a.pinned, b.pinned);
}

template void Region::readNoXfer<>(apache::thrift::BinaryProtocolReader*);
template uint32_t Region::write<>(apache::thrift::BinaryProtocolWriter*) const;
template uint32_t Region::serializedSize<>(apache::thrift::BinaryProtocolWriter const*) const;
template uint32_t Region::serializedSizeZC<>(apache::thrift::BinaryProtocolWriter const*) const;
template void Region::readNoXfer<>(apache::thrift::CompactProtocolReader*);
template uint32_t Region::write<>(apache::thrift::CompactProtocolWriter*) const;
template uint32_t Region::serializedSize<>(apache::thrift::CompactProtocolWriter const*) const;
template uint32_t Region::serializedSizeZC<>(apache::thrift::CompactProtocolWriter const*) const;

}}}} // facebook::cachelib::navy::serialization
namespace facebook { namespace cachelib { namespace navy { namespace serialization {

RegionData::RegionData(apache::thrift::FragileConstructor, ::std::vector< ::facebook::cachelib::navy::serialization::Region> regions__arg, int32_t regionSize__arg) :
    regions(std::move(regions__arg)),
    regionSize(std::move(regionSize__arg)) {}

void RegionData::__clear() {
  // clear all fields
  regions.clear();
  regionSize = 0;
}

bool RegionData::operator==(const RegionData& rhs) const {
  (void)rhs;
  auto& lhs = *this;
  (void)lhs;
  if (!(lhs.regions == rhs.regions)) {
    return false;
  }
  if (!(lhs.regionSize == rhs.regionSize)) {
    return false;
  }
  return true;
}

bool RegionData::operator<(const RegionData& rhs) const {
  (void)rhs;
  auto& lhs = *this;
  (void)lhs;
  if (!(lhs.regions == rhs.regions)) {
    return lhs.regions < rhs.regions;
  }
  if (!(lhs.regionSize == rhs.regionSize)) {
    return lhs.regionSize < rhs.regionSize;
  }
  return false;
}

const ::std::vector< ::facebook::cachelib::navy::serialization::Region>& RegionData::get_regions() const& {
  return regions;
}

::std::vector< ::facebook::cachelib::navy::serialization::Region> RegionData::get_regions() && {
  return std::move(regions);
}


void swap(RegionData& a, RegionData& b) {
  using ::std::swap;
  swap(a.regions, b.regions);
  swap(a.regionSize, b.regionSize);
}

template void RegionData::readNoXfer<>(apache::thrift::BinaryProtocolReader*);
template uint32_t RegionData::write<>(apache::thrift::BinaryProtocolWriter*) const;
template uint32_t RegionData::serializedSize<>(apache::thrift::BinaryProtocolWriter const*) const;
template uint32_t RegionData::serializedSizeZC<>(apache::thrift::BinaryProtocolWriter const*) const;
template void RegionData::readNoXfer<>(apache::thrift::CompactProtocolReader*);
template uint32_t RegionData::write<>(apache::thrift::CompactProtocolWriter*) const;
template uint32_t RegionData::serializedSize<>(apache::thrift::CompactProtocolWriter const*) const;
template uint32_t RegionData::serializedSizeZC<>(apache::thrift::CompactProtocolWriter const*) const;

}}}} // facebook::cachelib::navy::serialization
namespace facebook { namespace cachelib { namespace navy { namespace serialization {

AccessStats::AccessStats(apache::thrift::FragileConstructor, int8_t totalHits__arg, int8_t currHits__arg, int8_t numReinsertions__arg) :
    totalHits(std::move(totalHits__arg)),
    currHits(std::move(currHits__arg)),
    numReinsertions(std::move(numReinsertions__arg)) {
  __isset.totalHits = true;
  __isset.currHits = true;
  __isset.numReinsertions = true;
}

void AccessStats::__clear() {
  // clear all fields
  totalHits = static_cast<int8_t>(0);
  currHits = static_cast<int8_t>(0);
  numReinsertions = static_cast<int8_t>(0);
  __isset = {};
}

bool AccessStats::operator==(const AccessStats& rhs) const {
  (void)rhs;
  auto& lhs = *this;
  (void)lhs;
  if (!(lhs.totalHits == rhs.totalHits)) {
    return false;
  }
  if (!(lhs.currHits == rhs.currHits)) {
    return false;
  }
  if (!(lhs.numReinsertions == rhs.numReinsertions)) {
    return false;
  }
  return true;
}

bool AccessStats::operator<(const AccessStats& rhs) const {
  (void)rhs;
  auto& lhs = *this;
  (void)lhs;
  if (!(lhs.totalHits == rhs.totalHits)) {
    return lhs.totalHits < rhs.totalHits;
  }
  if (!(lhs.currHits == rhs.currHits)) {
    return lhs.currHits < rhs.currHits;
  }
  if (!(lhs.numReinsertions == rhs.numReinsertions)) {
    return lhs.numReinsertions < rhs.numReinsertions;
  }
  return false;
}


void swap(AccessStats& a, AccessStats& b) {
  using ::std::swap;
  swap(a.totalHits, b.totalHits);
  swap(a.currHits, b.currHits);
  swap(a.numReinsertions, b.numReinsertions);
  swap(a.__isset, b.__isset);
}

template void AccessStats::readNoXfer<>(apache::thrift::BinaryProtocolReader*);
template uint32_t AccessStats::write<>(apache::thrift::BinaryProtocolWriter*) const;
template uint32_t AccessStats::serializedSize<>(apache::thrift::BinaryProtocolWriter const*) const;
template uint32_t AccessStats::serializedSizeZC<>(apache::thrift::BinaryProtocolWriter const*) const;
template void AccessStats::readNoXfer<>(apache::thrift::CompactProtocolReader*);
template uint32_t AccessStats::write<>(apache::thrift::CompactProtocolWriter*) const;
template uint32_t AccessStats::serializedSize<>(apache::thrift::CompactProtocolWriter const*) const;
template uint32_t AccessStats::serializedSizeZC<>(apache::thrift::CompactProtocolWriter const*) const;

}}}} // facebook::cachelib::navy::serialization
namespace facebook { namespace cachelib { namespace navy { namespace serialization {

AccessTracker::AccessTracker(apache::thrift::FragileConstructor, ::std::map<int64_t,  ::facebook::cachelib::navy::serialization::AccessStats> data__arg) :
    data(std::move(data__arg)) {
  __isset.data = true;
}

void AccessTracker::__clear() {
  // clear all fields
  data.clear();
  __isset = {};
}

bool AccessTracker::operator==(const AccessTracker& rhs) const {
  (void)rhs;
  auto& lhs = *this;
  (void)lhs;
  if (!(lhs.data == rhs.data)) {
    return false;
  }
  return true;
}

bool AccessTracker::operator<(const AccessTracker& rhs) const {
  (void)rhs;
  auto& lhs = *this;
  (void)lhs;
  if (!(lhs.data == rhs.data)) {
    return lhs.data < rhs.data;
  }
  return false;
}

const ::std::map<int64_t,  ::facebook::cachelib::navy::serialization::AccessStats>& AccessTracker::get_data() const& {
  return data;
}

::std::map<int64_t,  ::facebook::cachelib::navy::serialization::AccessStats> AccessTracker::get_data() && {
  return std::move(data);
}


void swap(AccessTracker& a, AccessTracker& b) {
  using ::std::swap;
  swap(a.data, b.data);
  swap(a.__isset, b.__isset);
}

template void AccessTracker::readNoXfer<>(apache::thrift::BinaryProtocolReader*);
template uint32_t AccessTracker::write<>(apache::thrift::BinaryProtocolWriter*) const;
template uint32_t AccessTracker::serializedSize<>(apache::thrift::BinaryProtocolWriter const*) const;
template uint32_t AccessTracker::serializedSizeZC<>(apache::thrift::BinaryProtocolWriter const*) const;
template void AccessTracker::readNoXfer<>(apache::thrift::CompactProtocolReader*);
template uint32_t AccessTracker::write<>(apache::thrift::CompactProtocolWriter*) const;
template uint32_t AccessTracker::serializedSize<>(apache::thrift::CompactProtocolWriter const*) const;
template uint32_t AccessTracker::serializedSizeZC<>(apache::thrift::CompactProtocolWriter const*) const;

}}}} // facebook::cachelib::navy::serialization
namespace facebook { namespace cachelib { namespace navy { namespace serialization {

BlockCacheConfig::BlockCacheConfig() :
      version(0LL),
      cacheBaseOffset(0LL),
      cacheSize(0LL),
      blockSize(0),
      checksum(false),
      holeCount(0LL),
      holeSizeTotal(0LL),
      reinsertionPolicyEnabled(false) {}


BlockCacheConfig::~BlockCacheConfig() {}

BlockCacheConfig::BlockCacheConfig(apache::thrift::FragileConstructor, int64_t version__arg, int64_t cacheBaseOffset__arg, int64_t cacheSize__arg, int32_t blockSize__arg, ::std::set<int32_t> sizeClasses__arg, bool checksum__arg, ::std::map<int64_t, int64_t> sizeDist__arg, int64_t holeCount__arg, int64_t holeSizeTotal__arg, bool reinsertionPolicyEnabled__arg) :
    version(std::move(version__arg)),
    cacheBaseOffset(std::move(cacheBaseOffset__arg)),
    cacheSize(std::move(cacheSize__arg)),
    blockSize(std::move(blockSize__arg)),
    sizeClasses(std::move(sizeClasses__arg)),
    checksum(std::move(checksum__arg)),
    sizeDist(std::move(sizeDist__arg)),
    holeCount(std::move(holeCount__arg)),
    holeSizeTotal(std::move(holeSizeTotal__arg)),
    reinsertionPolicyEnabled(std::move(reinsertionPolicyEnabled__arg)) {
  __isset.sizeDist = true;
  __isset.holeCount = true;
  __isset.holeSizeTotal = true;
  __isset.reinsertionPolicyEnabled = true;
}

void BlockCacheConfig::__clear() {
  // clear all fields
  version = 0LL;
  cacheBaseOffset = 0LL;
  cacheSize = 0LL;
  blockSize = 0;
  sizeClasses.clear();
  checksum = false;
  sizeDist.clear();
  holeCount = 0LL;
  holeSizeTotal = 0LL;
  reinsertionPolicyEnabled = false;
  __isset = {};
}

bool BlockCacheConfig::operator==(const BlockCacheConfig& rhs) const {
  (void)rhs;
  auto& lhs = *this;
  (void)lhs;
  if (!(lhs.version == rhs.version)) {
    return false;
  }
  if (!(lhs.cacheBaseOffset == rhs.cacheBaseOffset)) {
    return false;
  }
  if (!(lhs.cacheSize == rhs.cacheSize)) {
    return false;
  }
  if (!(lhs.blockSize == rhs.blockSize)) {
    return false;
  }
  if (!(lhs.sizeClasses == rhs.sizeClasses)) {
    return false;
  }
  if (!(lhs.checksum == rhs.checksum)) {
    return false;
  }
  if (!(lhs.sizeDist == rhs.sizeDist)) {
    return false;
  }
  if (!(lhs.holeCount == rhs.holeCount)) {
    return false;
  }
  if (!(lhs.holeSizeTotal == rhs.holeSizeTotal)) {
    return false;
  }
  if (!(lhs.reinsertionPolicyEnabled == rhs.reinsertionPolicyEnabled)) {
    return false;
  }
  return true;
}

bool BlockCacheConfig::operator<(const BlockCacheConfig& rhs) const {
  (void)rhs;
  auto& lhs = *this;
  (void)lhs;
  if (!(lhs.version == rhs.version)) {
    return lhs.version < rhs.version;
  }
  if (!(lhs.cacheBaseOffset == rhs.cacheBaseOffset)) {
    return lhs.cacheBaseOffset < rhs.cacheBaseOffset;
  }
  if (!(lhs.cacheSize == rhs.cacheSize)) {
    return lhs.cacheSize < rhs.cacheSize;
  }
  if (!(lhs.blockSize == rhs.blockSize)) {
    return lhs.blockSize < rhs.blockSize;
  }
  if (!(lhs.sizeClasses == rhs.sizeClasses)) {
    return lhs.sizeClasses < rhs.sizeClasses;
  }
  if (!(lhs.checksum == rhs.checksum)) {
    return lhs.checksum < rhs.checksum;
  }
  if (!(lhs.sizeDist == rhs.sizeDist)) {
    return lhs.sizeDist < rhs.sizeDist;
  }
  if (!(lhs.holeCount == rhs.holeCount)) {
    return lhs.holeCount < rhs.holeCount;
  }
  if (!(lhs.holeSizeTotal == rhs.holeSizeTotal)) {
    return lhs.holeSizeTotal < rhs.holeSizeTotal;
  }
  if (!(lhs.reinsertionPolicyEnabled == rhs.reinsertionPolicyEnabled)) {
    return lhs.reinsertionPolicyEnabled < rhs.reinsertionPolicyEnabled;
  }
  return false;
}

const ::std::set<int32_t>& BlockCacheConfig::get_sizeClasses() const& {
  return sizeClasses;
}

::std::set<int32_t> BlockCacheConfig::get_sizeClasses() && {
  return std::move(sizeClasses);
}

const ::std::map<int64_t, int64_t>& BlockCacheConfig::get_sizeDist() const& {
  return sizeDist;
}

::std::map<int64_t, int64_t> BlockCacheConfig::get_sizeDist() && {
  return std::move(sizeDist);
}


void swap(BlockCacheConfig& a, BlockCacheConfig& b) {
  using ::std::swap;
  swap(a.version, b.version);
  swap(a.cacheBaseOffset, b.cacheBaseOffset);
  swap(a.cacheSize, b.cacheSize);
  swap(a.blockSize, b.blockSize);
  swap(a.sizeClasses, b.sizeClasses);
  swap(a.checksum, b.checksum);
  swap(a.sizeDist, b.sizeDist);
  swap(a.holeCount, b.holeCount);
  swap(a.holeSizeTotal, b.holeSizeTotal);
  swap(a.reinsertionPolicyEnabled, b.reinsertionPolicyEnabled);
  swap(a.__isset, b.__isset);
}

template void BlockCacheConfig::readNoXfer<>(apache::thrift::BinaryProtocolReader*);
template uint32_t BlockCacheConfig::write<>(apache::thrift::BinaryProtocolWriter*) const;
template uint32_t BlockCacheConfig::serializedSize<>(apache::thrift::BinaryProtocolWriter const*) const;
template uint32_t BlockCacheConfig::serializedSizeZC<>(apache::thrift::BinaryProtocolWriter const*) const;
template void BlockCacheConfig::readNoXfer<>(apache::thrift::CompactProtocolReader*);
template uint32_t BlockCacheConfig::write<>(apache::thrift::CompactProtocolWriter*) const;
template uint32_t BlockCacheConfig::serializedSize<>(apache::thrift::CompactProtocolWriter const*) const;
template uint32_t BlockCacheConfig::serializedSizeZC<>(apache::thrift::CompactProtocolWriter const*) const;

}}}} // facebook::cachelib::navy::serialization
namespace facebook { namespace cachelib { namespace navy { namespace serialization {

BigHashPersistentData::BigHashPersistentData() :
      version(0),
      generationTime(0LL),
      itemCount(0LL),
      bucketSize(0LL),
      cacheBaseOffset(0LL),
      numBuckets(0LL) {}


BigHashPersistentData::~BigHashPersistentData() {}

BigHashPersistentData::BigHashPersistentData(apache::thrift::FragileConstructor, int32_t version__arg, int64_t generationTime__arg, int64_t itemCount__arg, int64_t bucketSize__arg, int64_t cacheBaseOffset__arg, int64_t numBuckets__arg, ::std::map<int64_t, int64_t> sizeDist__arg) :
    version(std::move(version__arg)),
    generationTime(std::move(generationTime__arg)),
    itemCount(std::move(itemCount__arg)),
    bucketSize(std::move(bucketSize__arg)),
    cacheBaseOffset(std::move(cacheBaseOffset__arg)),
    numBuckets(std::move(numBuckets__arg)),
    sizeDist(std::move(sizeDist__arg)) {
  __isset.sizeDist = true;
}

void BigHashPersistentData::__clear() {
  // clear all fields
  version = 0;
  generationTime = 0LL;
  itemCount = 0LL;
  bucketSize = 0LL;
  cacheBaseOffset = 0LL;
  numBuckets = 0LL;
  sizeDist.clear();
  __isset = {};
}

bool BigHashPersistentData::operator==(const BigHashPersistentData& rhs) const {
  (void)rhs;
  auto& lhs = *this;
  (void)lhs;
  if (!(lhs.version == rhs.version)) {
    return false;
  }
  if (!(lhs.generationTime == rhs.generationTime)) {
    return false;
  }
  if (!(lhs.itemCount == rhs.itemCount)) {
    return false;
  }
  if (!(lhs.bucketSize == rhs.bucketSize)) {
    return false;
  }
  if (!(lhs.cacheBaseOffset == rhs.cacheBaseOffset)) {
    return false;
  }
  if (!(lhs.numBuckets == rhs.numBuckets)) {
    return false;
  }
  if (!(lhs.sizeDist == rhs.sizeDist)) {
    return false;
  }
  return true;
}

bool BigHashPersistentData::operator<(const BigHashPersistentData& rhs) const {
  (void)rhs;
  auto& lhs = *this;
  (void)lhs;
  if (!(lhs.version == rhs.version)) {
    return lhs.version < rhs.version;
  }
  if (!(lhs.generationTime == rhs.generationTime)) {
    return lhs.generationTime < rhs.generationTime;
  }
  if (!(lhs.itemCount == rhs.itemCount)) {
    return lhs.itemCount < rhs.itemCount;
  }
  if (!(lhs.bucketSize == rhs.bucketSize)) {
    return lhs.bucketSize < rhs.bucketSize;
  }
  if (!(lhs.cacheBaseOffset == rhs.cacheBaseOffset)) {
    return lhs.cacheBaseOffset < rhs.cacheBaseOffset;
  }
  if (!(lhs.numBuckets == rhs.numBuckets)) {
    return lhs.numBuckets < rhs.numBuckets;
  }
  if (!(lhs.sizeDist == rhs.sizeDist)) {
    return lhs.sizeDist < rhs.sizeDist;
  }
  return false;
}

const ::std::map<int64_t, int64_t>& BigHashPersistentData::get_sizeDist() const& {
  return sizeDist;
}

::std::map<int64_t, int64_t> BigHashPersistentData::get_sizeDist() && {
  return std::move(sizeDist);
}


void swap(BigHashPersistentData& a, BigHashPersistentData& b) {
  using ::std::swap;
  swap(a.version, b.version);
  swap(a.generationTime, b.generationTime);
  swap(a.itemCount, b.itemCount);
  swap(a.bucketSize, b.bucketSize);
  swap(a.cacheBaseOffset, b.cacheBaseOffset);
  swap(a.numBuckets, b.numBuckets);
  swap(a.sizeDist, b.sizeDist);
  swap(a.__isset, b.__isset);
}

template void BigHashPersistentData::readNoXfer<>(apache::thrift::BinaryProtocolReader*);
template uint32_t BigHashPersistentData::write<>(apache::thrift::BinaryProtocolWriter*) const;
template uint32_t BigHashPersistentData::serializedSize<>(apache::thrift::BinaryProtocolWriter const*) const;
template uint32_t BigHashPersistentData::serializedSizeZC<>(apache::thrift::BinaryProtocolWriter const*) const;
template void BigHashPersistentData::readNoXfer<>(apache::thrift::CompactProtocolReader*);
template uint32_t BigHashPersistentData::write<>(apache::thrift::CompactProtocolWriter*) const;
template uint32_t BigHashPersistentData::serializedSize<>(apache::thrift::CompactProtocolWriter const*) const;
template uint32_t BigHashPersistentData::serializedSizeZC<>(apache::thrift::CompactProtocolWriter const*) const;

}}}} // facebook::cachelib::navy::serialization
namespace facebook { namespace cachelib { namespace navy { namespace serialization {

BloomFilterPersistentData::BloomFilterPersistentData() :
      numFilters(0),
      hashTableBitSize(0LL),
      filterByteSize(0LL),
      fragmentSize(0) {}


BloomFilterPersistentData::~BloomFilterPersistentData() {}

BloomFilterPersistentData::BloomFilterPersistentData(apache::thrift::FragileConstructor, int32_t numFilters__arg, int64_t hashTableBitSize__arg, int64_t filterByteSize__arg, int32_t fragmentSize__arg, ::std::vector<int64_t> seeds__arg) :
    numFilters(std::move(numFilters__arg)),
    hashTableBitSize(std::move(hashTableBitSize__arg)),
    filterByteSize(std::move(filterByteSize__arg)),
    fragmentSize(std::move(fragmentSize__arg)),
    seeds(std::move(seeds__arg)) {}

void BloomFilterPersistentData::__clear() {
  // clear all fields
  numFilters = 0;
  hashTableBitSize = 0LL;
  filterByteSize = 0LL;
  fragmentSize = 0;
  seeds.clear();
}

bool BloomFilterPersistentData::operator==(const BloomFilterPersistentData& rhs) const {
  (void)rhs;
  auto& lhs = *this;
  (void)lhs;
  if (!(lhs.numFilters == rhs.numFilters)) {
    return false;
  }
  if (!(lhs.hashTableBitSize == rhs.hashTableBitSize)) {
    return false;
  }
  if (!(lhs.filterByteSize == rhs.filterByteSize)) {
    return false;
  }
  if (!(lhs.fragmentSize == rhs.fragmentSize)) {
    return false;
  }
  if (!(lhs.seeds == rhs.seeds)) {
    return false;
  }
  return true;
}

bool BloomFilterPersistentData::operator<(const BloomFilterPersistentData& rhs) const {
  (void)rhs;
  auto& lhs = *this;
  (void)lhs;
  if (!(lhs.numFilters == rhs.numFilters)) {
    return lhs.numFilters < rhs.numFilters;
  }
  if (!(lhs.hashTableBitSize == rhs.hashTableBitSize)) {
    return lhs.hashTableBitSize < rhs.hashTableBitSize;
  }
  if (!(lhs.filterByteSize == rhs.filterByteSize)) {
    return lhs.filterByteSize < rhs.filterByteSize;
  }
  if (!(lhs.fragmentSize == rhs.fragmentSize)) {
    return lhs.fragmentSize < rhs.fragmentSize;
  }
  if (!(lhs.seeds == rhs.seeds)) {
    return lhs.seeds < rhs.seeds;
  }
  return false;
}

const ::std::vector<int64_t>& BloomFilterPersistentData::get_seeds() const& {
  return seeds;
}

::std::vector<int64_t> BloomFilterPersistentData::get_seeds() && {
  return std::move(seeds);
}


void swap(BloomFilterPersistentData& a, BloomFilterPersistentData& b) {
  using ::std::swap;
  swap(a.numFilters, b.numFilters);
  swap(a.hashTableBitSize, b.hashTableBitSize);
  swap(a.filterByteSize, b.filterByteSize);
  swap(a.fragmentSize, b.fragmentSize);
  swap(a.seeds, b.seeds);
}

template void BloomFilterPersistentData::readNoXfer<>(apache::thrift::BinaryProtocolReader*);
template uint32_t BloomFilterPersistentData::write<>(apache::thrift::BinaryProtocolWriter*) const;
template uint32_t BloomFilterPersistentData::serializedSize<>(apache::thrift::BinaryProtocolWriter const*) const;
template uint32_t BloomFilterPersistentData::serializedSizeZC<>(apache::thrift::BinaryProtocolWriter const*) const;
template void BloomFilterPersistentData::readNoXfer<>(apache::thrift::CompactProtocolReader*);
template uint32_t BloomFilterPersistentData::write<>(apache::thrift::CompactProtocolWriter*) const;
template uint32_t BloomFilterPersistentData::serializedSize<>(apache::thrift::CompactProtocolWriter const*) const;
template uint32_t BloomFilterPersistentData::serializedSizeZC<>(apache::thrift::CompactProtocolWriter const*) const;

}}}} // facebook::cachelib::navy::serialization

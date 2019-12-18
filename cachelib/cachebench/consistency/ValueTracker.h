#pragma once

#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include <folly/Range.h>
#include <folly/hash/Hash.h>

#include "cachelib/cachebench/consistency/ShortThreadId.h"
#include "cachelib/cachebench/consistency/ValueHistory.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

class ValueTracker {
 public:
  using TrackerIndexT = uint32_t;
  using HistoryIndexT = uint32_t;

  // Opaque for the user index into the value history
  struct Index {
    friend ValueTracker;

   private:
    Index(TrackerIndexT ti, HistoryIndexT hi)
        : trackerIndex{ti}, historyIndex{hi} {}

    // Indexing scheme is like follows:
    // trackers_[@trackerIndex].getAt/setAt(@historyIndex)
    TrackerIndexT trackerIndex{};
    HistoryIndexT historyIndex{};
  };

  // Helper for the constructor
  static std::vector<folly::StringPiece> wrapStrings(
      const std::vector<std::string>& v);

  // Value tracker constructor
  //
  // Params:
  // @keys    list of keys to track
  explicit ValueTracker(std::vector<folly::StringPiece> keys);
  ValueTracker(const ValueTracker&) = delete;
  ValueTracker& operator=(const ValueTracker&) = delete;

  // Philosophy here is beginXXX function marks operation begin and returns
  // the operation id, that will be used in endXXX call for internal purpose.
  Index beginGet(folly::StringPiece key);
  Index beginSet(folly::StringPiece key, uint64_t value);
  Index beginDelete(folly::StringPiece key);
  // Returns false if inconsistent and feeds @es with the history that reveals
  // inconsistency (optional).
  bool endGet(Index beginIdx,
              uint64_t data,
              bool found,
              EventStream* es = nullptr);
  void endSet(Index beginIdx);
  void endDelete(Index beginIdx);

  void evicted(folly::StringPiece key);

 private:
  // Map from key to index into trackers vector
  using ValueHistoryMap = std::unordered_map<folly::StringPiece,
                                             TrackerIndexT,
                                             folly::hasher<folly::StringPiece>>;

  TrackerIndexT getTrackerIndexOrThrow(folly::StringPiece key) const;
  EventInfo eventInfo() const;

  ValueHistoryMap keyTrackerMap_;
  std::vector<ValueHistory> trackers_;
  mutable ShortThreadIdMap shortTids_;
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook

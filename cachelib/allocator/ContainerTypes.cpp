#include "cachelib/allocator/ChainedHashTable.h"
#include "cachelib/allocator/MM2Q.h"
#include "cachelib/allocator/MMLru.h"
#include "cachelib/allocator/MMTinyLFU.h"
namespace facebook {
namespace cachelib {
// MMType
const int MMLru::kId = 1;
const int MM2Q::kId = 2;
const int MMTinyLFU::kId = 3;

// AccessType
const int ChainedHashTable::kId = 1;
}
}

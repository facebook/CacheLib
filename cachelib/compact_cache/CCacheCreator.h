#pragma once

/**
 * @author adri
 * @file CCacheCreator.h
 *
 * This file provides two trait classes: CCacheCreator and
 * CCacheVariableCreator that are helpers for creating a compact cache
 * so that the user does not have to know about key type and value descriptor.
 * (see ccache_descriptor.h for more information on value descriptors,
 * and what can be stored in a compact cache).
 *
 * CCacheCreator can be used for creating a compact cache that stores
 * values of a fixed size, or compact caches that do not store values.
 * CCacheVariableCreator can be used for creating a compact cache that
 * stores values of a variable size.
 */

#include "CCache.h"
#include "CCacheDescriptor.h"

namespace facebook {
namespace cachelib {

/****************************************************************************/
/* Helper traits for creating compact cache instances */

/**
 * The following trait makes it easy to create a compact cache without knowing
 * about key type and value descriptor. It infers the key type and value
 * descriptor depending on the types passed to it and uses the default functors
 * created above.
 *
 * Some examples:
 *
 * using MyCCache = CCacheCreator<A, K>::type;
 *     maps a key made of type K to nothing.
 *
 * using MyCCache = CCacheCreator<A, K, V>::type;
 *     maps a key made of type K to a value of type V;
 */
template <typename AllocatorT, typename KeyT, typename ValueT = NoValue>
struct CCacheCreator {
 private:
  /* Create a value descriptor. If Value is an integral type, use
   * CounterValueDescriptor in order to have the compact cache provide the add
   * operation. */
  using ValueDesc = typename std::conditional<std::is_integral<ValueT>::value,
                                              CounterValueDescriptor<ValueT>,
                                              ValueDescriptor<ValueT>>::type;

  /* Create the compact cache descriptor. */
  using Descriptor = CompactCacheDescriptor<KeyT, ValueDesc>;

 public:
  /* Create the compact cache. */
  using type = CompactCache<Descriptor, AllocatorT>;
};

/**
 * The following trait can be used for creating a compact cache that stores
 * values of a variable size.
 *
 * For example:
 *  using MyCCache = CCacheVariableCreator<A, K, 400>::type;
 *     maps a key made of type K to values of a variable size up to 400B.
 */
template <typename AllocatorT, typename KeyT, unsigned MaxValueSize>
struct CCacheVariableCreator {
 private:
  /* Create the value descriptor for a variable size value. */
  using ValueDescriptor = VariableSizedValueDescriptor<MaxValueSize>;

  /* Create the compact cache descriptor. */
  using Descriptor = CompactCacheDescriptor<KeyT, ValueDescriptor>;

 public:
  /* Create the compact cache. */
  using type = CompactCache<Descriptor, AllocatorT>;
};
} // cachelib
} // facebook

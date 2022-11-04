/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::time::Duration;

use anyhow::Error;
use anyhow::Result;
use bytes::BufMut;

use crate::lrucache::VolatileLruCachePool;

/// Utility function for in-memory caching via Abomonation and a volatile pool
/// Will attempt to fetch and decode a cached item
pub fn get_cached<T>(cache_pool: &VolatileLruCachePool, cache_key: &str) -> Result<Option<T>>
where
    T: abomonation::Abomonation + Clone + Send + 'static,
{
    let cache_handle = cache_pool.get_handle(&cache_key)?;
    let cache_data: Option<Vec<u8>> = match cache_handle {
        None => None,
        Some(cache_handle) => {
            let mut bytes = Vec::new();
            bytes.put(cache_handle.get_reader()?);
            Some(bytes)
        }
    };

    Ok(cache_data.map(|mut vec| {
        let (obj, tail) = unsafe { abomonation::decode::<T>(&mut vec) }.expect("Failed to decode");
        assert!(
            tail.is_empty(),
            "Decode did not consume all data, left {}",
            tail.len()
        );
        obj.clone()
    }))
}

/// Encodes an item via Abomonation, and then inserts it into a volatile pool
///
/// Returns `false` if the entry could not be inserted (e.g. another entry with the same
/// key was inserted first)
pub fn set_cached<T>(
    cache_pool: &VolatileLruCachePool,
    cache_key: &str,
    entry: &T,
    ttl: Option<Duration>,
) -> Result<bool>
where
    T: abomonation::Abomonation + Clone + Send + 'static,
{
    let handle = cache_pool.allocate_with_ttl(cache_key, abomonation::measure(entry), ttl)?;
    let mut handle = handle.ok_or_else(|| Error::msg("can not allocate cachelib handle"))?;

    handle
        .get_writer()
        .and_then(|mut writer| unsafe {
            abomonation::encode(entry, &mut writer).map_err(|e| e.into())
        })
        .and_then(|_| cache_pool.insert_handle(handle))
}

#[cfg(test)]
mod test {
    use fbinit::FacebookInit;

    use super::*;
    use crate::lrucache::*;

    fn create_cache(fb: FacebookInit) {
        let config = LruCacheConfig::new(16 * 1024 * 1024);

        if let Err(e) = init_cache(fb, config) {
            panic!("{}", e);
        }
    }

    #[fbinit::test]
    fn get_repeatedly(fb: FacebookInit) {
        create_cache(fb);

        let pool = get_or_create_volatile_pool("get_twice", 4 * 1024 * 1024).unwrap();

        assert!(set_cached(&pool, &"key".to_string(), &"hello_world".to_string(), None).unwrap());
        assert_eq!(
            get_cached::<String>(&pool, &"key".to_string())
                .unwrap()
                .unwrap(),
            "hello_world"
        );

        assert!(
            !set_cached(
                &pool,
                &"key".to_string(),
                &"goodbye world".to_string(),
                Some(Duration::from_secs(100))
            )
            .unwrap()
        );
        assert_eq!(
            get_cached::<String>(&pool, &"key".to_string())
                .unwrap()
                .unwrap(),
            "hello_world"
        );

        assert!(
            set_cached(
                &pool,
                &"key2".to_string(),
                &"hello_world2".to_string(),
                None
            )
            .unwrap()
        );
        assert_eq!(
            get_cached::<String>(&pool, &"key2".to_string())
                .unwrap()
                .unwrap(),
            "hello_world2"
        );
    }
}

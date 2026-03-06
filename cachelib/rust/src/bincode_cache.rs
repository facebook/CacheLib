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

use std::io::Read;
use std::io::Write;
use std::time::Duration;

use anyhow::Error;
use anyhow::Result;
use bytes::Buf;

use crate::lrucache::VolatileLruCachePool;

impl<'a> bincode::de::read::Reader for crate::lrucache::LruCacheHandleReader<'a> {
    fn read(&mut self, bytes: &mut [u8]) -> Result<(), bincode::error::DecodeError> {
        self.read_exact(bytes)
            .map_err(|inner| bincode::error::DecodeError::Io {
                inner,
                additional: bytes.len() - self.remaining(),
            })?;
        Ok(())
    }

    fn peek_read(&mut self, cnt: usize) -> Option<&[u8]> {
        self.peek(cnt)
    }
    fn consume(&mut self, cnt: usize) {
        self.advance(cnt);
    }
}

impl<'a> bincode::enc::write::Writer for crate::lrucache::LruCacheHandleWriter<'a> {
    fn write(&mut self, bytes: &[u8]) -> Result<(), bincode::error::EncodeError> {
        self.write_all(bytes)
            .map_err(|inner| bincode::error::EncodeError::Io {
                inner,
                index: self.position(),
            })?;
        Ok(())
    }
}

const CONFIG: bincode::config::Configuration<
    bincode::config::LittleEndian,
    bincode::config::Fixint,
> = bincode::config::standard().with_fixed_int_encoding();

/// Utility function for in-memory caching via bincode and a volatile pool
/// Will attempt to fetch and decode a cached item
pub fn get_cached<T>(cache_pool: &VolatileLruCachePool, cache_key: &str) -> Result<Option<T>>
where
    T: bincode::Decode<()>,
{
    if let Some(cache_handle) = cache_pool.get_handle(cache_key)? {
        let reader = cache_handle.get_reader()?;
        let buffer_size = reader.remaining();
        let (obj, size) = bincode::decode_from_slice(reader.buffer(), CONFIG)?;
        anyhow::ensure!(
            size == buffer_size,
            "Buffer underrun decoding cache item '{cache_key}': {size} < {buffer_size}"
        );
        Ok(Some(obj))
    } else {
        Ok(None)
    }
}

struct SizeOnlyWriter<'a> {
    bytes_written: &'a mut usize,
}

impl<'a> bincode::enc::write::Writer for SizeOnlyWriter<'a> {
    fn write(&mut self, bytes: &[u8]) -> Result<(), bincode::error::EncodeError> {
        *self.bytes_written += bytes.len();
        Ok(())
    }
}

/// Compute the encoded size of an entry.
fn encoded_size<T, C>(entry: &T, config: C) -> Result<usize, bincode::error::EncodeError>
where
    T: bincode::Encode,
    C: bincode::config::Config,
{
    let mut size = 0usize;
    let writer = SizeOnlyWriter {
        bytes_written: &mut size,
    };
    let mut ei = bincode::enc::EncoderImpl::new(writer, config);
    entry.encode(&mut ei)?;
    Ok(size)
}

/// Encodes an item via bincode, and then inserts it into a volatile pool
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
    T: bincode::Encode,
{
    let size = encoded_size(entry, CONFIG)?;
    let handle = cache_pool.allocate_with_ttl(cache_key, size, ttl)?;
    let mut handle = handle.ok_or_else(|| Error::msg("cannot allocate cachelib handle"))?;
    let mut writer = handle.get_writer()?;
    let written = bincode::encode_into_slice(entry, writer.buffer(), CONFIG)?;
    anyhow::ensure!(
        written == size,
        "Buffer underrun encoding cache item '{cache_key}': {written} < {size}"
    );
    cache_pool.insert_handle(handle)
}

/// Encodes an item via bincode, and then inserts it into a volatile pool, replacing any pre-existing data.
pub fn set_or_replace_cached<T>(
    cache_pool: &VolatileLruCachePool,
    cache_key: &str,
    entry: &T,
    ttl: Option<Duration>,
) -> Result<()>
where
    T: bincode::Encode,
{
    let size = encoded_size(entry, CONFIG)?;
    let handle = cache_pool.allocate_with_ttl(cache_key, size, ttl)?;
    let mut handle = handle.ok_or_else(|| Error::msg("cannot allocate cachelib handle"))?;
    let mut writer = handle.get_writer()?;
    let written = bincode::encode_into_slice(entry, writer.buffer(), CONFIG)?;
    anyhow::ensure!(
        written == size,
        "Buffer underrun encoding cache item '{cache_key}': {written} < {size}"
    );
    cache_pool.insert_or_replace_handle(handle)
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

        assert!(set_cached(&pool, "key", &"hello_world".to_string(), None).unwrap());
        assert_eq!(
            get_cached::<String>(&pool, "key").unwrap().unwrap(),
            "hello_world"
        );

        assert!(
            !set_cached(
                &pool,
                "key",
                &"goodbye world".to_string(),
                Some(Duration::from_secs(100))
            )
            .unwrap()
        );
        assert_eq!(
            get_cached::<String>(&pool, "key").unwrap().unwrap(),
            "hello_world"
        );

        assert!(set_cached(&pool, "key2", &"hello_world2".to_string(), None).unwrap());
        assert_eq!(
            get_cached::<String>(&pool, "key2").unwrap().unwrap(),
            "hello_world2"
        );
    }
}

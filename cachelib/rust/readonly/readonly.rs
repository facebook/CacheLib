/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use cxx::let_cxx_string;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("Failed to attach ReadOnlySharedCacheView: {cxx_exn}")]
pub struct FailedToAttachError {
    #[from]
    cxx_exn: cxx::Exception,
}

#[derive(Debug, Error)]
#[error("Invalid remote item handle: {cxx_exn}")]
pub struct InvalidHandleError {
    #[from]
    cxx_exn: cxx::Exception,
}

#[cxx::bridge(namespace = "facebook::rust::cachelib")]
mod ffi {
    unsafe extern "C++" {
        include!("cachelib/rust/readonly/readonly.h");

        #[namespace = "facebook::cachelib"]
        type ReadOnlySharedCacheView;

        fn ro_cache_view_attach(
            cache_dir: &CxxString,
        ) -> Result<UniquePtr<ReadOnlySharedCacheView>>;
        fn ro_cache_view_attach_at_address(
            cache_dir: &CxxString,
            addr: usize,
        ) -> Result<UniquePtr<ReadOnlySharedCacheView>>;
        fn ro_cache_view_get_shm_mapping_address(cache: &ReadOnlySharedCacheView) -> usize;
        fn ro_cache_view_get_item_ptr_from_offset(
            cache: &ReadOnlySharedCacheView,
            offset: usize,
        ) -> Result<*const u8>;
    }
}

pub struct ReadOnlySharedCacheView {
    cache_view: cxx::UniquePtr<ffi::ReadOnlySharedCacheView>,
}

impl ReadOnlySharedCacheView {
    pub fn new(cache_dir: impl AsRef<Path>) -> Result<Self, FailedToAttachError> {
        let_cxx_string!(cache_dir = cache_dir.as_ref().as_os_str().as_bytes());
        let cache_view = ffi::ro_cache_view_attach(&cache_dir)?;
        Ok(Self { cache_view })
    }

    pub fn new_at_address(
        cache_dir: impl AsRef<Path>,
        addr: *mut std::ffi::c_void,
    ) -> Result<Self, FailedToAttachError> {
        let_cxx_string!(cache_dir = cache_dir.as_ref().as_os_str().as_bytes());
        let cache_view = ffi::ro_cache_view_attach_at_address(&cache_dir, addr as usize)?;
        Ok(Self { cache_view })
    }

    /// Return a byte slice from a (offset, len) pair within the cache.
    /// (offset, len) must be retrieved using [get_remote_handle] from an LruCacheHandle.
    pub fn get_bytes_from_offset<'a>(
        &'a self,
        offset: usize,
        len: usize,
    ) -> Result<&'a [u8], InvalidHandleError> {
        let item_ptr = ffi::ro_cache_view_get_item_ptr_from_offset(&*self.cache_view, offset)?;
        Ok(unsafe { std::slice::from_raw_parts(item_ptr, len) })
    }

    pub fn shm_mapping_address(&self) -> usize {
        let addr = ffi::ro_cache_view_get_shm_mapping_address(&*self.cache_view);
        if addr == 0 {
            // This shouldn't happen--the whole point of ReadOnlySharedCacheView
            // is that it's a view into a cache's shared memory, and
            // getShmMappingAddress should return nullptr only when the cache is
            // not using shared memory.
            panic!("ReadOnlySharedCacheView returned null shm_mapping_address")
        } else {
            addr
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::path::PathBuf;
    use std::time::Duration;

    use anyhow::Result;
    use bytes::Bytes;
    use cachelib::*;
    use fbinit::FacebookInit;
    use tempdir::TempDir;

    fn create_temp_dir(dir_prefix: &str) -> TempDir {
        TempDir::new(dir_prefix).expect("failed to create temp dir")
    }

    fn create_shared_cache(fb: FacebookInit, cache_directory: PathBuf) {
        let config = LruCacheConfig::new(128 * 1024 * 1024)
            .set_shrinker(ShrinkMonitor {
                shrinker_type: ShrinkMonitorType::ResidentSize {
                    max_process_size_gib: 16,
                    min_process_size_gib: 1,
                },
                interval: Duration::new(1, 0),
                max_resize_per_iteration_percent: 10,
                max_removed_percent: 90,
                strategy: RebalanceStrategy::LruTailAge {
                    age_difference_ratio: 0.1,
                    min_retained_slabs: 1,
                },
            })
            .set_pool_resizer(PoolResizeConfig {
                interval: Duration::new(1, 0),
                slabs_per_iteration: 100,
                strategy: RebalanceStrategy::LruTailAge {
                    age_difference_ratio: 0.1,
                    min_retained_slabs: 1,
                },
            })
            .set_cache_dir(cache_directory)
            .set_pool_rebalance(PoolRebalanceConfig {
                interval: Duration::new(1, 0),
                strategy: RebalanceStrategy::LruTailAge {
                    age_difference_ratio: 0.1,
                    min_retained_slabs: 1,
                },
            });

        if let Err(e) = init_cache(fb, config) {
            panic!("{}", e);
        }
    }

    #[fbinit::test]
    fn test_readonly_shared_cache(fb: FacebookInit) -> Result<()> {
        let temp_dir = create_temp_dir("test_shared_cache");
        create_shared_cache(fb, temp_dir.path().into());

        // Set value in original cache
        let pool = get_or_create_pool("find_pool_by_name", 4 * 1024 * 1024)?;
        let value = b"I am a fish";
        pool.set(b"test", Bytes::from(value.as_ref()))?;

        let test_handle = pool.get_handle(b"test")?.unwrap();
        let remote_handle = test_handle.get_remote_handle()?;

        // Get value from read-only cache
        let ro_cache_view = ReadOnlySharedCacheView::new(&temp_dir.path())?;
        let slice = ro_cache_view
            .get_bytes_from_offset(remote_handle.get_offset(), remote_handle.get_length())?;
        let reader_bytes = Bytes::copy_from_slice(slice);

        // Verify that value is the same
        assert_eq!(
            reader_bytes,
            Bytes::from(b"I am a fish".as_ref()),
            "Data does not match!"
        );

        Ok(())
    }

    #[test]
    fn test_non_existent_cache_dir() {
        let temp_dir = create_temp_dir("test_non_existent_cache_dir");

        let mut path = temp_dir.path().to_owned();
        path.push("this_dir_does_not_exist");

        match ReadOnlySharedCacheView::new(&path) {
            Ok(_) => panic!("ReadOnlySharedCacheView::new returned Ok for non-existent dir"),
            Err(FailedToAttachError { .. }) => {}
        }
    }
}

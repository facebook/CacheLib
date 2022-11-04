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

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ErrorKind {
    #[error("Not in a supported container type, but container config requested")]
    NotInContainer,
    #[error(
        "You must call cachelib::init_cache or init_cache_with_cacheadmin before using the cache"
    )]
    CacheNotInitialized,
    #[error("Get available space failed: {0}")]
    AvailableSpaceError(String),
    #[error("Cannot access cache memory: {0}")]
    CannotGetMemory(String),
    #[error("Handle insertion failed: {0}")]
    HandleInsertFail(String),
    #[error("Cannot allocate handle")]
    HandleAllocateFail,
    #[error("Cannot get handle: {0}")]
    HandleGetFail(String),
    #[error("Cannot get handle as offset: {0}")]
    HandleGetOffsetFail(String),
    #[error("Failed to get pointer from offset: {0}")]
    PointerFromOffsetFail(String),
    #[error("Pool resize failure: {0}")]
    PoolResizeFailure(String),
    #[error("CacheAdmin failure: {0}")]
    CacheAdminFailure(String),
    #[error("Bad access config: {0}")]
    BadAccessConfig(String),
    #[error("Bad persistence config: {0}")]
    BadPersistenceConfig(String),
    #[error("Requested a volatile cache pool from a persistent cache.")]
    VolatileCachePoolError,
}

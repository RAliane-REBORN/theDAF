//! Tier-aware cache hierarchy.
//!
//! - `MemoryCache` (L1): in-memory cache with LRU eviction and trie-based prefix operations.
//! - `MokaCache` (L2): concurrent process cache with prefix index for invalidation.
//! - `RedisCache` (L3): distributed shared cache behind `redis` feature flag.
//! - `SledCache` (L4): durable local materialized state behind `sled` feature flag.
//!
//! Feature compilation (`--all-features clippy`) proves backend behavior.

use std::any::Any;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::RwLock;

pub mod hierarchical;
pub mod moka;
#[cfg(feature = "redis")]
pub mod redis;
#[cfg(feature = "sled")]
pub mod sled;
pub mod trie;

pub use crate::hierarchical::HierarchicalCache;
pub use crate::moka::MokaCache;
#[cfg(feature = "redis")]
pub use crate::redis::RedisCache;
#[cfg(feature = "sled")]
pub use crate::sled::SledCache;
pub use crate::trie::{
    astar_collect, bfs_collect, dfs_collect, trie_collect, trie_delete, trie_delete_prefix,
    trie_insert, AStarEntry, TrieNode,
};

use async_trait::async_trait;
use daf_core::{CacheEntry, CacheError, Tier};

#[derive(Debug, Clone)]
pub struct MemoryCache {
    inner: Arc<RwLock<MemoryCacheInner>>,
}

#[derive(Debug)]
struct MemoryCacheInner {
    cache: HashMap<String, CacheEntry>,
    lru: lru::LruCache<String, ()>,
    trie: TrieNode,
    max_size: usize,
}

impl MemoryCache {
    pub fn new(max_size: usize) -> Self {
        let lru = if max_size > 0 {
            let nonzero = match NonZeroUsize::new(max_size) {
                Some(nz) => nz,
                None => unreachable!(),
            };
            lru::LruCache::new(nonzero)
        } else {
            lru::LruCache::unbounded()
        };
        let inner = MemoryCacheInner {
            cache: HashMap::new(),
            lru,
            trie: TrieNode::default(),
            max_size,
        };
        debug_assert!(inner.cache.is_empty(), "new MemoryCache must start empty");
        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    pub async fn get(&self, key: &str) -> Result<Option<CacheEntry>, CacheError> {
        debug_assert!(!key.is_empty(), "cache key must not be empty");
        let mut inner = self.inner.write().await;
        if let Some(entry) = inner.cache.get(key).cloned() {
            if inner.max_size > 0 {
                inner.lru.promote(key);
            }
            return Ok(Some(entry));
        }
        Ok(None)
    }

    pub async fn set(
        &self,
        key: String,
        value: Arc<dyn Any + Send + Sync>,
    ) -> Result<(), CacheError> {
        debug_assert!(!key.is_empty(), "cache key must not be empty");
        let mut inner = self.inner.write().await;
        let entry = CacheEntry {
            value,
            origin_tier: Tier::L1,
        };
        if inner.cache.contains_key(&key) {
            if inner.max_size > 0 {
                inner.lru.promote(&key);
            }
        } else {
            if inner.max_size > 0 && inner.cache.len() >= inner.max_size {
                self.evict_oldest(&mut inner);
            }
            if inner.max_size > 0 {
                inner.lru.put(key.clone(), ());
            }
        }
        inner.cache.insert(key.clone(), entry);
        trie_insert(&mut inner.trie, &key);
        Ok(())
    }

    pub async fn delete(&self, key: &str) -> Result<(), CacheError> {
        debug_assert!(!key.is_empty(), "cache key must not be empty");
        let mut inner = self.inner.write().await;
        if inner.cache.contains_key(key) {
            trie_delete(&mut inner.trie, key);
            inner.cache.remove(key);
            if inner.max_size > 0 {
                inner.lru.pop(key);
            }
        }
        Ok(())
    }

    pub async fn delete_prefix(&self, prefix: &str) -> Result<u64, CacheError> {
        debug_assert!(
            !prefix.is_empty(),
            "prefix must not be empty for delete_prefix"
        );
        let mut inner = self.inner.write().await;
        let keys = trie_delete_prefix(&mut inner.trie, prefix);
        let count = keys.len();
        for key in &keys {
            inner.cache.remove(key);
            if inner.max_size > 0 {
                inner.lru.pop(key);
            }
        }
        Ok(count as u64)
    }

    pub async fn shake(&self, prefix: &str) -> Result<usize, CacheError> {
        debug_assert!(!prefix.is_empty(), "prefix must not be empty for shake");
        let mut inner = self.inner.write().await;
        let keys = trie_delete_prefix(&mut inner.trie, prefix);
        for key in &keys {
            inner.cache.remove(key);
            if inner.max_size > 0 {
                inner.lru.pop(key);
            }
        }
        Ok(keys.len())
    }

    pub async fn clear(&self) -> Result<(), CacheError> {
        let mut inner = self.inner.write().await;
        inner.cache.clear();
        inner.trie = TrieNode::default();
        inner.lru.clear();
        Ok(())
    }

    pub async fn has(&self, key: &str) -> bool {
        debug_assert!(!key.is_empty(), "cache key must not be empty");
        let inner = self.inner.read().await;
        inner.cache.contains_key(key)
    }

    pub fn _dfs_collect(&self) -> std::collections::HashSet<String> {
        let inner = self.inner.blocking_read();
        dfs_collect(Some(&inner.trie))
    }

    pub fn _bfs_collect(&self) -> std::collections::HashSet<String> {
        let inner = self.inner.blocking_read();
        bfs_collect(&inner.trie)
    }

    pub fn _astar_collect(&self, target: &str) -> std::collections::HashSet<String> {
        debug_assert!(!target.is_empty(), "astar target must not be empty");
        let inner = self.inner.blocking_read();
        astar_collect(&inner.trie, target)
    }

    pub async fn _trie_collect(&self, prefix: &str) -> std::collections::HashSet<String> {
        debug_assert!(!prefix.is_empty(), "trie prefix must not be empty");
        let inner = self.inner.read().await;
        trie_collect(&inner.trie, prefix)
    }

    pub fn _trie_delete_prefix(&self, prefix: &str) -> std::collections::HashSet<String> {
        debug_assert!(!prefix.is_empty(), "trie delete prefix must not be empty");
        let mut inner = self.inner.blocking_write();
        trie_delete_prefix(&mut inner.trie, prefix)
    }

    fn evict_oldest(&self, inner: &mut MemoryCacheInner) {
        debug_assert!(inner.max_size > 0, "eviction requires bounded cache");
        if let Some((key, _)) = inner.lru.pop_lru() {
            trie_delete(&mut inner.trie, &key);
            inner.cache.remove(&key);
        }
    }
}

#[async_trait]
impl daf_core::Cache for MemoryCache {
    async fn get(&self, key: &str) -> Result<Option<CacheEntry>, CacheError> {
        MemoryCache::get(self, key).await
    }

    async fn set(&self, key: String, value: Arc<dyn Any + Send + Sync>) -> Result<(), CacheError> {
        MemoryCache::set(self, key, value).await
    }

    async fn delete(&self, key: &str) -> Result<(), CacheError> {
        MemoryCache::delete(self, key).await
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<u64, CacheError> {
        MemoryCache::delete_prefix(self, prefix).await
    }

    async fn shake(&self, prefix: &str) -> Result<usize, CacheError> {
        MemoryCache::shake(self, prefix).await
    }

    async fn clear(&self) -> Result<(), CacheError> {
        MemoryCache::clear(self).await
    }

    fn tier(&self) -> daf_core::Tier {
        daf_core::Tier::L1
    }
}

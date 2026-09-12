use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use daf_core::{Cache, CacheEntry, CacheError, Tier};
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct MokaCache {
    inner: moka::future::Cache<String, CacheEntry>,
    prefix_index: Arc<RwLock<HashMap<String, Vec<String>>>>,
}

impl MokaCache {
    pub fn new(max_capacity: u64) -> Self {
        debug_assert!(max_capacity > 0, "moka max_capacity must be positive");
        Self {
            inner: moka::future::Cache::new(max_capacity),
            prefix_index: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl Cache for MokaCache {
    async fn get(&self, key: &str) -> Result<Option<CacheEntry>, CacheError> {
        debug_assert!(!key.is_empty(), "cache key must not be empty");
        Ok(self.inner.get(key).await)
    }

    async fn set(
        &self,
        key: String,
        value: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Result<(), CacheError> {
        debug_assert!(!key.is_empty(), "cache key must not be empty");
        let entry = CacheEntry {
            value,
            origin_tier: Tier::L2,
        };
        self.inner.insert(key.clone(), entry).await;
        let mut index = self.prefix_index.write().await;
        for i in 1..=key.len() {
            index
                .entry(key[..i].to_string())
                .or_default()
                .push(key.clone());
        }
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), CacheError> {
        debug_assert!(!key.is_empty(), "cache key must not be empty");
        self.inner.invalidate(key).await;
        let mut index = self.prefix_index.write().await;
        for i in 1..=key.len() {
            if let Some(keys) = index.get_mut(&key[..i].to_string()) {
                keys.retain(|k| k != key);
                if keys.is_empty() {
                    index.remove(&key[..i].to_string());
                }
            }
        }
        Ok(())
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<u64, CacheError> {
        debug_assert!(
            !prefix.is_empty(),
            "prefix must not be empty for delete_prefix"
        );
        let keys_to_remove: Vec<String> = {
            let index = self.prefix_index.read().await;
            index.get(prefix).cloned().unwrap_or_default()
        };
        if keys_to_remove.is_empty() {
            return Err(CacheError::new("prefix not found"));
        }
        let unique_keys: std::collections::HashSet<String> = keys_to_remove.into_iter().collect();
        let count = unique_keys.len();
        for key in &unique_keys {
            self.inner.invalidate(key).await;
        }
        {
            let mut index = self.prefix_index.write().await;
            for key in unique_keys {
                for i in 1..=key.len() {
                    if let Some(keys) = index.get_mut(&key[..i].to_string()) {
                        keys.retain(|k| k != &key);
                        if keys.is_empty() {
                            index.remove(&key[..i].to_string());
                        }
                    }
                }
            }
        }
        Ok(count as u64)
    }

    async fn shake(&self, prefix: &str) -> Result<usize, CacheError> {
        self.delete_prefix(prefix).await.map(|n| n as usize)
    }

    async fn clear(&self) -> Result<(), CacheError> {
        self.inner.invalidate_all();
        self.prefix_index.write().await.clear();
        Ok(())
    }

    fn tier(&self) -> Tier {
        Tier::L2
    }
}

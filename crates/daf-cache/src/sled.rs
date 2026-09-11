use std::sync::Arc;

use async_trait::async_trait;
use daf_core::{Cache, CacheEntry, CacheError, Tier};

#[derive(Debug, Clone)]
pub struct SledCache {
    db: sled::Db,
}

impl SledCache {
    pub fn new(db: sled::Db) -> Self {
        Self { db }
    }
}

#[async_trait]
impl Cache for SledCache {
    async fn get(&self, key: &str) -> Result<Option<CacheEntry>, CacheError> {
        let result = self
            .db
            .get(key)
            .map_err(|e| CacheError::new(format!("sled get error: {}", e)))?;
        match result {
            Some(ivec) => {
                let bytes = ivec.to_vec();
                Ok(Some(CacheEntry {
                    value: Arc::new(bytes),
                    origin_tier: Tier::L4,
                }))
            }
            None => Ok(None),
        }
    }

    async fn set(
        &self,
        key: String,
        value: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Result<(), CacheError> {
        let bytes = value.downcast::<Vec<u8>>().map_err(|_| {
            CacheError::new("SledCache requires Vec<u8> values; serialize before set")
        })?;
        self.db
            .insert(key, bytes.as_ref())
            .map_err(|e| CacheError::new(format!("sled insert error: {}", e)))?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), CacheError> {
        self.db
            .remove(key)
            .map_err(|e| CacheError::new(format!("sled remove error: {}", e)))?;
        Ok(())
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<u64, CacheError> {
        let mut count = 0u64;
        for item in self.db.scan_prefix(prefix) {
            let (key, _) = item.map_err(|e| CacheError::new(format!("sled scan error: {}", e)))?;
            self.db
                .remove(key.clone())
                .map_err(|e| CacheError::new(format!("sled remove error: {}", e)))?;
            count += 1;
        }
        Ok(count)
    }

    async fn shake(&self, prefix: &str) -> Result<usize, CacheError> {
        self.delete_prefix(prefix).await.map(|n| n as usize)
    }

    async fn clear(&self) -> Result<(), CacheError> {
        self.db
            .clear()
            .map_err(|e| CacheError::new(format!("sled clear error: {}", e)))?;
        Ok(())
    }

    fn tier(&self) -> Tier {
        Tier::L4
    }
}

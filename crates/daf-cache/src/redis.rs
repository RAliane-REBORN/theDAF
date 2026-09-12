use std::sync::Arc;

use async_trait::async_trait;
use daf_core::{Cache, CacheEntry, CacheError, Tier};

#[derive(Debug, Clone)]
pub struct RedisCache {
    conn: redis::aio::ConnectionManager,
}

impl RedisCache {
    pub async fn new(client: redis::Client) -> Result<Self, CacheError> {
        let conn = client
            .get_connection_manager()
            .await
            .map_err(|e| CacheError::new(format!("redis connection manager error: {}", e)))?;
        Ok(Self { conn })
    }
}

#[async_trait]
impl Cache for RedisCache {
    async fn get(&self, key: &str) -> Result<Option<CacheEntry>, CacheError> {
        let mut conn = self.conn.clone();
        let bytes: Option<Vec<u8>> = conn
            .get(key)
            .await
            .map_err(|e| CacheError::new(format!("redis get error: {}", e)))?;
        Ok(bytes.map(|b| CacheEntry {
            value: Arc::new(b),
            origin_tier: Tier::L3,
        }))
    }

    async fn set(
        &self,
        key: String,
        value: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Result<(), CacheError> {
        let bytes = value.downcast::<Vec<u8>>().map_err(|_| {
            CacheError::new("RedisCache requires Vec<u8> values; serialize before set")
        })?;
        let mut conn = self.conn.clone();
        conn.set(&key, bytes.as_ref())
            .await
            .map_err(|e| CacheError::new(format!("redis set error: {}", e)))?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), CacheError> {
        let mut conn = self.conn.clone();
        conn.del(key)
            .await
            .map_err(|e| CacheError::new(format!("redis del error: {}", e)))?;
        Ok(())
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<u64, CacheError> {
        let mut conn = self.conn.clone();
        let keys: Vec<String> = conn
            .keys(prefix)
            .await
            .map_err(|e| CacheError::new(format!("redis keys error: {}", e)))?;
        let count = keys.len();
        if !keys.is_empty() {
            conn.del(keys)
                .await
                .map_err(|e| CacheError::new(format!("redis del error: {}", e)))?;
        }
        Ok(count as u64)
    }

    async fn shake(&self, prefix: &str) -> Result<usize, CacheError> {
        self.delete_prefix(prefix).await.map(|n| n as usize)
    }

    async fn clear(&self) -> Result<(), CacheError> {
        let mut conn = self.conn.clone();
        conn.flushdb::<()>()
            .await
            .map_err(|e| CacheError::new(format!("redis flushdb error: {}", e)))?;
        Ok(())
    }

    fn tier(&self) -> Tier {
        Tier::L3
    }
}

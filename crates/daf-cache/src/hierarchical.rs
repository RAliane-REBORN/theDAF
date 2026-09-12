use std::fmt;

use std::sync::Arc;

use async_trait::async_trait;
use daf_core::{Cache, CacheEntry, CacheError};

#[derive(Clone)]
pub struct HierarchicalCache {
    l0: Option<Arc<dyn Cache>>,
    l1: Arc<dyn Cache>,
    l2: Arc<dyn Cache>,
    l3: Arc<dyn Cache>,
    l4: Arc<dyn Cache>,
    l5: Option<Arc<dyn Cache>>,
}

impl HierarchicalCache {
    pub fn new(
        l0: Option<Arc<dyn Cache>>,
        l1: Arc<dyn Cache>,
        l2: Arc<dyn Cache>,
        l3: Arc<dyn Cache>,
        l4: Arc<dyn Cache>,
        l5: Option<Arc<dyn Cache>>,
    ) -> Self {
        Self {
            l0,
            l1,
            l2,
            l3,
            l4,
            l5,
        }
    }

    pub fn l0(&self) -> Option<&Arc<dyn Cache>> {
        self.l0.as_ref()
    }

    pub fn l1(&self) -> &Arc<dyn Cache> {
        debug_assert!(
            Arc::strong_count(&self.l1) > 0,
            "l1 cache Arc must be valid"
        );
        &self.l1
    }

    pub fn l2(&self) -> &Arc<dyn Cache> {
        debug_assert!(
            Arc::strong_count(&self.l2) > 0,
            "l2 cache Arc must be valid"
        );
        &self.l2
    }

    pub fn l3(&self) -> &Arc<dyn Cache> {
        debug_assert!(
            Arc::strong_count(&self.l3) > 0,
            "l3 cache Arc must be valid"
        );
        &self.l3
    }

    pub fn l4(&self) -> &Arc<dyn Cache> {
        debug_assert!(
            Arc::strong_count(&self.l4) > 0,
            "l4 cache Arc must be valid"
        );
        &self.l4
    }

    pub fn l5(&self) -> Option<&Arc<dyn Cache>> {
        self.l5.as_ref()
    }
}

impl fmt::Debug for HierarchicalCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HierarchicalCache")
            .field("l0", &self.l0.as_ref().map(|_| "Arc<dyn Cache>"))
            .field("l1", &"Arc<dyn Cache>")
            .field("l2", &"Arc<dyn Cache>")
            .field("l3", &"Arc<dyn Cache>")
            .field("l4", &"Arc<dyn Cache>")
            .field("l5", &self.l5.as_ref().map(|_| "Arc<dyn Cache>"))
            .finish()
    }
}

#[async_trait]
impl Cache for HierarchicalCache {
    async fn get(&self, key: &str) -> Result<Option<CacheEntry>, CacheError> {
        debug_assert!(!key.is_empty(), "cache key must not be empty");
        let l0 = self.l0.as_ref().ok_or_else(|| {
            CacheError::new("L0 cache tier is not configured")
        })?;
        let l5 = self.l5.as_ref().ok_or_else(|| {
            CacheError::new("L5 cache tier is not configured")
        })?;
        let tiers: [&Arc<dyn Cache>; 6] =
            [l0, &self.l1, &self.l2, &self.l3, &self.l4, l5];
        for (i, tier) in tiers.iter().enumerate() {
            match tier.get(key).await {
                Ok(Some(e)) => {
                    if i > 0 {
                        self.l1
                            .set(key.to_string(), Arc::clone(&e.value))
                            .await?;
                    }
                    return Ok(Some(e));
                }
                Ok(None) => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(None)
    }

    async fn set(
        &self,
        key: String,
        value: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Result<(), CacheError> {
        debug_assert!(!key.is_empty(), "cache key must not be empty");
        if let Some(l0) = &self.l0 {
            l0.set(key.clone(), Arc::clone(&value)).await?;
        }
        self.l1.set(key, value).await
    }

    async fn delete(&self, key: &str) -> Result<(), CacheError> {
        debug_assert!(!key.is_empty(), "cache key must not be empty");
        let l0 = self.l0.as_ref().ok_or_else(|| {
            CacheError::new("L0 cache tier is not configured")
        })?;
        let l5 = self.l5.as_ref().ok_or_else(|| {
            CacheError::new("L5 cache tier is not configured")
        })?;
        let tiers: [&Arc<dyn Cache>; 6] =
            [l0, &self.l1, &self.l2, &self.l3, &self.l4, l5];
        for tier in tiers {
            tier.delete(key).await?;
        }
        Ok(())
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<u64, CacheError> {
        debug_assert!(
            !prefix.is_empty(),
            "prefix must not be empty for delete_prefix"
        );
        let l0 = self.l0.as_ref().ok_or_else(|| {
            CacheError::new("L0 cache tier is not configured")
        })?;
        let l5 = self.l5.as_ref().ok_or_else(|| {
            CacheError::new("L5 cache tier is not configured")
        })?;
        let tiers: [&Arc<dyn Cache>; 6] =
            [l0, &self.l1, &self.l2, &self.l3, &self.l4, l5];
        let mut total: u64 = 0;
        for tier in tiers {
            total += tier.delete_prefix(prefix).await?;
        }
        Ok(total)
    }

    async fn clear(&self) -> Result<(), CacheError> {
        let l0 = self.l0.as_ref().ok_or_else(|| {
            CacheError::new("L0 cache tier is not configured")
        })?;
        let l5 = self.l5.as_ref().ok_or_else(|| {
            CacheError::new("L5 cache tier is not configured")
        })?;
        let tiers: [&Arc<dyn Cache>; 6] =
            [l0, &self.l1, &self.l2, &self.l3, &self.l4, l5];
        for tier in tiers {
            tier.clear().await?;
        }
        Ok(())
    }

    async fn shake(&self, prefix: &str) -> Result<usize, CacheError> {
        debug_assert!(!prefix.is_empty(), "prefix must not be empty for shake");
        let l0 = self.l0.as_ref().ok_or_else(|| {
            CacheError::new("L0 cache tier is not configured")
        })?;
        let l5 = self.l5.as_ref().ok_or_else(|| {
            CacheError::new("L5 cache tier is not configured")
        })?;
        let tiers: [&Arc<dyn Cache>; 6] =
            [l0, &self.l1, &self.l2, &self.l3, &self.l4, l5];
        let mut total: usize = 0;
        for tier in tiers {
            total += tier.shake(prefix).await?;
        }
        Ok(total)
    }

    fn tier(&self) -> daf_core::Tier {
        daf_core::Tier::L1
    }
}

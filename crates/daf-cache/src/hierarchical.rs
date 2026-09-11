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
        let tiers: [&Arc<dyn Cache>; 6] = [
            self.l0.as_ref().unwrap_or(&self.l1),
            &self.l1,
            &self.l2,
            &self.l3,
            &self.l4,
            self.l5.as_ref().unwrap_or(&self.l4),
        ];
        for (i, tier) in tiers.iter().enumerate() {
            match tier.get(key).await {
                Ok(Some(e)) => {
                    if i > 0 {
                        if let Err(promo_err) =
                            self.l1.set(key.to_string(), Arc::clone(&e.value)).await
                        {
                            tracing::warn!(
                                tier = %i,
                                error = %promo_err,
                                "l1 promotion failed; serving value without caching at l1"
                            );
                        }
                    }
                    return Ok(Some(e));
                }
                Ok(None) => continue,
                Err(e) => {
                    tracing::warn!(tier = %i, error = %e, "tier read error; falling through");
                    continue;
                }
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
            if let Err(e) = l0.set(key.clone(), Arc::clone(&value)).await {
                tracing::warn!(error = %e, "l0 set degraded; continuing");
            }
        }
        self.l1.set(key, value).await
    }

    async fn delete(&self, key: &str) -> Result<(), CacheError> {
        debug_assert!(!key.is_empty(), "cache key must not be empty");
        let tiers: [&Arc<dyn Cache>; 6] = [
            self.l0.as_ref().unwrap_or(&self.l1),
            &self.l1,
            &self.l2,
            &self.l3,
            &self.l4,
            self.l5.as_ref().unwrap_or(&self.l4),
        ];
        for tier in tiers {
            if let Err(e) = tier.delete(key).await {
                tracing::warn!(error = %e, "tier delete degraded; continuing");
            }
        }
        Ok(())
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<u64, CacheError> {
        debug_assert!(
            !prefix.is_empty(),
            "prefix must not be empty for delete_prefix"
        );
        let tiers: [&Arc<dyn Cache>; 6] = [
            self.l0.as_ref().unwrap_or(&self.l1),
            &self.l1,
            &self.l2,
            &self.l3,
            &self.l4,
            self.l5.as_ref().unwrap_or(&self.l4),
        ];
        let mut total: u64 = 0;
        for tier in tiers {
            match tier.delete_prefix(prefix).await {
                Ok(n) => total += n,
                Err(e) => tracing::warn!(error = %e, "tier delete_prefix degraded; continuing"),
            }
        }
        Ok(total)
    }

    async fn clear(&self) -> Result<(), CacheError> {
        let tiers: [&Arc<dyn Cache>; 6] = [
            self.l0.as_ref().unwrap_or(&self.l1),
            &self.l1,
            &self.l2,
            &self.l3,
            &self.l4,
            self.l5.as_ref().unwrap_or(&self.l4),
        ];
        for tier in tiers {
            if let Err(e) = tier.clear().await {
                tracing::warn!(error = %e, "tier clear degraded; continuing");
            }
        }
        Ok(())
    }

    async fn shake(&self, prefix: &str) -> Result<usize, CacheError> {
        debug_assert!(!prefix.is_empty(), "prefix must not be empty for shake");
        let tiers: [&Arc<dyn Cache>; 6] = [
            self.l0.as_ref().unwrap_or(&self.l1),
            &self.l1,
            &self.l2,
            &self.l3,
            &self.l4,
            self.l5.as_ref().unwrap_or(&self.l4),
        ];
        let mut total: usize = 0;
        for tier in tiers {
            match tier.shake(prefix).await {
                Ok(n) => total += n,
                Err(e) => tracing::warn!(error = %e, "tier shake degraded; continuing"),
            }
        }
        Ok(total)
    }

    fn tier(&self) -> daf_core::Tier {
        daf_core::Tier::L1
    }
}

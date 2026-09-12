use std::collections::HashMap;
use std::sync::Arc;

use ::hex;
use async_trait::async_trait;
use daf_application::DataAccess;
use daf_application::DataAccessFactory;
use daf_cache::MemoryCache;
use daf_core::{
    AuthorizationError, Authorizer, Cache, DeleteInfo, Generation, JsonValue, PostInfo, PutInfo,
    QueryInfo, Repository, RepositoryError, ResourceId, Tier, UserId,
};
use daf_repository::MemoryRepository;
use sha2::{Digest, Sha256};

struct FakeAuthorizer {
    owned: HashMap<String, String>,
}

#[async_trait]
impl Authorizer for FakeAuthorizer {
    async fn authorize(
        &self,
        _operation: &str,
        resource_id: Option<&ResourceId>,
        user: Option<&UserId>,
        _data: Option<Arc<dyn std::any::Any + Send + Sync>>,
    ) -> Result<(), AuthorizationError> {
        if user.is_none() {
            return Err(AuthorizationError::new("Unauthenticated"));
        }
        let rid = resource_id.map(|r| r.0.clone()).unwrap_or_default();
        if let Some(owner) = self.owned.get(&rid) {
            let uid = &user.unwrap().0;
            if owner != uid {
                return Err(AuthorizationError::new("Access denied"));
            }
        }
        Ok(())
    }
}

fn make_daf(
    repo: Arc<dyn Repository<JsonValue>>,
    cache: Arc<dyn daf_core::Cache>,
    authorizer: Option<Arc<dyn Authorizer>>,
) -> DataAccess {
    DataAccess::new(repo, cache, None, authorizer)
}

async fn save(repo: &Arc<MemoryRepository<JsonValue>>, id: &str, data: HashMap<String, JsonValue>) {
    repo.save(
        &ResourceId::new(id),
        JsonValue::Object(data.into_iter().collect()),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn test_authorization_cache_isolation() {
    let repo = Arc::new(MemoryRepository::<JsonValue>::new());
    let cache = Arc::new(MemoryCache::new(1024));
    let auth: Arc<dyn Authorizer> = Arc::new(FakeAuthorizer {
        owned: HashMap::from([("123".to_string(), "user-1".to_string())]),
    });
    let daf = make_daf(
        repo.clone() as Arc<dyn Repository<JsonValue>>,
        cache.clone() as Arc<dyn daf_core::Cache>,
        Some(auth),
    );

    save(
        &repo,
        "123",
        HashMap::from([("name".to_string(), JsonValue::String("John".to_string()))]),
    )
    .await;

    let r1 = daf
        .query(
            QueryInfo {
                resource_id: ResourceId::new("123"),
                filters: None,
                algorithm: None,
            },
            Some(&UserId::new("user-1")),
        )
        .await
        .unwrap();
    assert!(r1.success);
    assert!(!r1.cache_hit);

    let r2 = daf
        .query(
            QueryInfo {
                resource_id: ResourceId::new("123"),
                filters: None,
                algorithm: None,
            },
            Some(&UserId::new("user-1")),
        )
        .await
        .unwrap();
    assert!(r2.cache_hit);

    let err = daf
        .query(
            QueryInfo {
                resource_id: ResourceId::new("123"),
                filters: None,
                algorithm: None,
            },
            Some(&UserId::new("user-2")),
        )
        .await;
    assert!(err.is_err());
}

#[tokio::test]
async fn test_prefix_invalidation_clears_all_projections() {
    let repo = Arc::new(MemoryRepository::<JsonValue>::new());
    let cache = Arc::new(MemoryCache::new(1024));
    let daf = make_daf(
        repo.clone() as Arc<dyn Repository<JsonValue>>,
        cache.clone() as Arc<dyn daf_core::Cache>,
        None,
    );

    save(
        &repo,
        "123",
        HashMap::from([("name".to_string(), JsonValue::String("John".to_string()))]),
    )
    .await;

    daf.query(
        QueryInfo {
            resource_id: ResourceId::new("123"),
            filters: None,
            algorithm: None,
        },
        None,
    )
    .await
    .unwrap();

    let keys_before = cache._trie_collect("query:").await;
    assert_eq!(keys_before.len(), 1);

    daf.put(
        PutInfo {
            resource_id: ResourceId::new("123"),
            data: HashMap::from([("name".to_string(), JsonValue::String("Jane".to_string()))]),
        },
        None,
    )
    .await
    .unwrap();

    let keys_after = cache._trie_collect("query:").await;
    assert!(keys_after.is_empty());
}

#[tokio::test]
async fn test_stale_cache_entry_rejected_after_mutation() {
    let repo = Arc::new(MemoryRepository::<JsonValue>::new());
    let cache = Arc::new(MemoryCache::new(1024));
    let daf = make_daf(
        repo.clone() as Arc<dyn Repository<JsonValue>>,
        cache.clone() as Arc<dyn daf_core::Cache>,
        None,
    );

    save(
        &repo,
        "123",
        HashMap::from([("name".to_string(), JsonValue::String("John".to_string()))]),
    )
    .await;

    let r1 = daf
        .query(
            QueryInfo {
                resource_id: ResourceId::new("123"),
                filters: None,
                algorithm: None,
            },
            None,
        )
        .await
        .unwrap();
    assert!(r1.success);
    assert!(!r1.cache_hit);

    daf.put(
        PutInfo {
            resource_id: ResourceId::new("123"),
            data: HashMap::from([("name".to_string(), JsonValue::String("Jane".to_string()))]),
        },
        None,
    )
    .await
    .unwrap();

    let r2 = daf
        .query(
            QueryInfo {
                resource_id: ResourceId::new("123"),
                filters: None,
                algorithm: None,
            },
            None,
        )
        .await
        .unwrap();
    assert!(r2.success);
    assert!(!r2.cache_hit);
    assert_eq!(
        r2.data,
        Some(JsonValue::Object(
            [("name".to_string(), JsonValue::String("Jane".to_string()))]
                .into_iter()
                .collect()
        ))
    );
}

#[tokio::test]
async fn test_not_found_error_for_missing_resource() {
    let repo = Arc::new(MemoryRepository::<JsonValue>::new());
    let cache = Arc::new(MemoryCache::new(1024));
    let daf = make_daf(repo, cache, None);
    let err = daf
        .query(
            QueryInfo {
                resource_id: ResourceId::new("nonexistent"),
                filters: None,
                algorithm: None,
            },
            None,
        )
        .await;
    assert!(err.is_err());
}

#[tokio::test]
async fn test_post_creates_resource() {
    let repo = Arc::new(MemoryRepository::<JsonValue>::new());
    let cache = Arc::new(MemoryCache::new(1024));
    let daf = make_daf(repo, cache, None);
    let result = daf
        .post(
            PostInfo {
                resource_type: "user".to_string(),
                data: HashMap::from([("name".to_string(), JsonValue::String("John".to_string()))]),
            },
            None,
        )
        .await
        .unwrap();
    assert!(result.success);
    assert!(result.resource_id.is_some());
}

#[tokio::test]
async fn test_concurrent_mutations_generation_monotonic() {
    let repo = Arc::new(MemoryRepository::<JsonValue>::new());
    let cache = Arc::new(MemoryCache::new(1024));

    save(
        &repo,
        "123",
        HashMap::from([("name".to_string(), JsonValue::String("John".to_string()))]),
    )
    .await;

    let daf1 = make_daf(
        repo.clone() as Arc<dyn Repository<JsonValue>>,
        cache.clone() as Arc<dyn daf_core::Cache>,
        None,
    );
    let daf2 = make_daf(
        repo.clone() as Arc<dyn Repository<JsonValue>>,
        cache.clone() as Arc<dyn daf_core::Cache>,
        None,
    );

    let r1 = daf1.put(
        PutInfo {
            resource_id: ResourceId::new("123"),
            data: HashMap::from([("name".to_string(), JsonValue::String("Jane".to_string()))]),
        },
        None,
    );
    let r2 = daf2.put(
        PutInfo {
            resource_id: ResourceId::new("123"),
            data: HashMap::from([("name".to_string(), JsonValue::String("Jack".to_string()))]),
        },
        None,
    );

    let (_res1, _res2) = tokio::join!(r1, r2);

    let namespace = hex::encode(Sha256::digest("123"));
    let gen_key = format!("_daf_gen:{namespace}");
    let gen_val = cache.get(&gen_key).await.unwrap();
    let gen = gen_val
        .and_then(|v| {
            v.value
                .downcast_ref::<Generation>()
                .and_then(Generation::as_u64)
        })
        .unwrap_or(0);
    assert!(gen >= 1);
}

#[tokio::test]
async fn test_cache_hit_reauthorizes() {
    let repo = Arc::new(MemoryRepository::<JsonValue>::new());
    let cache = Arc::new(MemoryCache::new(1024));
    let auth: Arc<dyn Authorizer> = Arc::new(FakeAuthorizer {
        owned: HashMap::from([("123".to_string(), "user-1".to_string())]),
    });
    let daf = make_daf(
        repo.clone() as Arc<dyn Repository<JsonValue>>,
        cache.clone() as Arc<dyn daf_core::Cache>,
        Some(auth),
    );

    save(
        &repo,
        "123",
        HashMap::from([("name".to_string(), JsonValue::String("John".to_string()))]),
    )
    .await;

    let r1 = daf
        .query(
            QueryInfo {
                resource_id: ResourceId::new("123"),
                filters: None,
                algorithm: None,
            },
            Some(&UserId::new("user-1")),
        )
        .await
        .unwrap();
    assert!(r1.success);
    assert!(!r1.cache_hit);

    let r2 = daf
        .query(
            QueryInfo {
                resource_id: ResourceId::new("123"),
                filters: None,
                algorithm: None,
            },
            Some(&UserId::new("user-2")),
        )
        .await;
    assert!(r2.is_err());
}

#[tokio::test]
async fn test_unauthorized_user_cannot_query() {
    let repo = Arc::new(MemoryRepository::<JsonValue>::new());
    let cache = Arc::new(MemoryCache::new(1024));
    let auth: Arc<dyn Authorizer> = Arc::new(FakeAuthorizer {
        owned: HashMap::from([("123".to_string(), "owner".to_string())]),
    });
    let daf = make_daf(
        repo.clone() as Arc<dyn Repository<JsonValue>>,
        cache.clone() as Arc<dyn daf_core::Cache>,
        Some(auth),
    );

    save(
        &repo,
        "123",
        HashMap::from([("name".to_string(), JsonValue::String("John".to_string()))]),
    )
    .await;

    let err = daf
        .query(
            QueryInfo {
                resource_id: ResourceId::new("123"),
                filters: None,
                algorithm: None,
            },
            Some(&UserId::new("intruder")),
        )
        .await;
    assert!(err.is_err());
}

struct DenyAllAuthorizer;

#[async_trait]
impl Authorizer for DenyAllAuthorizer {
    async fn authorize(
        &self,
        _operation: &str,
        _resource_id: Option<&ResourceId>,
        _user: Option<&UserId>,
        _data: Option<Arc<dyn std::any::Any + Send + Sync>>,
    ) -> Result<(), AuthorizationError> {
        Err(AuthorizationError::new("Access denied"))
    }
}

#[tokio::test]
async fn test_unauthorized_user_cannot_post() {
    let repo = Arc::new(MemoryRepository::<JsonValue>::new());
    let cache = Arc::new(MemoryCache::new(1024));
    let auth: Arc<dyn Authorizer> = Arc::new(DenyAllAuthorizer);
    let daf = make_daf(
        repo.clone() as Arc<dyn Repository<JsonValue>>,
        cache.clone() as Arc<dyn daf_core::Cache>,
        Some(auth),
    );

    let err = daf
        .post(
            PostInfo {
                resource_type: "user".to_string(),
                data: HashMap::from([("name".to_string(), JsonValue::String("John".to_string()))]),
            },
            Some(&UserId::new("intruder")),
        )
        .await;
    assert!(err.is_err());
}

#[tokio::test]
async fn test_unauthorized_user_cannot_put() {
    let repo = Arc::new(MemoryRepository::<JsonValue>::new());
    let cache = Arc::new(MemoryCache::new(1024));
    let auth: Arc<dyn Authorizer> = Arc::new(FakeAuthorizer {
        owned: HashMap::from([("123".to_string(), "owner".to_string())]),
    });
    let daf = make_daf(
        repo.clone() as Arc<dyn Repository<JsonValue>>,
        cache.clone() as Arc<dyn daf_core::Cache>,
        Some(auth),
    );

    save(
        &repo,
        "123",
        HashMap::from([("name".to_string(), JsonValue::String("John".to_string()))]),
    )
    .await;

    let err = daf
        .put(
            PutInfo {
                resource_id: ResourceId::new("123"),
                data: HashMap::from([("name".to_string(), JsonValue::String("Jane".to_string()))]),
            },
            Some(&UserId::new("intruder")),
        )
        .await;
    assert!(err.is_err());
}

#[tokio::test]
async fn test_unauthorized_user_cannot_delete() {
    let repo = Arc::new(MemoryRepository::<JsonValue>::new());
    let cache = Arc::new(MemoryCache::new(1024));
    let auth: Arc<dyn Authorizer> = Arc::new(FakeAuthorizer {
        owned: HashMap::from([("123".to_string(), "owner".to_string())]),
    });
    let daf = make_daf(
        repo.clone() as Arc<dyn Repository<JsonValue>>,
        cache.clone() as Arc<dyn daf_core::Cache>,
        Some(auth),
    );

    save(
        &repo,
        "123",
        HashMap::from([("name".to_string(), JsonValue::String("John".to_string()))]),
    )
    .await;

    let err = daf
        .delete(
            DeleteInfo {
                resource_id: ResourceId::new("123"),
            },
            Some(&UserId::new("intruder")),
        )
        .await;
    assert!(err.is_err());
}

#[tokio::test]
async fn test_empty_resource_id_rejected_before_auth() {
    let repo = Arc::new(MemoryRepository::<JsonValue>::new());
    let cache = Arc::new(MemoryCache::new(1024));
    let auth: Arc<dyn Authorizer> = Arc::new(FakeAuthorizer {
        owned: HashMap::new(),
    });
    let daf = make_daf(
        repo.clone() as Arc<dyn Repository<JsonValue>>,
        cache.clone() as Arc<dyn daf_core::Cache>,
        Some(auth),
    );

    let err = daf
        .query(
            QueryInfo {
                resource_id: ResourceId("".to_string()),
                filters: None,
                algorithm: None,
            },
            Some(&UserId::new("user-1")),
        )
        .await;
    assert!(err.is_err());
}

#[tokio::test]
async fn test_no_authorizer_allows_all_operations() {
    let repo = Arc::new(MemoryRepository::<JsonValue>::new());
    let cache = Arc::new(MemoryCache::new(1024));
    let daf = make_daf(
        repo.clone() as Arc<dyn Repository<JsonValue>>,
        cache.clone() as Arc<dyn daf_core::Cache>,
        None,
    );

    save(
        &repo,
        "123",
        HashMap::from([("name".to_string(), JsonValue::String("John".to_string()))]),
    )
    .await;

    let r = daf
        .query(
            QueryInfo {
                resource_id: ResourceId::new("123"),
                filters: None,
                algorithm: None,
            },
            None,
        )
        .await;
    assert!(r.is_ok());

    let r = daf
        .put(
            PutInfo {
                resource_id: ResourceId::new("123"),
                data: HashMap::from([("name".to_string(), JsonValue::String("Jane".to_string()))]),
            },
            None,
        )
        .await;
    assert!(r.is_ok());

    let r = daf
        .delete(
            DeleteInfo {
                resource_id: ResourceId::new("123"),
            },
            None,
        )
        .await;
    assert!(r.is_ok());
}

#[tokio::test]
async fn test_cache_isolation_between_different_resources() {
    let repo = Arc::new(MemoryRepository::<JsonValue>::new());
    let cache = Arc::new(MemoryCache::new(1024));
    let daf = make_daf(
        repo.clone() as Arc<dyn Repository<JsonValue>>,
        cache.clone() as Arc<dyn daf_core::Cache>,
        None,
    );

    save(
        &repo,
        "res-a",
        HashMap::from([("value".to_string(), JsonValue::String("A".to_string()))]),
    )
    .await;
    save(
        &repo,
        "res-b",
        HashMap::from([("value".to_string(), JsonValue::String("B".to_string()))]),
    )
    .await;

    let r_a = daf
        .query(
            QueryInfo {
                resource_id: ResourceId::new("res-a"),
                filters: None,
                algorithm: None,
            },
            None,
        )
        .await
        .unwrap();
    assert!(r_a.success);
    assert_eq!(
        r_a.data,
        Some(JsonValue::Object(
            [("value".to_string(), JsonValue::String("A".to_string()))]
                .into_iter()
                .collect()
        ))
    );

    let r_b = daf
        .query(
            QueryInfo {
                resource_id: ResourceId::new("res-b"),
                filters: None,
                algorithm: None,
            },
            None,
        )
        .await
        .unwrap();
    assert!(r_b.success);
    assert_eq!(
        r_b.data,
        Some(JsonValue::Object(
            [("value".to_string(), JsonValue::String("B".to_string()))]
                .into_iter()
                .collect()
        ))
    );
}

#[tokio::test]
async fn test_authorization_prevents_mutation_side_effects() {
    let repo = Arc::new(MemoryRepository::<JsonValue>::new());
    let cache = Arc::new(MemoryCache::new(1024));
    let auth: Arc<dyn Authorizer> = Arc::new(FakeAuthorizer {
        owned: HashMap::from([("123".to_string(), "owner".to_string())]),
    });
    let daf = make_daf(
        repo.clone() as Arc<dyn Repository<JsonValue>>,
        cache.clone() as Arc<dyn daf_core::Cache>,
        Some(auth),
    );

    save(
        &repo,
        "123",
        HashMap::from([("name".to_string(), JsonValue::String("John".to_string()))]),
    )
    .await;

    let namespace = hex::encode(Sha256::digest("123"));
    let gen_key = format!("_daf_gen:{namespace}");
    let gen_before = cache.get(&gen_key).await.unwrap();
    let gen_val_before = gen_before
        .and_then(|v| {
            v.value
                .downcast_ref::<Generation>()
                .and_then(Generation::as_u64)
        })
        .unwrap_or(0);

    let err = daf
        .put(
            PutInfo {
                resource_id: ResourceId::new("123"),
                data: HashMap::from([("name".to_string(), JsonValue::String("Jane".to_string()))]),
            },
            Some(&UserId::new("intruder")),
        )
        .await;
    assert!(err.is_err());

    let gen_after = cache.get(&gen_key).await.unwrap();
    let gen_val_after = gen_after
        .and_then(|v| {
            v.value
                .downcast_ref::<Generation>()
                .and_then(Generation::as_u64)
        })
        .unwrap_or(0);
    assert_eq!(gen_val_before, gen_val_after);
}

#[tokio::test]
async fn test_post_creates_unique_resource_id() {
    let repo = Arc::new(MemoryRepository::<JsonValue>::new());
    let cache = Arc::new(MemoryCache::new(1024));
    let daf = make_daf(repo.clone(), cache, None);

    let r1 = daf
        .post(
            PostInfo {
                resource_type: "user".to_string(),
                data: HashMap::from([("name".to_string(), JsonValue::String("John".to_string()))]),
            },
            None,
        )
        .await
        .unwrap();
    let r2 = daf
        .post(
            PostInfo {
                resource_type: "user".to_string(),
                data: HashMap::from([("name".to_string(), JsonValue::String("Jane".to_string()))]),
            },
            None,
        )
        .await
        .unwrap();

    assert!(r1.resource_id.is_some());
    assert!(r2.resource_id.is_some());
    assert_ne!(r1.resource_id, r2.resource_id);
}

#[tokio::test]
async fn test_query_after_successful_post_returns_data() {
    let repo = Arc::new(MemoryRepository::<JsonValue>::new());
    let cache = Arc::new(MemoryCache::new(1024));
    let daf = make_daf(repo.clone(), cache, None);

    let post_result = daf
        .post(
            PostInfo {
                resource_type: "user".to_string(),
                data: HashMap::from([("name".to_string(), JsonValue::String("John".to_string()))]),
            },
            None,
        )
        .await
        .unwrap();
    let resource_id = post_result.resource_id.unwrap();

    let query_result = daf
        .query(
            QueryInfo {
                resource_id: resource_id.clone(),
                filters: None,
                algorithm: None,
            },
            None,
        )
        .await
        .unwrap();
    assert!(query_result.success);
    assert_eq!(
        query_result.data,
        Some(JsonValue::Object(
            [("name".to_string(), JsonValue::String("John".to_string()))]
                .into_iter()
                .collect()
        ))
    );
}

#[tokio::test]
async fn test_put_returns_conflict_on_concurrent_update() {
    let repo = Arc::new(MemoryRepository::<JsonValue>::new());
    let cache = Arc::new(MemoryCache::new(1024));
    let daf = make_daf(repo.clone(), cache, None);

    save(
        &repo,
        "123",
        HashMap::from([("name".to_string(), JsonValue::String("John".to_string()))]),
    )
    .await;

    let r1 = daf
        .put(
            PutInfo {
                resource_id: ResourceId::new("123"),
                data: HashMap::from([("name".to_string(), JsonValue::String("Jane".to_string()))]),
            },
            None,
        )
        .await
        .unwrap();
    assert!(r1.success);

    let r2 = daf
        .put(
            PutInfo {
                resource_id: ResourceId::new("123"),
                data: HashMap::from([("name".to_string(), JsonValue::String("Jack".to_string()))]),
            },
            None,
        )
        .await
        .unwrap();
    assert!(r2.success);
    assert_eq!(
        r2.data,
        Some(JsonValue::Object(
            [("name".to_string(), JsonValue::String("Jack".to_string()))]
                .into_iter()
                .collect()
        ))
    );
}

#[tokio::test]
async fn test_delete_returns_conflict_on_concurrent_update() {
    let repo = Arc::new(MemoryRepository::<JsonValue>::new());
    let cache = Arc::new(MemoryCache::new(1024));
    let daf = make_daf(repo.clone(), cache, None);

    save(
        &repo,
        "123",
        HashMap::from([("name".to_string(), JsonValue::String("John".to_string()))]),
    )
    .await;

    let r1 = daf
        .delete(
            DeleteInfo {
                resource_id: ResourceId::new("123"),
            },
            None,
        )
        .await
        .unwrap();
    assert!(r1.success);

    let r2 = daf
        .query(
            QueryInfo {
                resource_id: ResourceId::new("123"),
                filters: None,
                algorithm: None,
            },
            None,
        )
        .await;
    assert!(r2.is_err());
}

#[tokio::test]
async fn test_generation_advances_on_post() {
    let repo = Arc::new(MemoryRepository::<JsonValue>::new());
    let cache = Arc::new(MemoryCache::new(1024));
    let daf = make_daf(repo.clone(), cache.clone(), None);

    let post_result = daf
        .post(
            PostInfo {
                resource_type: "user".to_string(),
                data: HashMap::from([("name".to_string(), JsonValue::String("John".to_string()))]),
            },
            None,
        )
        .await
        .unwrap();
    let resource_id = post_result.resource_id.unwrap();

    let namespace = hex::encode(Sha256::digest(&resource_id.0));
    let gen_key = format!("_daf_gen:{namespace}");
    let gen_val = cache.get(&gen_key).await.unwrap();
    let gen = gen_val
        .and_then(|v| {
            v.value
                .downcast_ref::<daf_core::Generation>()
                .copied()
                .and_then(|g| g.as_u64())
        })
        .unwrap_or(0);
    assert_eq!(gen, 1);
}

#[tokio::test]
async fn test_generation_advances_on_put() {
    let repo = Arc::new(MemoryRepository::<JsonValue>::new());
    let cache = Arc::new(MemoryCache::new(1024));
    let daf = make_daf(repo.clone(), cache.clone(), None);

    save(
        &repo,
        "123",
        HashMap::from([("name".to_string(), JsonValue::String("John".to_string()))]),
    )
    .await;

    let namespace = hex::encode(Sha256::digest("123"));
    let gen_key = format!("_daf_gen:{namespace}");

    daf.query(
        QueryInfo {
            resource_id: ResourceId::new("123"),
            filters: None,
            algorithm: None,
        },
        None,
    )
    .await
    .unwrap();

    let gen_before = cache.get(&gen_key).await.unwrap();
    let gen_val_before = gen_before
        .and_then(|v| {
            v.value
                .downcast_ref::<daf_core::Generation>()
                .copied()
                .and_then(|g| g.as_u64())
        })
        .unwrap_or(0);

    daf.put(
        PutInfo {
            resource_id: ResourceId::new("123"),
            data: HashMap::from([("name".to_string(), JsonValue::String("Jane".to_string()))]),
        },
        None,
    )
    .await
    .unwrap();

    let gen_after = cache.get(&gen_key).await.unwrap();
    let gen_val_after = gen_after
        .and_then(|v| {
            v.value
                .downcast_ref::<daf_core::Generation>()
                .copied()
                .and_then(|g| g.as_u64())
        })
        .unwrap_or(0);
    assert_eq!(gen_val_after, gen_val_before + 1);
}

#[tokio::test]
async fn test_generation_advances_on_delete() {
    let repo = Arc::new(MemoryRepository::<JsonValue>::new());
    let cache = Arc::new(MemoryCache::new(1024));
    let daf = make_daf(repo.clone(), cache.clone(), None);

    save(
        &repo,
        "123",
        HashMap::from([("name".to_string(), JsonValue::String("John".to_string()))]),
    )
    .await;

    let namespace = hex::encode(Sha256::digest("123"));
    let gen_key = format!("_daf_gen:{namespace}");

    daf.query(
        QueryInfo {
            resource_id: ResourceId::new("123"),
            filters: None,
            algorithm: None,
        },
        None,
    )
    .await
    .unwrap();

    let gen_before = cache.get(&gen_key).await.unwrap();
    let gen_val_before = gen_before
        .and_then(|v| {
            v.value
                .downcast_ref::<Generation>()
                .and_then(Generation::as_u64)
        })
        .unwrap_or(0);

    daf.delete(
        DeleteInfo {
            resource_id: ResourceId::new("123"),
        },
        None,
    )
    .await
    .unwrap();

    let gen_after = cache.get(&gen_key).await.unwrap();
    let gen_val_after = gen_after
        .and_then(|v| {
            v.value
                .downcast_ref::<Generation>()
                .and_then(Generation::as_u64)
        })
        .unwrap_or(0);
    assert_eq!(gen_val_after, gen_val_before + 1);
}

#[tokio::test]
async fn test_query_with_filters_returns_matching_data() {
    let repo = Arc::new(MemoryRepository::<JsonValue>::new());
    let cache = Arc::new(MemoryCache::new(1024));
    let daf = make_daf(repo.clone(), cache, None);

    save(
        &repo,
        "123",
        HashMap::from([
            ("name".to_string(), JsonValue::String("John".to_string())),
            (
                "age".to_string(),
                JsonValue::Number(serde_json::Number::from(30)),
            ),
        ]),
    )
    .await;

    let r = daf
        .query(
            QueryInfo {
                resource_id: ResourceId::new("123"),
                filters: Some(HashMap::from([(
                    "name".to_string(),
                    JsonValue::String("John".to_string()),
                )])),
                algorithm: None,
            },
            None,
        )
        .await
        .unwrap();
    assert!(r.success);
    assert_eq!(
        r.data,
        Some(JsonValue::Object(
            [
                ("name".to_string(), JsonValue::String("John".to_string())),
                (
                    "age".to_string(),
                    JsonValue::Number(serde_json::Number::from(30))
                ),
            ]
            .into_iter()
            .collect()
        ))
    );

    let r_filtered = daf
        .query(
            QueryInfo {
                resource_id: ResourceId::new("123"),
                filters: Some(HashMap::from([(
                    "name".to_string(),
                    JsonValue::String("Jane".to_string()),
                )])),
                algorithm: None,
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(r_filtered.data, Some(JsonValue::Null));
}

#[tokio::test]
async fn test_data_access_factory_creates_data_access() {
    let repo: Arc<dyn daf_core::Repository<JsonValue>> = Arc::new(MemoryRepository::new());
    let cache: Arc<dyn daf_core::Cache> = Arc::new(MemoryCache::new(1024));
    let factory = DataAccessFactory::new(repo.clone(), cache.clone(), None, None);
    let daf = factory.create();
    let (repo_out, cache_out, algs_out) = daf.get_components();
    assert!(Arc::ptr_eq(&repo_out, &repo));
    assert!(Arc::ptr_eq(&cache_out, &cache));
    assert!(algs_out.is_empty());
}

#[tokio::test]
async fn test_post_then_query_returns_fresh_data() {
    let repo = Arc::new(MemoryRepository::<JsonValue>::new());
    let cache = Arc::new(MemoryCache::new(1024));
    let factory = DataAccessFactory::new(repo.clone(), cache.clone(), None, None);
    let daf = factory.create();

    let post_result = daf
        .post(
            PostInfo {
                resource_type: "user".to_string(),
                data: HashMap::from([("name".to_string(), JsonValue::String("Alice".to_string()))]),
            },
            None,
        )
        .await
        .unwrap();
    let resource_id = post_result.resource_id.unwrap();

    let r1 = daf
        .query(
            QueryInfo {
                resource_id: resource_id.clone(),
                filters: None,
                algorithm: None,
            },
            None,
        )
        .await
        .unwrap();
    assert!(r1.success);
    assert!(!r1.cache_hit);

    let r2 = daf
        .query(
            QueryInfo {
                resource_id: resource_id.clone(),
                filters: None,
                algorithm: None,
            },
            None,
        )
        .await
        .unwrap();
    assert!(r2.cache_hit);

    daf.put(
        PutInfo {
            resource_id: resource_id.clone(),
            data: HashMap::from([("name".to_string(), JsonValue::String("Bob".to_string()))]),
        },
        None,
    )
    .await
    .unwrap();

    let r3 = daf
        .query(
            QueryInfo {
                resource_id,
                filters: None,
                algorithm: None,
            },
            None,
        )
        .await
        .unwrap();
    assert!(!r3.cache_hit);
    assert_eq!(
        r3.data,
        Some(JsonValue::Object(
            [("name".to_string(), JsonValue::String("Bob".to_string()))]
                .into_iter()
                .collect()
        ))
    );
}

#[tokio::test]
async fn test_concurrent_queries_share_cache_hit() {
    let repo = Arc::new(MemoryRepository::<JsonValue>::new());
    let cache = Arc::new(MemoryCache::new(1024));
    let daf1 = DataAccess::new(repo.clone(), cache.clone(), None, None);
    let daf2 = DataAccess::new(repo.clone(), cache.clone(), None, None);

    save(
        &repo,
        "123",
        HashMap::from([("name".to_string(), JsonValue::String("John".to_string()))]),
    )
    .await;

    let r1 = daf1
        .query(
            QueryInfo {
                resource_id: ResourceId::new("123"),
                filters: None,
                algorithm: None,
            },
            None,
        )
        .await
        .unwrap();
    assert!(!r1.cache_hit);

    let r2 = daf2
        .query(
            QueryInfo {
                resource_id: ResourceId::new("123"),
                filters: None,
                algorithm: None,
            },
            None,
        )
        .await
        .unwrap();
    assert!(r2.cache_hit);
}

#[tokio::test]
async fn test_generation_missing_initializes_to_zero_on_miss() {
    let repo = Arc::new(MemoryRepository::<JsonValue>::new());
    let cache = Arc::new(MemoryCache::new(1024));
    let factory = DataAccessFactory::new(repo.clone(), cache.clone(), None, None);
    let daf = factory.create();

    save(
        &repo,
        "123",
        HashMap::from([("name".to_string(), JsonValue::String("John".to_string()))]),
    )
    .await;

    let r = daf
        .query(
            QueryInfo {
                resource_id: ResourceId::new("123"),
                filters: None,
                algorithm: None,
            },
            None,
        )
        .await
        .unwrap();
    assert!(r.success);
    assert!(!r.cache_hit);

    let namespace = hex::encode(Sha256::digest("123"));
    let gen_key = format!("_daf_gen:{namespace}");
    let gen_val = cache.get(&gen_key).await.unwrap();
    let gen = gen_val
        .and_then(|v| {
            v.value
                .downcast_ref::<Generation>()
                .and_then(Generation::as_u64)
        })
        .unwrap_or(0);
    assert_eq!(gen, 0);
}

#[tokio::test]
async fn test_cache_entry_tier_l1_on_set() {
    let cache = Arc::new(MemoryCache::new(1024));
    cache.set("k".to_string(), Arc::new("v")).await.unwrap();
    let entry = cache.get("k").await.unwrap();
    assert_eq!(entry.unwrap().origin_tier, Tier::L1);
}

#[tokio::test]
async fn test_cache_entry_tier_from_hierarchical() {
    let repo = Arc::new(MemoryRepository::<JsonValue>::new());
    let l1 = Arc::new(MemoryCache::new(1024)) as Arc<dyn daf_core::Cache>;
    let l2 = Arc::new(MemoryCache::new(1024)) as Arc<dyn daf_core::Cache>;
    let l3 = Arc::new(MemoryCache::new(1024)) as Arc<dyn daf_core::Cache>;
    let l4 = Arc::new(MemoryCache::new(1024)) as Arc<dyn daf_core::Cache>;
    let hierarchical = Arc::new(daf_cache::HierarchicalCache::new(
        None, l1, l2, l3, l4, None,
    ));

    let daf = DataAccessFactory::new(repo.clone(), hierarchical, None, None).create();

    save(
        &repo,
        "123",
        HashMap::from([("name".to_string(), JsonValue::String("John".to_string()))]),
    )
    .await;

    let r = daf
        .query(
            QueryInfo {
                resource_id: ResourceId::new("123"),
                filters: None,
                algorithm: None,
            },
            None,
        )
        .await
        .unwrap();
    assert!(r.success);
}

#[tokio::test]
async fn hierarchical_delete_prefix_is_best_effort_across_tiers() {
    use daf_cache::{HierarchicalCache, MokaCache};

    let l1 = Arc::new(MemoryCache::new(1024)) as Arc<dyn daf_core::Cache>;
    let l2 = Arc::new(MokaCache::new(1024)) as Arc<dyn daf_core::Cache>;
    let l3 = Arc::new(MemoryCache::new(1024)) as Arc<dyn daf_core::Cache>;
    let l4 = Arc::new(MemoryCache::new(1024)) as Arc<dyn daf_core::Cache>;
    let cache = Arc::new(HierarchicalCache::new(None, l1, l2, l3, l4, None));

    // Moka L2 returns Err on non-empty prefix; best-effort (INV-001) must not abort
    // and must still return Ok (invalidation is advisory; generation check safety).
    let result = cache.delete_prefix("ns:").await;
    assert!(result.is_ok());
}

/// Repository wrapper that commits, signals commit, then blocks on a barrier —
/// used to force the deterministic "committed repository + pre-advance" window.
struct CoordinatingRepo {
    inner: Arc<dyn Repository<JsonValue>>,
    committed: Arc<tokio::sync::Notify>,
    update_barrier: Arc<tokio::sync::Barrier>,
}

#[async_trait]
impl Repository<JsonValue> for CoordinatingRepo {
    async fn get(&self, key: &ResourceId) -> Result<Option<Arc<JsonValue>>, RepositoryError> {
        self.inner.get(key).await
    }
    async fn save(&self, key: &ResourceId, value: JsonValue) -> Result<(), RepositoryError> {
        self.inner.save(key, value).await
    }
    async fn delete(&self, key: &ResourceId) -> Result<(), RepositoryError> {
        self.inner.delete(key).await
    }
    async fn create(&self, value: JsonValue) -> Result<ResourceId, RepositoryError> {
        self.inner.create(value).await
    }
    async fn try_update(
        &self,
        key: &ResourceId,
        expected: &JsonValue,
        update: Box<dyn FnOnce(JsonValue) -> JsonValue + Send + 'static>,
    ) -> Result<Option<JsonValue>, RepositoryError> {
        // Commit FIRST, then signal, then block: the repository holds V1 while this
        // call is in flight (and, with the COH-001 fix, while `put` holds the gen lock).
        let res = self.inner.try_update(key, expected, update).await?;
        self.committed.notify_one();
        self.update_barrier.wait().await;
        Ok(res)
    }
    async fn try_delete(
        &self,
        key: &ResourceId,
        expected: &JsonValue,
    ) -> Result<bool, RepositoryError> {
        self.inner.try_delete(key, expected).await
    }
}

#[tokio::test]
async fn a11_generation_value_read_skew_no_stale_after_commit() {
    let repo = Arc::new(MemoryRepository::<JsonValue>::new());
    save(
        &repo,
        "123",
        HashMap::from([("name".to_string(), JsonValue::String("John".to_string()))]),
    )
    .await;

    // Prime the cache with a query result under generation Missing ("John").
    let cache = Arc::new(MemoryCache::new(1024));
    let daf = make_daf(
        repo.clone() as Arc<dyn Repository<JsonValue>>,
        cache.clone(),
        None,
    );
    daf.query(
        QueryInfo {
            resource_id: ResourceId::new("123"),
            filters: None,
            algorithm: None,
        },
        None,
    )
    .await
    .unwrap();

    let committed = Arc::new(tokio::sync::Notify::new());
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let coord = Arc::new(CoordinatingRepo {
        inner: repo.clone() as Arc<dyn Repository<JsonValue>>,
        committed: committed.clone(),
        update_barrier: barrier.clone(),
    }) as Arc<dyn Repository<JsonValue>>;
    let daf2 = make_daf(coord, cache.clone(), None);

    // Writer: commits V1 ("Jane"), signals, then blocks in try_update while holding
    // the per-resource generation lock (with the COH-001 fix).
    let writer = tokio::spawn(async move {
        daf2.put(
            PutInfo {
                resource_id: ResourceId::new("123"),
                data: HashMap::from([("name".to_string(), JsonValue::String("Jane".to_string()))]),
            },
            None,
        )
        .await
    });

    // Wait until the repository commit has landed (gen lock held by the writer).
    committed.notified().await;

    // Reader: must block on the generation lock until the writer advances it.
    let reader_daf = make_daf(
        repo.clone() as Arc<dyn Repository<JsonValue>>,
        cache.clone(),
        None,
    );
    let reader = tokio::spawn(async move {
        reader_daf
            .query(
                QueryInfo {
                    resource_id: ResourceId::new("123"),
                    filters: None,
                    algorithm: None,
                },
                None,
            )
            .await
    });

    // Release the writer: try_update returns, generation advances, caches
    // invalidate, gen lock releases, reader proceeds and must NOT see "John".
    barrier.wait().await;

    let put_result = writer.await.unwrap();
    assert!(put_result.is_ok(), "put should succeed");
    let q = reader.await.unwrap().unwrap();
    assert_eq!(
        q.data.expect("reader must return data"),
        JsonValue::Object(
            [("name".to_string(), JsonValue::String("Jane".to_string()))]
                .into_iter()
                .collect(),
        ),
        "COH-001: reader must not return stale pre-mutation value after commit"
    );
}

#[tokio::test]
async fn generation_enum_comparison_in_query() {
    let repo = Arc::new(MemoryRepository::<JsonValue>::new());
    let cache = Arc::new(MemoryCache::new(1024));
    let daf = make_daf(repo.clone(), cache.clone(), None);

    save(
        &repo,
        "123",
        HashMap::from([("name".to_string(), JsonValue::String("John".to_string()))]),
    )
    .await;

    let r1 = daf
        .query(
            QueryInfo {
                resource_id: ResourceId::new("123"),
                filters: None,
                algorithm: None,
            },
            None,
        )
        .await
        .unwrap();
    assert!(r1.success);
    assert!(!r1.cache_hit);

    daf.put(
        PutInfo {
            resource_id: ResourceId::new("123"),
            data: HashMap::from([("name".to_string(), JsonValue::String("Jane".to_string()))]),
        },
        None,
    )
    .await
    .unwrap();

    let namespace = hex::encode(Sha256::digest("123"));
    let mut payload = serde_json::Map::new();
    payload.insert("algorithm".to_string(), serde_json::Value::Null);
    payload.insert("filters".to_string(), serde_json::Value::Null);
    payload.insert("resource_id".to_string(), serde_json::json!("123"));
    payload.insert("user_id".to_string(), serde_json::json!("anonymous"));
    let canonical = serde_json::to_string(&payload).unwrap();
    let digest = hex::encode(Sha256::digest(canonical.as_bytes()));
    let cache_key = format!("query:{namespace}:{digest}");

    let stale_value = serde_json::json!({
        "raw": JsonValue::Object([("name".to_string(), JsonValue::String("Stale".to_string()))].into_iter().collect()),
        "transformed": JsonValue::Object([("name".to_string(), JsonValue::String("Stale".to_string()))].into_iter().collect()),
        "generation": 1,
    });
    cache.set(cache_key, Arc::new(stale_value)).await.unwrap();

    let r2 = daf
        .query(
            QueryInfo {
                resource_id: ResourceId::new("123"),
                filters: None,
                algorithm: None,
            },
            None,
        )
        .await
        .unwrap();
    assert!(r2.success);
    assert!(!r2.cache_hit);
    assert_eq!(
        r2.data,
        Some(JsonValue::Object(
            [("name".to_string(), JsonValue::String("Jane".to_string()))]
                .into_iter()
                .collect()
        ))
    );
}

#[tokio::test]
async fn put_with_moka_l2_advances_generation_despite_cache_degradation() {
    use daf_cache::{HierarchicalCache, MokaCache};

    let repo = Arc::new(MemoryRepository::<JsonValue>::new());
    let l1 = Arc::new(MemoryCache::new(1024)) as Arc<dyn daf_core::Cache>;
    let l2 = Arc::new(MokaCache::new(1024)) as Arc<dyn daf_core::Cache>;
    let l3 = Arc::new(MemoryCache::new(1024)) as Arc<dyn daf_core::Cache>;
    let l4 = Arc::new(MemoryCache::new(1024)) as Arc<dyn daf_core::Cache>;
    let cache = Arc::new(HierarchicalCache::new(None, l1, l2, l3, l4, None));
    let daf = make_daf(
        repo.clone() as Arc<dyn Repository<JsonValue>>,
        cache.clone(),
        None,
    );

    save(
        &repo,
        "123",
        HashMap::from([("name".to_string(), JsonValue::String("John".to_string()))]),
    )
    .await;

    daf.query(
        QueryInfo {
            resource_id: ResourceId::new("123"),
            filters: None,
            algorithm: None,
        },
        None,
    )
    .await
    .unwrap();

    let result = daf
        .put(
            PutInfo {
                resource_id: ResourceId::new("123"),
                data: HashMap::from([("name".to_string(), JsonValue::String("Jane".to_string()))]),
            },
            None,
        )
        .await;

    assert!(result.is_ok());
    let repo_data = repo.get(&ResourceId::new("123")).await.unwrap().unwrap();
    assert_eq!(
        *repo_data,
        JsonValue::Object(
            [("name".to_string(), JsonValue::String("Jane".to_string()))]
                .into_iter()
                .collect()
        )
    );
}

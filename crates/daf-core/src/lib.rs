#![allow(clippy::assertions_on_constants)]
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use thiserror::Error;

pub mod lock_registry;
pub use lock_registry::{LockGuard, LockRegistry};

pub type JsonValue = serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Tier {
    L0,
    L1,
    L2,
    L3,
    L4,
    L5,
}

#[derive(Debug, Clone)]
pub struct CacheEntry {
    pub value: Arc<dyn Any + Send + Sync>,
    pub origin_tier: Tier,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ResourceId(pub String);

impl ResourceId {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
}

impl fmt::Display for ResourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct UserId(pub String);

impl UserId {
    pub fn new(s: impl Into<String>) -> Self {
        let inner = s.into();
        debug_assert!(!inner.is_empty(), "UserId must not be empty");
        Self(inner)
    }
}

impl fmt::Display for UserId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Error)]
pub enum DataAccessError {
    #[error("resource not found: {0}")]
    NotFound(#[from] NotFoundError),
    #[error("{0}")]
    Validation(#[from] ValidationError),
    #[error("repository error: {0}")]
    Repository(#[from] RepositoryError),
    #[error("cache error: {0}")]
    Cache(#[from] CacheError),
    #[error("generation key missing or malformed")]
    GenerationKeyError,
    #[error("algorithm error: {0}")]
    Algorithm(#[from] AlgorithmError),
    #[error("authorization failed: {0}")]
    Authorization(#[from] AuthorizationError),
}

#[derive(Debug, Error, Clone)]
#[error("resource not found: {0}")]
pub struct NotFoundError(pub String);

impl NotFoundError {
    pub fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

#[derive(Debug, Error, Clone)]
#[error("validation failed: {message}")]
pub struct ValidationError {
    pub message: String,
}

impl ValidationError {
    pub fn new(msg: impl Into<String>) -> Self {
        Self {
            message: msg.into(),
        }
    }
}

#[derive(Debug, Error, Clone)]
#[error("repository error: {0}")]
pub struct RepositoryError(pub String);

impl RepositoryError {
    pub fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

#[derive(Debug, Error, Clone)]
#[error("cache error: {0}")]
pub struct CacheError(pub String);

impl CacheError {
    pub fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

#[derive(Debug, Error, Clone)]
#[error("algorithm error: {0}")]
pub struct AlgorithmError(pub String);

impl AlgorithmError {
    pub fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

#[derive(Debug, Error, Clone)]
#[error("authorization failed: {0}")]
pub struct AuthorizationError(pub String);

impl AuthorizationError {
    pub fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryInfo {
    pub resource_id: ResourceId,
    pub filters: Option<HashMap<String, JsonValue>>,
    pub algorithm: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostInfo {
    pub resource_type: String,
    pub data: HashMap<String, JsonValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PutInfo {
    pub resource_id: ResourceId,
    pub data: HashMap<String, JsonValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteInfo {
    pub resource_id: ResourceId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub success: bool,
    pub data: Option<JsonValue>,
    pub error: Option<String>,
    pub error_type: Option<String>,
    pub cache_hit: bool,
    pub algorithm_stats: Option<AlgorithmStats>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MutationResult {
    pub success: bool,
    pub resource_id: Option<ResourceId>,
    pub data: Option<JsonValue>,
    pub error: Option<String>,
    pub error_type: Option<String>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlgorithmStats {
    pub iterations: u64,
    pub cache_hits: u64,
    pub memo_size: usize,
}

impl AlgorithmStats {
    pub fn new(iterations: u64, cache_hits: u64, memo_size: usize) -> Self {
        Self {
            iterations,
            cache_hits,
            memo_size,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum Generation {
    #[default]
    Missing,
    Valid(u64),
}

impl Generation {
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Generation::Valid(n) => {
                debug_assert!(*n > 0, "Valid generation must be positive");
                Some(*n)
            }
            Generation::Missing => None,
        }
    }

    pub fn advance(self) -> Self {
        match self {
            Generation::Missing => Generation::Valid(1),
            Generation::Valid(n) => {
                debug_assert!(n < u64::MAX, "Valid(n) -> Valid(n+1) overflow guard");
                Generation::Valid(n + 1)
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("resource not found")]
    NotFound,
    #[error("authorization failed")]
    AuthorizationFailed,
    #[error("validation failed: {message}")]
    ValidationFailed { message: String },
    #[error("unknown algorithm: {name}")]
    UnknownAlgorithm { name: String },
    #[error("cache error: {0}")]
    CacheError(#[from] CacheError),
    #[error("repository error: {0}")]
    RepositoryError(#[from] RepositoryError),
}

#[async_trait]
pub trait Repository<T>: Send + Sync {
    async fn get(&self, key: &ResourceId) -> Result<Option<Arc<T>>, RepositoryError>;
    async fn save(&self, key: &ResourceId, value: T) -> Result<(), RepositoryError>;
    async fn delete(&self, key: &ResourceId) -> Result<(), RepositoryError>;
    async fn create(&self, value: T) -> Result<ResourceId, RepositoryError>;
    async fn try_update(
        &self,
        key: &ResourceId,
        expected: &T,
        update: Box<dyn FnOnce(T) -> T + Send + 'static>,
    ) -> Result<Option<T>, RepositoryError>;
    async fn try_delete(&self, key: &ResourceId, expected: &T) -> Result<bool, RepositoryError>;
}

#[async_trait]
pub trait Cache: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<CacheEntry>, CacheError>;
    async fn set(&self, key: String, value: Arc<dyn Any + Send + Sync>) -> Result<(), CacheError>;
    async fn delete(&self, key: &str) -> Result<(), CacheError>;
    async fn delete_prefix(&self, prefix: &str) -> Result<u64, CacheError>;
    async fn shake(&self, prefix: &str) -> Result<usize, CacheError>;
    async fn clear(&self) -> Result<(), CacheError>;

    fn tier(&self) -> Tier {
        Tier::L1
    }
}

#[async_trait]
pub trait Algorithm: Send + Sync {
    async fn execute(
        &self,
        input: Arc<dyn Any + Send + Sync>,
    ) -> Result<Arc<dyn Any + Send + Sync>, AlgorithmError>;
    async fn get_stats(&self) -> Result<AlgorithmStats, AlgorithmError>;
}

#[async_trait]
pub trait Authorizer: Send + Sync {
    async fn authorize(
        &self,
        operation: &str,
        resource_id: Option<&ResourceId>,
        user: Option<&UserId>,
        data: Option<Arc<dyn Any + Send + Sync>>,
    ) -> Result<(), AuthorizationError>;
}

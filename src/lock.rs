use dashmap::DashMap;
use std::sync::Arc;
use std::time::Duration;

pub struct LockGuard {
    #[allow(dead_code)]
    key: String,
    release: Option<Box<dyn FnOnce() + Send>>,
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            release();
        }
    }
}

#[allow(async_fn_in_trait)]
pub trait ConditionalLock: Send + Sync {
    async fn try_lock(&self, key: &str) -> Option<LockGuard>;
}

#[derive(Clone)]
pub struct InMemoryLock {
    locks: Arc<DashMap<String, ()>>,
}

impl InMemoryLock {
    pub fn new() -> Self {
        Self {
            locks: Arc::new(DashMap::new()),
        }
    }
}

impl Default for InMemoryLock {
    fn default() -> Self {
        Self::new()
    }
}

impl ConditionalLock for InMemoryLock {
    async fn try_lock(&self, key: &str) -> Option<LockGuard> {
        use dashmap::mapref::entry::Entry;
        match self.locks.entry(key.to_string()) {
            Entry::Occupied(_) => None,
            Entry::Vacant(v) => {
                v.insert(());
                let locks = self.locks.clone();
                let key = key.to_string();
                Some(LockGuard {
                    key: key.clone(),
                    release: Some(Box::new(move || {
                        locks.remove(&key);
                    })),
                })
            }
        }
    }
}

pub struct RedisLock {
    client: redis::Client,
    ttl: Duration,
    prefix: String,
}

impl RedisLock {
    pub fn new(redis_url: &str, ttl: Duration) -> Result<Self, redis::RedisError> {
        let client = redis::Client::open(redis_url)?;
        Ok(Self {
            client,
            ttl,
            prefix: "bunny-s3-lock:".to_string(),
        })
    }

    fn lock_key(&self, key: &str) -> String {
        format!("{}{}", self.prefix, key)
    }
}

impl ConditionalLock for RedisLock {
    async fn try_lock(&self, key: &str) -> Option<LockGuard> {
        let mut conn = self.client.get_multiplexed_async_connection().await.ok()?;
        let lock_key = self.lock_key(key);
        let lock_value = uuid::Uuid::new_v4().to_string();

        let result: Option<String> = redis::cmd("SET")
            .arg(&lock_key)
            .arg(&lock_value)
            .arg("NX")
            .arg("PX")
            .arg(self.ttl.as_millis() as u64)
            .query_async(&mut conn)
            .await
            .ok()?;

        if result.is_some() {
            let client = self.client.clone();
            let lock_key_owned = lock_key.clone();
            let lock_value_owned = lock_value.clone();

            Some(LockGuard {
                key: key.to_string(),
                release: Some(Box::new(move || {
                    tokio::spawn(async move {
                        if let Ok(mut conn) = client.get_multiplexed_async_connection().await {
                            let script = redis::Script::new(
                                r#"if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end"#,
                            );
                            let _: Result<i32, _> = script
                                .key(&lock_key_owned)
                                .arg(&lock_value_owned)
                                .invoke_async(&mut conn)
                                .await;
                        }
                    });
                })),
            })
        } else {
            None
        }
    }
}

pub enum Lock {
    InMemory(InMemoryLock),
    Redis(RedisLock),
}

impl ConditionalLock for Lock {
    async fn try_lock(&self, key: &str) -> Option<LockGuard> {
        match self {
            Lock::InMemory(lock) => lock.try_lock(key).await,
            Lock::Redis(lock) => lock.try_lock(key).await,
        }
    }
}

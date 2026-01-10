use axum::{
    body::Body,
    extract::State,
    http::{HeaderMap, Method, StatusCode, Uri, header},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use chrono::Utc;
use futures::StreamExt;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::oneshot;

use crate::bunny::{BunnyClient, UploadOptions};
use crate::config::Config;
use crate::error::{ProxyError, Result};
use crate::lock::{ConditionalLock, InMemoryLock, Lock};

use super::auth::{AwsAuth, EMPTY_PAYLOAD_HASH, UNSIGNED_PAYLOAD, calculate_payload_hash};
use super::multipart::MultipartManager;
use super::types::{
    CompleteMultipartUpload, CopySource, DeleteRequest, ListObjectsV2Query, S3Bucket,
    S3CommonPrefix, S3Object, S3Owner,
};
use super::xml;

struct HashingStream<S, H> {
    inner: S,
    hasher: H,
    hash_sender: Option<oneshot::Sender<String>>,
}

impl<S> HashingStream<S, Sha256> {
    fn new_sha256(inner: S) -> (Self, oneshot::Receiver<String>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                inner,
                hasher: Sha256::new(),
                hash_sender: Some(tx),
            },
            rx,
        )
    }
}

impl<S> HashingStream<S, md5::Md5> {
    fn new_md5(inner: S) -> (Self, oneshot::Receiver<String>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                inner,
                hasher: md5::Md5::new(),
                hash_sender: Some(tx),
            },
            rx,
        )
    }
}

impl<S: Unpin, H> Unpin for HashingStream<S, H> {}

impl<S, E, H> futures::Stream for HashingStream<S, H>
where
    S: futures::Stream<Item = std::result::Result<Bytes, E>> + Unpin,
    H: Digest + Clone,
{
    type Item = std::result::Result<Bytes, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                this.hasher.update(&chunk);
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => {
                if let Some(sender) = this.hash_sender.take() {
                    let hash = hex::encode(this.hasher.clone().finalize());
                    let _ = sender.send(hash);
                }
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(Clone)]
pub struct AppState {
    pub bunny: BunnyClient,
    pub auth: AwsAuth,
    pub config: Arc<Config>,
    pub lock: Arc<Lock>,
}

impl AppState {
    pub fn new(config: Config) -> Self {
        let lock = Self::create_lock(&config);
        Self {
            bunny: BunnyClient::new((&config).into()),
            auth: AwsAuth::new(
                config.s3_access_key_id.clone(),
                config.s3_secret_access_key.clone(),
            ),
            config: Arc::new(config),
            lock: Arc::new(lock),
        }
    }

    fn create_lock(config: &Config) -> Lock {
        if let Some(redis_url) = &config.redis_url {
            match crate::lock::RedisLock::new(
                redis_url,
                std::time::Duration::from_millis(config.redis_lock_ttl_ms),
            ) {
                Ok(redis_lock) => {
                    tracing::info!("Using Redis for conditional write locks");
                    return Lock::Redis(redis_lock);
                }
                Err(e) => {
                    tracing::warn!("Failed to connect to Redis: {}", e);
                }
            }
        }
        tracing::info!("Using in-memory conditional write locks");
        Lock::InMemory(InMemoryLock::new())
    }
}

pub async fn handle_s3_request(
    State(state): State<AppState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let path = uri.path();
    let (bucket, key) = parse_s3_path(path);

    let payload_hash = headers
        .get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let has_auth = headers.get("authorization").is_some();
    let content_length: Option<u64> = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse().ok());

    let query = uri.query().unwrap_or("");
    let is_multipart_part = query.contains("partNumber") && query.contains("uploadId");

    if method == Method::PUT && bucket.is_some() && key.is_some() {
        if has_auth {
            let hash_for_sig = payload_hash.as_deref().unwrap_or(UNSIGNED_PAYLOAD);
            if let Err(e) = state
                .auth
                .verify_request(&method, &uri, &headers, hash_for_sig)
            {
                return e.into_response();
            }
        }

        if is_multipart_part {
            return match handle_upload_part_stream(
                state,
                bucket.as_deref().unwrap(),
                query,
                body,
                content_length,
            )
            .await
            {
                Ok(r) => r,
                Err(e) => e.into_response(),
            };
        }

        let verify_hash = payload_hash.filter(|h| h != UNSIGNED_PAYLOAD);
        return match handle_put_object_stream(
            state,
            bucket.as_deref().unwrap(),
            key.as_deref().unwrap(),
            &headers,
            body,
            content_length,
            verify_hash,
        )
        .await
        {
            Ok(r) => r,
            Err(e) => e.into_response(),
        };
    }

    let body_bytes = match axum::body::to_bytes(body, 10 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            return ProxyError::InvalidRequest(format!("Failed to read body: {}", e))
                .into_response();
        }
    };

    let payload_hash = payload_hash.unwrap_or_else(|| {
        if body_bytes.is_empty() {
            EMPTY_PAYLOAD_HASH.to_string()
        } else {
            calculate_payload_hash(&body_bytes)
        }
    });

    if has_auth
        && let Err(e) = state
            .auth
            .verify_request(&method, &uri, &headers, &payload_hash)
    {
        return e.into_response();
    }

    match route_request(state, method, uri, headers, bucket, key, body_bytes).await {
        Ok(r) => r,
        Err(e) => e.into_response(),
    }
}

fn parse_s3_path(path: &str) -> (Option<String>, Option<String>) {
    let path = path.trim_start_matches('/');
    if path.is_empty() {
        return (None, None);
    }
    let parts: Vec<&str> = path.splitn(2, '/').collect();
    match parts.len() {
        1 => (Some(parts[0].to_string()), None),
        2 => (Some(parts[0].to_string()), Some(parts[1].to_string())),
        _ => (None, None),
    }
}

async fn route_request(
    state: AppState,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    bucket: Option<String>,
    key: Option<String>,
    body: Bytes,
) -> Result<Response> {
    let query = uri.query().unwrap_or("");

    match (&method, bucket.as_deref(), key.as_deref()) {
        (&Method::GET, None, None) => handle_list_buckets(state).await,
        (&Method::HEAD, Some(b), None) => handle_head_bucket(state, b).await,
        (&Method::GET, Some(b), None) if query.contains("uploads") => {
            handle_list_multipart_uploads(state, b, query).await
        }
        (&Method::GET, Some(b), None) => handle_list_objects_v2(state, b, &uri).await,
        (&Method::PUT, Some(b), None) => handle_create_bucket(b).await,
        (&Method::DELETE, Some(_), None) => {
            Err(ProxyError::InvalidRequest("Cannot delete bucket".into()))
        }

        (&Method::HEAD, Some(b), Some(k)) => handle_head_object(state, b, k).await,
        (&Method::GET, Some(b), Some(k)) if query.contains("uploadId") => {
            handle_list_parts(state, b, k, query).await
        }
        (&Method::GET, Some(b), Some(k)) => handle_get_object(state, b, k, &headers).await,
        (&Method::PUT, Some(b), Some(k)) if headers.contains_key("x-amz-copy-source") => {
            handle_copy_object(state, b, k, &headers).await
        }
        (&Method::PUT, Some(b), Some(k)) => handle_put_object(state, b, k, &headers, body).await,
        (&Method::DELETE, Some(_), Some(_)) if query.contains("uploadId") => {
            handle_abort_multipart_upload(state, query).await
        }
        (&Method::DELETE, Some(b), Some(k)) => handle_delete_object(state, b, k).await,
        (&Method::POST, Some(b), None) if query.contains("delete") => {
            handle_delete_objects(state, b, body).await
        }
        (&Method::POST, Some(b), Some(k)) if query.contains("uploads") => {
            handle_initiate_multipart_upload(state, b, k).await
        }
        (&Method::POST, Some(b), Some(k)) if query.contains("uploadId") => {
            handle_complete_multipart_upload(state, b, k, query, body).await
        }

        _ => Err(ProxyError::InvalidRequest(format!(
            "Unsupported: {} {}",
            method,
            uri.path()
        ))),
    }
}

async fn handle_list_buckets(state: AppState) -> Result<Response> {
    let buckets = vec![S3Bucket {
        name: state.config.storage_zone.clone(),
        creation_date: Utc::now(),
    }];
    let owner = S3Owner {
        id: state.auth.access_key_id().to_string(),
        display_name: state.auth.access_key_id().to_string(),
    };
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/xml")],
        xml::list_buckets_response(&buckets, &owner),
    )
        .into_response())
}

async fn handle_head_bucket(state: AppState, bucket: &str) -> Result<Response> {
    if bucket != state.config.storage_zone {
        return Err(ProxyError::BucketNotFound(bucket.to_string()));
    }
    state.bunny.list("").await?;
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/xml")],
        "",
    )
        .into_response())
}

async fn handle_create_bucket(_bucket: &str) -> Result<Response> {
    Ok((StatusCode::OK, "").into_response())
}

async fn handle_list_objects_v2(state: AppState, bucket: &str, uri: &Uri) -> Result<Response> {
    if bucket != state.config.storage_zone {
        return Err(ProxyError::BucketNotFound(bucket.to_string()));
    }

    let query: ListObjectsV2Query = uri
        .query()
        .map(|q| serde_urlencoded::from_str(q).unwrap_or_default())
        .unwrap_or_default();
    let prefix = query.prefix.as_deref().unwrap_or("");
    let delimiter = query.delimiter.as_deref();
    let max_keys = query.max_keys.unwrap_or(1000).min(1000);

    let objects = if delimiter.is_some() {
        state.bunny.list(prefix).await?
    } else {
        state
            .bunny
            .list_recursive(prefix, Some(max_keys as usize + 1))
            .await?
    };

    let mut s3_objects = Vec::new();
    let mut common_prefixes_set = HashSet::new();

    for obj in &objects {
        let key = obj.s3_key();
        if !key.starts_with(prefix) {
            continue;
        }

        if let Some(delim) = delimiter {
            let suffix = &key[prefix.len()..];
            if let Some(pos) = suffix.find(delim) {
                common_prefixes_set.insert(format!("{}{}{}", prefix, &suffix[..pos], delim));
                continue;
            }
        }

        if obj.is_directory {
            if delimiter.is_some() {
                common_prefixes_set.insert(if key.ends_with('/') {
                    key.clone()
                } else {
                    format!("{}/", key)
                });
            }
            continue;
        }

        s3_objects.push(S3Object {
            key,
            last_modified: obj.last_changed,
            etag: obj.etag(),
            size: obj.length.max(0),
            storage_class: "STANDARD".to_string(),
            owner: None,
        });
    }

    if let Some(start_after) = &query.start_after {
        s3_objects.retain(|o| o.key.as_str() > start_after.as_str());
    }
    s3_objects.sort_by(|a, b| a.key.cmp(&b.key));

    let is_truncated = s3_objects.len() > max_keys as usize;
    let s3_objects: Vec<_> = s3_objects.into_iter().take(max_keys as usize).collect();
    let next_token = if is_truncated {
        s3_objects.last().map(|o| o.key.clone())
    } else {
        None
    };
    let common_prefixes: Vec<S3CommonPrefix> = common_prefixes_set
        .into_iter()
        .map(|p| S3CommonPrefix { prefix: p })
        .collect();

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/xml")],
        xml::list_objects_v2_response(xml::ListObjectsV2Params {
            bucket,
            prefix: Some(prefix),
            delimiter,
            max_keys,
            objects: &s3_objects,
            common_prefixes: &common_prefixes,
            is_truncated,
            next_continuation_token: next_token.as_deref(),
            key_count: s3_objects.len() as u32,
            continuation_token: query.continuation_token.as_deref(),
            start_after: query.start_after.as_deref(),
        }),
    )
        .into_response())
}

async fn handle_head_object(state: AppState, bucket: &str, key: &str) -> Result<Response> {
    if bucket != state.config.storage_zone {
        return Err(ProxyError::BucketNotFound(bucket.to_string()));
    }
    let obj = state.bunny.describe(key).await?;

    // Bunny returns Length: -1 for non-existent files, or isDirectory for folders
    if obj.length < 0 || obj.is_directory {
        return Err(ProxyError::NotFound(key.to_string()));
    }

    let mut r = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_LENGTH, obj.length)
        .header(header::CONTENT_TYPE, &obj.content_type)
        .header(
            header::LAST_MODIFIED,
            obj.last_changed
                .format("%a, %d %b %Y %H:%M:%S GMT")
                .to_string(),
        )
        .header(header::ETAG, format!("\"{}\"", obj.etag()));
    if let Some(checksum) = &obj.checksum {
        r = r.header("x-amz-checksum-sha256", checksum);
    }
    Ok(r.body(Body::empty()).unwrap())
}

async fn handle_get_object(
    state: AppState,
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
) -> Result<Response> {
    if bucket != state.config.storage_zone {
        return Err(ProxyError::BucketNotFound(bucket.to_string()));
    }
    let download = state.bunny.download(key).await?;
    let total_size = download.content_length();
    let content_type = download
        .content_type()
        .unwrap_or("application/octet-stream")
        .to_string();
    let etag = download.etag();
    let last_modified = download.last_modified();

    if let Some(if_none_match) = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        && let Some(server_etag) = &etag
    {
        let server_etag_normalized = server_etag.trim_matches('"');
        let matches = if_none_match == "*"
            || if_none_match.split(',').any(|e| {
                e.trim()
                    .trim_matches('"')
                    .trim_start_matches("W/")
                    .trim_matches('"')
                    == server_etag_normalized
            });
        if matches {
            let mut r = Response::builder()
                .status(StatusCode::NOT_MODIFIED)
                .header(header::ETAG, format!("\"{}\"", server_etag_normalized));
            if let Some(lm) = &last_modified {
                r = r.header(header::LAST_MODIFIED, lm);
            }
            return Ok(r.body(Body::empty()).unwrap());
        }
    }

    if let Some(range_header) = headers.get(header::RANGE).and_then(|v| v.to_str().ok())
        && let Some(size) = total_size
        && let Some((start, end)) = parse_range(range_header, size)
    {
        let data = download.bytes().await?;
        let end = end.min(data.len() as u64 - 1);
        let slice = data.slice(start as usize..=end as usize);

        let mut r = Response::builder()
            .status(StatusCode::PARTIAL_CONTENT)
            .header(header::CONTENT_LENGTH, slice.len())
            .header(header::CONTENT_TYPE, content_type)
            .header(
                header::CONTENT_RANGE,
                format!("bytes {}-{}/{}", start, end, size),
            )
            .header(header::ACCEPT_RANGES, "bytes");
        if let Some(etag) = etag {
            r = r.header(header::ETAG, format!("\"{}\"", etag.trim_matches('"')));
        }
        if let Some(lm) = last_modified {
            r = r.header(header::LAST_MODIFIED, lm);
        }
        return Ok(r.body(Body::from(slice)).unwrap());
    }

    let mut r = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::ACCEPT_RANGES, "bytes");
    if let Some(size) = total_size {
        r = r.header(header::CONTENT_LENGTH, size);
    }
    if let Some(etag) = etag {
        r = r.header(header::ETAG, format!("\"{}\"", etag.trim_matches('"')));
    }
    if let Some(lm) = last_modified {
        r = r.header(header::LAST_MODIFIED, lm);
    }

    Ok(r.body(Body::from_stream(download.bytes_stream())).unwrap())
}

fn parse_range(header: &str, total_size: u64) -> Option<(u64, u64)> {
    let header = header.strip_prefix("bytes=")?;
    let parts: Vec<&str> = header.split('-').collect();
    if parts.len() != 2 {
        return None;
    }

    match (parts[0].parse::<u64>(), parts[1].parse::<u64>()) {
        (Ok(start), Ok(end)) => Some((start, end.min(total_size - 1))),
        (Ok(start), Err(_)) => Some((start, total_size - 1)), // "bytes=100-" means from 100 to end
        (Err(_), Ok(suffix)) => Some((total_size.saturating_sub(suffix), total_size - 1)), // "bytes=-100" means last 100 bytes
        _ => None,
    }
}

async fn handle_put_object(
    state: AppState,
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
    body: Bytes,
) -> Result<Response> {
    if bucket != state.config.storage_zone {
        return Err(ProxyError::BucketNotFound(bucket.to_string()));
    }

    let is_conditional = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.trim() == "*");

    let _lock_guard = if is_conditional {
        match state.lock.try_lock(key).await {
            Some(guard) => {
                if state.bunny.describe(key).await.is_ok() {
                    return Ok(Response::builder()
                        .status(StatusCode::PRECONDITION_FAILED)
                        .body(Body::empty())
                        .unwrap());
                }
                Some(guard)
            }
            None => {
                return Ok(Response::builder()
                    .status(StatusCode::CONFLICT)
                    .body(Body::from("Concurrent write in progress"))
                    .unwrap());
            }
        }
    } else {
        None
    };

    let options = UploadOptions {
        content_type: headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string()),
        sha256_checksum: headers
            .get("x-amz-checksum-sha256")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string()),
    };
    state.bunny.upload(key, body.clone(), options).await?;

    use md5::Digest;
    let etag = format!("{:x}", md5::Md5::digest(&body));
    Ok((
        StatusCode::OK,
        [(header::ETAG, format!("\"{}\"", etag))],
        "",
    )
        .into_response())
}

async fn handle_put_object_stream(
    state: AppState,
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
    body: Body,
    content_length: Option<u64>,
    claimed_hash: Option<String>,
) -> Result<Response> {
    if bucket != state.config.storage_zone {
        return Err(ProxyError::BucketNotFound(bucket.to_string()));
    }

    let is_conditional = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.trim() == "*");

    let _lock_guard = if is_conditional {
        match state.lock.try_lock(key).await {
            Some(guard) => {
                if state.bunny.describe(key).await.is_ok() {
                    return Ok(Response::builder()
                        .status(StatusCode::PRECONDITION_FAILED)
                        .body(Body::empty())
                        .unwrap());
                }
                Some(guard)
            }
            None => {
                return Ok(Response::builder()
                    .status(StatusCode::CONFLICT)
                    .body(Body::from("Concurrent write in progress"))
                    .unwrap());
            }
        }
    } else {
        None
    };

    let stream = body.into_data_stream();
    let stream = stream.map(|r| r.map_err(std::io::Error::other));

    let computed_hash = if let Some(ref expected) = claimed_hash {
        let (hashing_stream, hash_rx) = HashingStream::new_sha256(stream);
        state
            .bunny
            .upload_stream(key, hashing_stream, content_length)
            .await?;

        let computed = hash_rx.await.map_err(|_| {
            ProxyError::InvalidRequest("Failed to compute content hash".to_string())
        })?;

        if computed != *expected {
            tracing::warn!(
                "Content hash mismatch for {}: expected {}, got {}",
                key,
                expected,
                computed
            );
            let _ = state.bunny.delete(key).await;
            return Err(ProxyError::InvalidRequest(
                "Content hash mismatch".to_string(),
            ));
        }
        Some(computed)
    } else {
        state
            .bunny
            .upload_stream(key, stream, content_length)
            .await?;
        None
    };

    let etag = computed_hash
        .or_else(|| content_length.map(|l| format!("{:x}", l)))
        .unwrap_or_else(|| "streaming".to_string());

    Ok((
        StatusCode::OK,
        [(header::ETAG, format!("\"{}\"", etag))],
        "",
    )
        .into_response())
}

async fn handle_delete_object(state: AppState, bucket: &str, key: &str) -> Result<Response> {
    if bucket != state.config.storage_zone {
        return Err(ProxyError::BucketNotFound(bucket.to_string()));
    }
    state.bunny.delete(key).await?;
    Ok((StatusCode::NO_CONTENT, "").into_response())
}

async fn handle_copy_object(
    state: AppState,
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
) -> Result<Response> {
    if bucket != state.config.storage_zone {
        return Err(ProxyError::BucketNotFound(bucket.to_string()));
    }

    let copy_source = headers
        .get("x-amz-copy-source")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| ProxyError::InvalidRequest("Missing x-amz-copy-source".into()))?;
    let source = CopySource::parse(copy_source)
        .ok_or_else(|| ProxyError::InvalidRequest("Invalid copy source".into()))?;
    if source.bucket != state.config.storage_zone {
        return Err(ProxyError::BucketNotFound(source.bucket));
    }

    state.bunny.copy(&source.key, key).await?;
    let obj = state.bunny.describe(key).await?;

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/xml")],
        xml::copy_object_response(&obj.etag(), obj.last_changed),
    )
        .into_response())
}

async fn handle_delete_objects(state: AppState, bucket: &str, body: Bytes) -> Result<Response> {
    if bucket != state.config.storage_zone {
        return Err(ProxyError::BucketNotFound(bucket.to_string()));
    }

    let req: DeleteRequest = quick_xml::de::from_str(
        std::str::from_utf8(&body).map_err(|e| ProxyError::InvalidRequest(e.to_string()))?,
    )
    .map_err(|e| ProxyError::InvalidRequest(e.to_string()))?;
    let quiet = req.quiet.unwrap_or(false);
    let mut deleted = Vec::new();
    let mut errors = Vec::new();

    for obj in req.object {
        match state.bunny.delete(&obj.key).await {
            Ok(_) => deleted.push((obj.key, obj.version_id)),
            Err(e) => errors.push((obj.key, "InternalError".to_string(), e.to_string())),
        }
    }

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/xml")],
        xml::delete_objects_response(&deleted, &errors, quiet),
    )
        .into_response())
}

async fn handle_initiate_multipart_upload(
    state: AppState,
    bucket: &str,
    key: &str,
) -> Result<Response> {
    if bucket != state.config.storage_zone {
        return Err(ProxyError::BucketNotFound(bucket.to_string()));
    }
    let upload_id = MultipartManager::create(&state.bunny, bucket, key).await?;
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/xml")],
        xml::initiate_multipart_upload_response(bucket, key, &upload_id),
    )
        .into_response())
}

async fn handle_upload_part_stream(
    state: AppState,
    bucket: &str,
    query: &str,
    body: Body,
    content_length: Option<u64>,
) -> Result<Response> {
    if bucket != state.config.storage_zone {
        return Err(ProxyError::BucketNotFound(bucket.to_string()));
    }

    let params: std::collections::HashMap<String, String> =
        serde_urlencoded::from_str(query).unwrap_or_default();
    let upload_id = params
        .get("uploadId")
        .ok_or_else(|| ProxyError::InvalidRequest("Missing uploadId".into()))?;
    let part_number: i32 = params
        .get("partNumber")
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| ProxyError::InvalidRequest("Invalid partNumber".into()))?;

    let path = format!("__multipart/{}/{:05}", upload_id, part_number);

    let stream = body.into_data_stream();
    let stream = stream.map(|r| r.map_err(std::io::Error::other));
    let (hashing_stream, hash_rx) = HashingStream::new_md5(stream);

    state
        .bunny
        .upload_stream(&path, hashing_stream, content_length)
        .await?;

    let etag = hash_rx
        .await
        .map_err(|_| ProxyError::InvalidRequest("Failed to compute ETag".to_string()))?;

    MultipartManager::store_part_etag(&state.bunny, upload_id, part_number, &etag).await?;

    Ok((
        StatusCode::OK,
        [(header::ETAG, format!("\"{}\"", etag))],
        "",
    )
        .into_response())
}

async fn handle_complete_multipart_upload(
    state: AppState,
    bucket: &str,
    key: &str,
    query: &str,
    body: Bytes,
) -> Result<Response> {
    use axum::body::Body;

    if bucket != state.config.storage_zone {
        return Err(ProxyError::BucketNotFound(bucket.to_string()));
    }

    let params: std::collections::HashMap<String, String> =
        serde_urlencoded::from_str(query).unwrap_or_default();
    let upload_id = params
        .get("uploadId")
        .ok_or_else(|| ProxyError::InvalidRequest("Missing uploadId".into()))?
        .clone();

    let req: CompleteMultipartUpload = quick_xml::de::from_str(
        std::str::from_utf8(&body).map_err(|e| ProxyError::InvalidRequest(e.to_string()))?,
    )
    .map_err(|e| ProxyError::InvalidRequest(e.to_string()))?;
    let parts: Vec<(i32, String)> = req
        .part
        .into_iter()
        .map(|p| (p.part_number, p.etag))
        .collect();

    let bucket = bucket.to_string();
    let key = key.to_string();
    let region_base_url = state.config.region.base_url().to_string();

    let (tx, rx) = tokio::sync::mpsc::channel::<std::result::Result<Bytes, std::io::Error>>(16);

    tokio::spawn(async move {
        let _ = tx
            .send(Ok(Bytes::from(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><!-- ",
            )))
            .await;

        let keepalive_tx = tx.clone();
        let keepalive_handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                if keepalive_tx.send(Ok(Bytes::from(" "))).await.is_err() {
                    break;
                }
            }
        });

        let result =
            MultipartManager::complete(&state.bunny, &bucket, &upload_id, &key, &parts).await;

        keepalive_handle.abort();

        match result {
            Ok(etag) => {
                let location = format!("{}/{}/{}", region_base_url, bucket, key);
                let response = format!(
                    r#" --><CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Location>{}</Location><Bucket>{}</Bucket><Key>{}</Key><ETag>"{}"</ETag></CompleteMultipartUploadResult>"#,
                    location, bucket, key, etag
                );
                let _ = tx.send(Ok(Bytes::from(response))).await;
            }
            Err(e) => {
                let error_xml = format!(
                    r#" --><Error><Code>InternalError</Code><Message>{}</Message></Error>"#,
                    e
                );
                let _ = tx.send(Ok(Bytes::from(error_xml))).await;
            }
        }
    });

    let body = Body::from_stream(tokio_stream::wrappers::ReceiverStream::new(rx));

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/xml")
        .body(body)
        .unwrap())
}

async fn handle_abort_multipart_upload(state: AppState, query: &str) -> Result<Response> {
    let params: std::collections::HashMap<String, String> =
        serde_urlencoded::from_str(query).unwrap_or_default();
    let upload_id = params
        .get("uploadId")
        .ok_or_else(|| ProxyError::InvalidRequest("Missing uploadId".into()))?;
    MultipartManager::abort(&state.bunny, upload_id).await?;
    Ok((StatusCode::NO_CONTENT, "").into_response())
}

async fn handle_list_parts(
    state: AppState,
    bucket: &str,
    key: &str,
    query: &str,
) -> Result<Response> {
    if bucket != state.config.storage_zone {
        return Err(ProxyError::BucketNotFound(bucket.to_string()));
    }

    let params: std::collections::HashMap<String, String> =
        serde_urlencoded::from_str(query).unwrap_or_default();
    let upload_id = params
        .get("uploadId")
        .ok_or_else(|| ProxyError::InvalidRequest("Missing uploadId".into()))?;
    let max_parts: u32 = params
        .get("max-parts")
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);

    let parts = MultipartManager::list_parts(&state.bunny, upload_id).await?;
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/xml")],
        xml::list_parts_response(bucket, key, upload_id, &parts, false, None, max_parts),
    )
        .into_response())
}

async fn handle_list_multipart_uploads(
    state: AppState,
    bucket: &str,
    query: &str,
) -> Result<Response> {
    if bucket != state.config.storage_zone {
        return Err(ProxyError::BucketNotFound(bucket.to_string()));
    }

    let params: std::collections::HashMap<String, String> =
        serde_urlencoded::from_str(query).unwrap_or_default();
    let prefix = params.get("prefix").map(|s| s.as_str());
    let delimiter = params.get("delimiter").map(|s| s.as_str());
    let max_uploads: u32 = params
        .get("max-uploads")
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);

    let uploads: Vec<_> = MultipartManager::list_uploads(&state.bunny, bucket)
        .await?
        .into_iter()
        .filter(|(key, _, _)| prefix.map(|p| key.starts_with(p)).unwrap_or(true))
        .take(max_uploads as usize)
        .collect();

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/xml")],
        xml::list_multipart_uploads_response(
            bucket,
            &uploads,
            prefix,
            delimiter,
            max_uploads,
            false,
        ),
    )
        .into_response())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;

    #[tokio::test]
    async fn test_hashing_stream_computes_correct_sha256() {
        let data = b"hello world";
        let expected_hash = hex::encode(Sha256::digest(data));

        let chunks: Vec<std::result::Result<Bytes, std::io::Error>> =
            vec![Ok(Bytes::from_static(data))];
        let input_stream = stream::iter(chunks);

        let (hashing_stream, hash_rx) = HashingStream::new_sha256(input_stream);

        let collected: Vec<_> = hashing_stream.collect().await;
        assert_eq!(collected.len(), 1);
        assert_eq!(collected[0].as_ref().unwrap().as_ref(), data);

        let computed_hash = hash_rx.await.unwrap();
        assert_eq!(computed_hash, expected_hash);
    }

    #[tokio::test]
    async fn test_hashing_stream_multiple_chunks() {
        let chunk1 = b"hello ";
        let chunk2 = b"world";
        let mut hasher = Sha256::new();
        hasher.update(chunk1);
        hasher.update(chunk2);
        let expected_hash = hex::encode(hasher.finalize());

        let chunks: Vec<std::result::Result<Bytes, std::io::Error>> = vec![
            Ok(Bytes::from_static(chunk1)),
            Ok(Bytes::from_static(chunk2)),
        ];
        let input_stream = stream::iter(chunks);

        let (hashing_stream, hash_rx) = HashingStream::new_sha256(input_stream);

        let collected: Vec<_> = hashing_stream.collect().await;
        assert_eq!(collected.len(), 2);

        let computed_hash = hash_rx.await.unwrap();
        assert_eq!(computed_hash, expected_hash);
    }

    #[tokio::test]
    async fn test_hashing_stream_empty() {
        let expected_hash = hex::encode(Sha256::digest(b""));

        let chunks: Vec<std::result::Result<Bytes, std::io::Error>> = vec![];
        let input_stream = stream::iter(chunks);

        let (hashing_stream, hash_rx) = HashingStream::new_sha256(input_stream);

        let collected: Vec<_> = hashing_stream.collect().await;
        assert_eq!(collected.len(), 0);

        let computed_hash = hash_rx.await.unwrap();
        assert_eq!(computed_hash, expected_hash);
    }
}

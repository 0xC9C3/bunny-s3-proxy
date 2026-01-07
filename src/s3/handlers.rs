use axum::{body::Body, extract::State, http::{header, HeaderMap, Method, StatusCode, Uri}, response::{IntoResponse, Response}};
use bytes::Bytes;
use chrono::Utc;
use std::collections::HashSet;
use std::sync::Arc;

use crate::bunny::{BunnyClient, UploadOptions};
use crate::config::Config;
use crate::error::{ProxyError, Result};
use crate::lock::{ConditionalLock, InMemoryLock, Lock};

use super::auth::{calculate_payload_hash, AwsAuth, EMPTY_PAYLOAD_HASH, UNSIGNED_PAYLOAD};
use super::multipart::MultipartManager;
use super::types::{CompleteMultipartUpload, CopySource, DeleteRequest, ListObjectsV2Query, S3Bucket, S3CommonPrefix, S3Object, S3Owner};
use super::xml;

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
            auth: AwsAuth::new(config.s3_access_key_id.clone(), config.s3_secret_access_key.clone()),
            config: Arc::new(config),
            lock: Arc::new(lock),
        }
    }

    fn create_lock(config: &Config) -> Lock {
        if let Some(redis_url) = &config.redis_url {
            match crate::lock::RedisLock::new(redis_url, std::time::Duration::from_millis(config.redis_lock_ttl_ms)) {
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

pub async fn handle_s3_request(State(state): State<AppState>, method: Method, uri: Uri, headers: HeaderMap, body: Body) -> Response {
    let path = uri.path();
    let (bucket, key) = parse_s3_path(path);

    let body_bytes = match axum::body::to_bytes(body, 100 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => return ProxyError::InvalidRequest(format!("Failed to read body: {}", e)).into_response(),
    };

    let payload_hash = headers.get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| if body_bytes.is_empty() { EMPTY_PAYLOAD_HASH.to_string() } else { calculate_payload_hash(&body_bytes) });

    let skip_auth = payload_hash == UNSIGNED_PAYLOAD || headers.get("authorization").is_none();
    if !skip_auth
        && let Err(e) = state.auth.verify_request(&method, &uri, &headers, &payload_hash) {
            return e.into_response();
        }

    match route_request(state, method, uri, headers, bucket, key, body_bytes).await {
        Ok(r) => r,
        Err(e) => e.into_response(),
    }
}

fn parse_s3_path(path: &str) -> (Option<String>, Option<String>) {
    let path = path.trim_start_matches('/');
    if path.is_empty() { return (None, None); }
    let parts: Vec<&str> = path.splitn(2, '/').collect();
    match parts.len() {
        1 => (Some(parts[0].to_string()), None),
        2 => (Some(parts[0].to_string()), Some(parts[1].to_string())),
        _ => (None, None),
    }
}

async fn route_request(state: AppState, method: Method, uri: Uri, headers: HeaderMap, bucket: Option<String>, key: Option<String>, body: Bytes) -> Result<Response> {
    let query = uri.query().unwrap_or("");

    match (&method, bucket.as_deref(), key.as_deref()) {
        (&Method::GET, None, None) => handle_list_buckets(state).await,
        (&Method::HEAD, Some(b), None) => handle_head_bucket(state, b).await,
        (&Method::GET, Some(b), None) if query.contains("uploads") => handle_list_multipart_uploads(state, b, query).await,
        (&Method::GET, Some(b), None) => handle_list_objects_v2(state, b, &uri).await,
        (&Method::PUT, Some(b), None) => handle_create_bucket(b).await,
        (&Method::DELETE, Some(_), None) => Err(ProxyError::InvalidRequest("Cannot delete bucket".into())),

        (&Method::HEAD, Some(b), Some(k)) => handle_head_object(state, b, k).await,
        (&Method::GET, Some(b), Some(k)) if query.contains("uploadId") => handle_list_parts(state, b, k, query).await,
        (&Method::GET, Some(b), Some(k)) => handle_get_object(state, b, k, &headers).await,
        (&Method::PUT, Some(b), Some(k)) if headers.contains_key("x-amz-copy-source") => handle_copy_object(state, b, k, &headers).await,
        (&Method::PUT, Some(b), Some(k)) if query.contains("partNumber") => handle_upload_part(state, b, query, body).await,
        (&Method::PUT, Some(b), Some(k)) => handle_put_object(state, b, k, &headers, body).await,
        (&Method::DELETE, Some(_), Some(_)) if query.contains("uploadId") => handle_abort_multipart_upload(state, query).await,
        (&Method::DELETE, Some(b), Some(k)) => handle_delete_object(state, b, k).await,
        (&Method::POST, Some(b), None) if query.contains("delete") => handle_delete_objects(state, b, body).await,
        (&Method::POST, Some(b), Some(k)) if query.contains("uploads") => handle_initiate_multipart_upload(state, b, k).await,
        (&Method::POST, Some(b), Some(k)) if query.contains("uploadId") => handle_complete_multipart_upload(state, b, k, query, body).await,

        _ => Err(ProxyError::InvalidRequest(format!("Unsupported: {} {}", method, uri.path()))),
    }
}

async fn handle_list_buckets(state: AppState) -> Result<Response> {
    let buckets = vec![S3Bucket { name: state.config.storage_zone.clone(), creation_date: Utc::now() }];
    let owner = S3Owner { id: state.auth.access_key_id().to_string(), display_name: state.auth.access_key_id().to_string() };
    Ok((StatusCode::OK, [(header::CONTENT_TYPE, "application/xml")], xml::list_buckets_response(&buckets, &owner)).into_response())
}

async fn handle_head_bucket(state: AppState, bucket: &str) -> Result<Response> {
    if bucket != state.config.storage_zone { return Err(ProxyError::BucketNotFound(bucket.to_string())); }
    state.bunny.list("").await?;
    Ok((StatusCode::OK, [(header::CONTENT_TYPE, "application/xml")], "").into_response())
}

async fn handle_create_bucket(_bucket: &str) -> Result<Response> {
    Ok((StatusCode::OK, "").into_response())
}

async fn handle_list_objects_v2(state: AppState, bucket: &str, uri: &Uri) -> Result<Response> {
    if bucket != state.config.storage_zone { return Err(ProxyError::BucketNotFound(bucket.to_string())); }

    let query: ListObjectsV2Query = uri.query().map(|q| serde_urlencoded::from_str(q).unwrap_or_default()).unwrap_or_default();
    let prefix = query.prefix.as_deref().unwrap_or("");
    let delimiter = query.delimiter.as_deref();
    let max_keys = query.max_keys.unwrap_or(1000).min(1000);

    let objects = if delimiter.is_some() { state.bunny.list(prefix).await? } else { state.bunny.list_recursive(prefix, Some(max_keys as usize + 1)).await? };

    let mut s3_objects = Vec::new();
    let mut common_prefixes_set = HashSet::new();

    for obj in &objects {
        let key = obj.s3_key();
        if !key.starts_with(prefix) { continue; }

        if let Some(delim) = delimiter {
            let suffix = &key[prefix.len()..];
            if let Some(pos) = suffix.find(delim) {
                common_prefixes_set.insert(format!("{}{}{}", prefix, &suffix[..pos], delim));
                continue;
            }
        }

        if obj.is_directory {
            if delimiter.is_some() {
                common_prefixes_set.insert(if key.ends_with('/') { key.clone() } else { format!("{}/", key) });
            }
            continue;
        }

        s3_objects.push(S3Object { key, last_modified: obj.last_changed, etag: obj.etag(), size: obj.length.max(0), storage_class: "STANDARD".to_string(), owner: None });
    }

    if let Some(start_after) = &query.start_after {
        s3_objects.retain(|o| o.key.as_str() > start_after.as_str());
    }
    s3_objects.sort_by(|a, b| a.key.cmp(&b.key));

    let is_truncated = s3_objects.len() > max_keys as usize;
    let s3_objects: Vec<_> = s3_objects.into_iter().take(max_keys as usize).collect();
    let next_token = if is_truncated { s3_objects.last().map(|o| o.key.clone()) } else { None };
    let common_prefixes: Vec<S3CommonPrefix> = common_prefixes_set.into_iter().map(|p| S3CommonPrefix { prefix: p }).collect();

    Ok((StatusCode::OK, [(header::CONTENT_TYPE, "application/xml")],
        xml::list_objects_v2_response(xml::ListObjectsV2Params {
            bucket, prefix: Some(prefix), delimiter, max_keys, objects: &s3_objects,
            common_prefixes: &common_prefixes, is_truncated, next_continuation_token: next_token.as_deref(),
            key_count: s3_objects.len() as u32, continuation_token: query.continuation_token.as_deref(),
            start_after: query.start_after.as_deref(),
        })).into_response())
}

async fn handle_head_object(state: AppState, bucket: &str, key: &str) -> Result<Response> {
    if bucket != state.config.storage_zone { return Err(ProxyError::BucketNotFound(bucket.to_string())); }
    let obj = state.bunny.describe(key).await?;

    // Bunny returns Length: -1 for non-existent files, or isDirectory for folders
    if obj.length < 0 || obj.is_directory { return Err(ProxyError::NotFound(key.to_string())); }

    let mut r = Response::builder().status(StatusCode::OK)
        .header(header::CONTENT_LENGTH, obj.length)
        .header(header::CONTENT_TYPE, &obj.content_type)
        .header(header::LAST_MODIFIED, obj.last_changed.format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .header(header::ETAG, format!("\"{}\"", obj.etag()));
    if let Some(checksum) = &obj.checksum { r = r.header("x-amz-checksum-sha256", checksum); }
    Ok(r.body(Body::empty()).unwrap())
}

async fn handle_get_object(state: AppState, bucket: &str, key: &str, headers: &HeaderMap) -> Result<Response> {
    if bucket != state.config.storage_zone { return Err(ProxyError::BucketNotFound(bucket.to_string())); }
    let download = state.bunny.download(key).await?;
    let total_size = download.content_length();
    let content_type = download.content_type().unwrap_or("application/octet-stream").to_string();
    let etag = download.etag();
    let last_modified = download.last_modified();

    if let Some(if_none_match) = headers.get(header::IF_NONE_MATCH).and_then(|v| v.to_str().ok())
        && let Some(server_etag) = &etag {
            let server_etag_normalized = server_etag.trim_matches('"');
            let matches = if_none_match == "*" || if_none_match.split(',')
                .any(|e| e.trim().trim_matches('"').trim_start_matches("W/").trim_matches('"') == server_etag_normalized);
            if matches {
                let mut r = Response::builder().status(StatusCode::NOT_MODIFIED)
                    .header(header::ETAG, format!("\"{}\"", server_etag_normalized));
                if let Some(lm) = &last_modified { r = r.header(header::LAST_MODIFIED, lm); }
                return Ok(r.body(Body::empty()).unwrap());
            }
        }

    if let Some(range_header) = headers.get(header::RANGE).and_then(|v| v.to_str().ok())
        && let Some(size) = total_size
        && let Some((start, end)) = parse_range(range_header, size) {
            let data = download.bytes().await?;
            let end = end.min(data.len() as u64 - 1);
            let slice = data.slice(start as usize..=end as usize);

            let mut r = Response::builder()
                .status(StatusCode::PARTIAL_CONTENT)
                .header(header::CONTENT_LENGTH, slice.len())
                .header(header::CONTENT_TYPE, content_type)
                .header(header::CONTENT_RANGE, format!("bytes {}-{}/{}", start, end, size))
                .header(header::ACCEPT_RANGES, "bytes");
            if let Some(etag) = etag { r = r.header(header::ETAG, format!("\"{}\"", etag.trim_matches('"'))); }
            if let Some(lm) = last_modified { r = r.header(header::LAST_MODIFIED, lm); }
            return Ok(r.body(Body::from(slice)).unwrap());
        }

    let mut r = Response::builder().status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::ACCEPT_RANGES, "bytes");
    if let Some(size) = total_size { r = r.header(header::CONTENT_LENGTH, size); }
    if let Some(etag) = etag { r = r.header(header::ETAG, format!("\"{}\"", etag.trim_matches('"'))); }
    if let Some(lm) = last_modified { r = r.header(header::LAST_MODIFIED, lm); }

    Ok(r.body(Body::from_stream(download.bytes_stream())).unwrap())
}

fn parse_range(header: &str, total_size: u64) -> Option<(u64, u64)> {
    let header = header.strip_prefix("bytes=")?;
    let parts: Vec<&str> = header.split('-').collect();
    if parts.len() != 2 { return None; }

    match (parts[0].parse::<u64>(), parts[1].parse::<u64>()) {
        (Ok(start), Ok(end)) => Some((start, end.min(total_size - 1))),
        (Ok(start), Err(_)) => Some((start, total_size - 1)), // "bytes=100-" means from 100 to end
        (Err(_), Ok(suffix)) => Some((total_size.saturating_sub(suffix), total_size - 1)), // "bytes=-100" means last 100 bytes
        _ => None,
    }
}

async fn handle_put_object(state: AppState, bucket: &str, key: &str, headers: &HeaderMap, body: Bytes) -> Result<Response> {
    if bucket != state.config.storage_zone { return Err(ProxyError::BucketNotFound(bucket.to_string())); }

    let is_conditional = headers.get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.trim() == "*");

    let _lock_guard = if is_conditional {
        match state.lock.try_lock(key).await {
            Some(guard) => {
                if state.bunny.describe(key).await.is_ok() {
                    return Ok(Response::builder().status(StatusCode::PRECONDITION_FAILED).body(Body::empty()).unwrap());
                }
                Some(guard)
            }
            None => return Ok(Response::builder().status(StatusCode::CONFLICT).body(Body::from("Concurrent write in progress")).unwrap()),
        }
    } else {
        None
    };

    let options = UploadOptions {
        content_type: headers.get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()).map(|s| s.to_string()),
        sha256_checksum: headers.get("x-amz-checksum-sha256").and_then(|v| v.to_str().ok()).map(|s| s.to_string()),
    };
    state.bunny.upload(key, body.clone(), options).await?;

    use md5::Digest;
    let etag = format!("{:x}", md5::Md5::digest(&body));
    Ok((StatusCode::OK, [(header::ETAG, format!("\"{}\"", etag))], "").into_response())
}

async fn handle_delete_object(state: AppState, bucket: &str, key: &str) -> Result<Response> {
    if bucket != state.config.storage_zone { return Err(ProxyError::BucketNotFound(bucket.to_string())); }
    state.bunny.delete(key).await?;
    Ok((StatusCode::NO_CONTENT, "").into_response())
}

async fn handle_copy_object(state: AppState, bucket: &str, key: &str, headers: &HeaderMap) -> Result<Response> {
    if bucket != state.config.storage_zone { return Err(ProxyError::BucketNotFound(bucket.to_string())); }

    let copy_source = headers.get("x-amz-copy-source").and_then(|v| v.to_str().ok()).ok_or_else(|| ProxyError::InvalidRequest("Missing x-amz-copy-source".into()))?;
    let source = CopySource::parse(copy_source).ok_or_else(|| ProxyError::InvalidRequest("Invalid copy source".into()))?;
    if source.bucket != state.config.storage_zone { return Err(ProxyError::BucketNotFound(source.bucket)); }

    state.bunny.copy(&source.key, key).await?;
    let obj = state.bunny.describe(key).await?;

    Ok((StatusCode::OK, [(header::CONTENT_TYPE, "application/xml")], xml::copy_object_response(&obj.etag(), obj.last_changed)).into_response())
}

async fn handle_delete_objects(state: AppState, bucket: &str, body: Bytes) -> Result<Response> {
    if bucket != state.config.storage_zone { return Err(ProxyError::BucketNotFound(bucket.to_string())); }

    let req: DeleteRequest = quick_xml::de::from_str(std::str::from_utf8(&body).map_err(|e| ProxyError::InvalidRequest(e.to_string()))?).map_err(|e| ProxyError::InvalidRequest(e.to_string()))?;
    let quiet = req.quiet.unwrap_or(false);
    let mut deleted = Vec::new();
    let mut errors = Vec::new();

    for obj in req.object {
        match state.bunny.delete(&obj.key).await {
            Ok(_) => deleted.push((obj.key, obj.version_id)),
            Err(e) => errors.push((obj.key, "InternalError".to_string(), e.to_string())),
        }
    }

    Ok((StatusCode::OK, [(header::CONTENT_TYPE, "application/xml")], xml::delete_objects_response(&deleted, &errors, quiet)).into_response())
}

async fn handle_initiate_multipart_upload(state: AppState, bucket: &str, key: &str) -> Result<Response> {
    if bucket != state.config.storage_zone { return Err(ProxyError::BucketNotFound(bucket.to_string())); }
    let upload_id = MultipartManager::create(&state.bunny, bucket, key).await?;
    Ok((StatusCode::OK, [(header::CONTENT_TYPE, "application/xml")], xml::initiate_multipart_upload_response(bucket, key, &upload_id)).into_response())
}

async fn handle_upload_part(state: AppState, bucket: &str, query: &str, body: Bytes) -> Result<Response> {
    if bucket != state.config.storage_zone { return Err(ProxyError::BucketNotFound(bucket.to_string())); }

    let params: std::collections::HashMap<String, String> = serde_urlencoded::from_str(query).unwrap_or_default();
    let upload_id = params.get("uploadId").ok_or_else(|| ProxyError::InvalidRequest("Missing uploadId".into()))?;
    let part_number: i32 = params.get("partNumber").and_then(|s| s.parse().ok()).ok_or_else(|| ProxyError::InvalidRequest("Invalid partNumber".into()))?;

    let etag = MultipartManager::upload_part(&state.bunny, upload_id, part_number, body).await?;
    Ok((StatusCode::OK, [(header::ETAG, format!("\"{}\"", etag))], "").into_response())
}

async fn handle_complete_multipart_upload(state: AppState, bucket: &str, key: &str, query: &str, body: Bytes) -> Result<Response> {
    if bucket != state.config.storage_zone { return Err(ProxyError::BucketNotFound(bucket.to_string())); }

    let params: std::collections::HashMap<String, String> = serde_urlencoded::from_str(query).unwrap_or_default();
    let upload_id = params.get("uploadId").ok_or_else(|| ProxyError::InvalidRequest("Missing uploadId".into()))?;

    let req: CompleteMultipartUpload = quick_xml::de::from_str(std::str::from_utf8(&body).map_err(|e| ProxyError::InvalidRequest(e.to_string()))?).map_err(|e| ProxyError::InvalidRequest(e.to_string()))?;
    let parts: Vec<(i32, String)> = req.part.into_iter().map(|p| (p.part_number, p.etag)).collect();

    let etag = MultipartManager::complete(&state.bunny, bucket, upload_id, key, &parts).await?;

    let location = format!("{}/{}/{}", state.config.region.base_url(), bucket, key);
    Ok((StatusCode::OK, [(header::CONTENT_TYPE, "application/xml")], xml::complete_multipart_upload_response(bucket, key, &location, &etag)).into_response())
}

async fn handle_abort_multipart_upload(state: AppState, query: &str) -> Result<Response> {
    let params: std::collections::HashMap<String, String> = serde_urlencoded::from_str(query).unwrap_or_default();
    let upload_id = params.get("uploadId").ok_or_else(|| ProxyError::InvalidRequest("Missing uploadId".into()))?;
    MultipartManager::abort(&state.bunny, upload_id).await?;
    Ok((StatusCode::NO_CONTENT, "").into_response())
}

async fn handle_list_parts(state: AppState, bucket: &str, key: &str, query: &str) -> Result<Response> {
    if bucket != state.config.storage_zone { return Err(ProxyError::BucketNotFound(bucket.to_string())); }

    let params: std::collections::HashMap<String, String> = serde_urlencoded::from_str(query).unwrap_or_default();
    let upload_id = params.get("uploadId").ok_or_else(|| ProxyError::InvalidRequest("Missing uploadId".into()))?;
    let max_parts: u32 = params.get("max-parts").and_then(|s| s.parse().ok()).unwrap_or(1000);

    let parts = MultipartManager::list_parts(&state.bunny, upload_id).await?;
    Ok((StatusCode::OK, [(header::CONTENT_TYPE, "application/xml")], xml::list_parts_response(bucket, key, upload_id, &parts, false, None, max_parts)).into_response())
}

async fn handle_list_multipart_uploads(state: AppState, bucket: &str, query: &str) -> Result<Response> {
    if bucket != state.config.storage_zone { return Err(ProxyError::BucketNotFound(bucket.to_string())); }

    let params: std::collections::HashMap<String, String> = serde_urlencoded::from_str(query).unwrap_or_default();
    let prefix = params.get("prefix").map(|s| s.as_str());
    let delimiter = params.get("delimiter").map(|s| s.as_str());
    let max_uploads: u32 = params.get("max-uploads").and_then(|s| s.parse().ok()).unwrap_or(1000);

    let uploads: Vec<_> = MultipartManager::list_uploads(&state.bunny, bucket).await?
        .into_iter()
        .filter(|(key, _, _)| prefix.map(|p| key.starts_with(p)).unwrap_or(true))
        .take(max_uploads as usize).collect();

    Ok((StatusCode::OK, [(header::CONTENT_TYPE, "application/xml")], xml::list_multipart_uploads_response(bucket, &uploads, prefix, delimiter, max_uploads, false)).into_response())
}

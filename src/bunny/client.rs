use bytes::Bytes;
use futures::Stream;
use reqwest::{Body, Client, Method, Response, StatusCode};
use std::sync::Arc;

use crate::config::StorageZoneConfig;
use crate::error::{ProxyError, Result};

use super::types::{StorageObject, UploadOptions};

#[derive(Clone)]
pub struct BunnyClient {
    client: Client,
    config: Arc<StorageZoneConfig>,
}

impl BunnyClient {
    pub fn new(config: StorageZoneConfig) -> Self {
        let client = Client::builder()
            .user_agent("bunny-s3-proxy/0.1.0")
            .connect_timeout(std::time::Duration::from_secs(30))
            .http2_initial_stream_window_size(16 * 1024)
            .http2_initial_connection_window_size(32 * 1024)
            .http2_adaptive_window(false)
            .build()
            .expect("Failed to create HTTP client");

        Self {
            client,
            config: Arc::new(config),
        }
    }

    fn build_url(&self, path: &str) -> String {
        let base = self.config.region.base_url();
        let zone = &self.config.name;
        let clean_path = path.trim_start_matches('/');

        if clean_path.is_empty() {
            format!("{}/{}/", base, zone)
        } else {
            format!("{}/{}/{}", base, zone, clean_path)
        }
    }

    pub async fn list(&self, path: &str) -> Result<Vec<StorageObject>> {
        let mut url = self.build_url(path);
        if !url.ends_with('/') {
            url.push('/');
        }

        let response = self
            .client
            .get(&url)
            .header("AccessKey", &self.config.access_key)
            .header("Accept", "application/json")
            .send()
            .await?;

        match response.status() {
            StatusCode::OK => Ok(response.json().await?),
            StatusCode::NOT_FOUND => Ok(Vec::new()),
            StatusCode::UNAUTHORIZED => Err(ProxyError::AccessDenied),
            status => Err(ProxyError::BunnyApi(format!("List failed: {}", status))),
        }
    }

    pub async fn list_recursive(
        &self,
        prefix: &str,
        max_keys: Option<usize>,
    ) -> Result<Vec<StorageObject>> {
        let mut all_objects = Vec::new();
        let mut dirs_to_process = vec![prefix.to_string()];

        while let Some(dir) = dirs_to_process.pop() {
            if let Some(max) = max_keys
                && all_objects.len() >= max
            {
                break;
            }

            let objects = self.list(&dir).await?;
            for obj in objects {
                if obj.is_directory {
                    dirs_to_process.push(obj.full_path());
                } else {
                    all_objects.push(obj);
                    if let Some(max) = max_keys
                        && all_objects.len() >= max
                    {
                        break;
                    }
                }
            }
        }

        Ok(all_objects)
    }

    pub async fn describe(&self, path: &str) -> Result<StorageObject> {
        let url = self.build_url(path);

        let response = self
            .client
            .request(Method::from_bytes(b"DESCRIBE").unwrap(), &url)
            .header("AccessKey", &self.config.access_key)
            .header("Accept", "application/json")
            .send()
            .await?;

        match response.status() {
            StatusCode::OK => Ok(response.json().await?),
            StatusCode::NOT_FOUND => Err(ProxyError::NotFound(path.to_string())),
            StatusCode::UNAUTHORIZED => Err(ProxyError::AccessDenied),
            status => Err(ProxyError::BunnyApi(format!("Describe failed: {}", status))),
        }
    }

    pub async fn download(&self, path: &str) -> Result<DownloadResponse> {
        let url = self.build_url(path);

        let response = self
            .client
            .get(&url)
            .header("AccessKey", &self.config.access_key)
            .send()
            .await?;

        match response.status() {
            StatusCode::OK => Ok(DownloadResponse::new(response)),
            StatusCode::NOT_FOUND => Err(ProxyError::NotFound(path.to_string())),
            StatusCode::UNAUTHORIZED => Err(ProxyError::AccessDenied),
            status => Err(ProxyError::BunnyApi(format!("Download failed: {}", status))),
        }
    }

    pub async fn upload(&self, path: &str, body: Bytes, options: UploadOptions) -> Result<()> {
        let url = self.build_url(path);

        let mut request = self
            .client
            .put(&url)
            .header("AccessKey", &self.config.access_key)
            .header("Content-Type", "application/octet-stream");

        if let Some(checksum) = options.sha256_checksum {
            request = request.header("Checksum", checksum);
        }
        if let Some(content_type) = options.content_type {
            request = request.header("Override-Content-Type", content_type);
        }

        let response = request.body(body).send().await?;

        match response.status() {
            StatusCode::OK | StatusCode::CREATED => Ok(()),
            StatusCode::BAD_REQUEST => Err(ProxyError::InvalidRequest(
                "Invalid path or checksum".into(),
            )),
            StatusCode::UNAUTHORIZED => Err(ProxyError::AccessDenied),
            status => Err(ProxyError::BunnyApi(format!("Upload failed: {}", status))),
        }
    }

    pub async fn upload_stream(
        &self,
        path: &str,
        stream: impl Stream<Item = std::result::Result<Bytes, std::io::Error>> + Send + 'static,
        content_length: Option<u64>,
    ) -> Result<()> {
        let url = self.build_url(path);
        let body = Body::wrap_stream(stream);

        let mut request = self
            .client
            .put(&url)
            .header("AccessKey", &self.config.access_key)
            .header("Content-Type", "application/octet-stream");

        if let Some(len) = content_length {
            request = request.header("Content-Length", len);
        }

        let response = request.body(body).send().await?;

        match response.status() {
            StatusCode::OK | StatusCode::CREATED => Ok(()),
            StatusCode::BAD_REQUEST => Err(ProxyError::InvalidRequest(
                "Invalid path or checksum".into(),
            )),
            StatusCode::UNAUTHORIZED => Err(ProxyError::AccessDenied),
            status => Err(ProxyError::BunnyApi(format!("Upload failed: {}", status))),
        }
    }

    pub async fn delete(&self, path: &str) -> Result<()> {
        let url = self.build_url(path);

        let response = self
            .client
            .delete(&url)
            .header("AccessKey", &self.config.access_key)
            .send()
            .await?;

        match response.status() {
            StatusCode::OK | StatusCode::NOT_FOUND | StatusCode::BAD_REQUEST => Ok(()),
            StatusCode::UNAUTHORIZED => Err(ProxyError::AccessDenied),
            status => Err(ProxyError::BunnyApi(format!("Delete failed: {}", status))),
        }
    }

    pub async fn copy(&self, source: &str, dest: &str) -> Result<()> {
        let download = self.download(source).await?;
        let bytes = download.bytes().await?;
        self.upload(dest, bytes, UploadOptions::default()).await
    }
}

pub struct DownloadResponse {
    response: Response,
}

impl DownloadResponse {
    fn new(response: Response) -> Self {
        Self { response }
    }

    pub fn content_length(&self) -> Option<u64> {
        self.response.content_length()
    }

    pub fn content_type(&self) -> Option<&str> {
        self.response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
    }

    pub fn etag(&self) -> Option<String> {
        self.response
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
    }

    pub fn last_modified(&self) -> Option<String> {
        self.response
            .headers()
            .get("last-modified")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
    }

    pub async fn bytes(self) -> Result<Bytes> {
        Ok(self.response.bytes().await?)
    }

    pub fn bytes_stream(
        self,
    ) -> impl futures::Stream<Item = std::result::Result<Bytes, reqwest::Error>> + Send {
        self.response.bytes_stream()
    }
}

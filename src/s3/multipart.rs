use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::bunny::client::BunnyClient;
use crate::error::{ProxyError, Result};

enum PartState {
    NeedVerify,
    Verifying(
        Pin<
            Box<
                dyn std::future::Future<
                        Output = crate::error::Result<crate::bunny::client::DownloadResponse>,
                    > + Send,
            >,
        >,
    ),
    Downloading(
        Pin<
            Box<
                dyn std::future::Future<
                        Output = crate::error::Result<crate::bunny::client::DownloadResponse>,
                    > + Send,
            >,
        >,
    ),
    Streaming(Pin<Box<dyn Stream<Item = std::result::Result<Bytes, reqwest::Error>> + Send>>),
}

struct PartConcatStream {
    client: BunnyClient,
    upload_id: String,
    parts: std::vec::IntoIter<(i32, String)>,
    current_part: Option<(i32, String)>,
    state: PartState,
    verified_etags: Vec<String>,
}

impl PartConcatStream {
    fn new(client: BunnyClient, upload_id: String, parts: Vec<(i32, String)>) -> Self {
        Self {
            client,
            upload_id,
            parts: parts.into_iter(),
            current_part: None,
            state: PartState::NeedVerify,
            verified_etags: Vec::new(),
        }
    }
}

impl Stream for PartConcatStream {
    type Item = std::result::Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                PartState::NeedVerify => match self.parts.next() {
                    Some((part_number, expected_etag)) => {
                        self.current_part = Some((part_number, expected_etag));
                        let path = MultipartManager::part_etag_path(&self.upload_id, part_number);
                        let client = self.client.clone();
                        self.state =
                            PartState::Verifying(Box::pin(
                                async move { client.download(&path).await },
                            ));
                        continue;
                    }
                    None => return Poll::Ready(None),
                },

                PartState::Verifying(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(download)) => {
                        let (part_number, expected_etag) = self.current_part.as_ref().unwrap();
                        let expected = expected_etag.trim_matches('"').to_string();
                        let part_number = *part_number;
                        let upload_id = self.upload_id.clone();
                        let client = self.client.clone();

                        self.state = PartState::Downloading(Box::pin(async move {
                            let data = download.bytes().await?;
                            let actual_etag = String::from_utf8(data.to_vec()).map_err(|_| {
                                ProxyError::InvalidPart(format!(
                                    "Invalid ETag for part {}",
                                    part_number
                                ))
                            })?;

                            if actual_etag != expected {
                                return Err(ProxyError::InvalidPart(format!(
                                    "Part {} ETag mismatch: expected {}, got {}",
                                    part_number, expected, actual_etag
                                )));
                            }

                            let path = MultipartManager::part_path(&upload_id, part_number);
                            client.download(&path).await
                        }));
                        continue;
                    }
                    Poll::Ready(Err(e)) => {
                        return Poll::Ready(Some(Err(std::io::Error::other(e.to_string()))));
                    }
                    Poll::Pending => return Poll::Pending,
                },

                PartState::Downloading(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(download)) => {
                        if let Some((_, expected_etag)) = self.current_part.take() {
                            self.verified_etags
                                .push(expected_etag.trim_matches('"').to_string());
                        }
                        self.state = PartState::Streaming(Box::pin(download.bytes_stream()));
                        continue;
                    }
                    Poll::Ready(Err(e)) => {
                        return Poll::Ready(Some(Err(std::io::Error::other(e.to_string()))));
                    }
                    Poll::Pending => return Poll::Pending,
                },

                PartState::Streaming(stream) => match stream.as_mut().poll_next(cx) {
                    Poll::Ready(Some(Ok(chunk))) => {
                        return Poll::Ready(Some(Ok(chunk)));
                    }
                    Poll::Ready(Some(Err(e))) => {
                        return Poll::Ready(Some(Err(std::io::Error::other(e.to_string()))));
                    }
                    Poll::Ready(None) => {
                        if let Some((part_num, _)) = &self.current_part {
                            tracing::debug!("PartConcatStream: finished part {}", part_num);
                        }
                        self.current_part = None;
                        self.state = PartState::NeedVerify;
                        continue;
                    }
                    Poll::Pending => return Poll::Pending,
                },
            }
        }
    }
}

const MULTIPART_PREFIX: &str = "__multipart";

pub struct MultipartManager;

impl MultipartManager {
    fn part_path(upload_id: &str, part_number: i32) -> String {
        format!("{}/{}/{:05}", MULTIPART_PREFIX, upload_id, part_number)
    }

    fn part_etag_path(upload_id: &str, part_number: i32) -> String {
        format!("{}/{}/{:05}.etag", MULTIPART_PREFIX, upload_id, part_number)
    }

    fn meta_path(upload_id: &str) -> String {
        format!("{}/{}/_meta", MULTIPART_PREFIX, upload_id)
    }

    fn upload_dir(upload_id: &str) -> String {
        format!("{}/{}", MULTIPART_PREFIX, upload_id)
    }

    pub async fn create(client: &BunnyClient, _bucket: &str, key: &str) -> Result<String> {
        let upload_id = uuid::Uuid::new_v4().to_string();
        let meta = format!("{}|{}", key, Utc::now().to_rfc3339());
        client
            .upload(
                &Self::meta_path(&upload_id),
                Bytes::from(meta),
                Default::default(),
            )
            .await?;
        Ok(upload_id)
    }

    pub async fn store_part_etag(
        client: &BunnyClient,
        upload_id: &str,
        part_number: i32,
        etag: &str,
    ) -> Result<()> {
        let path = Self::part_etag_path(upload_id, part_number);
        client
            .upload(&path, Bytes::from(etag.to_string()), Default::default())
            .await
    }

    async fn read_part_etag(
        client: &BunnyClient,
        upload_id: &str,
        part_number: i32,
    ) -> Result<String> {
        let path = Self::part_etag_path(upload_id, part_number);
        let download = client.download(&path).await?;
        let data = download.bytes().await?;
        String::from_utf8(data.to_vec())
            .map_err(|_| ProxyError::InvalidPart(format!("Invalid ETag for part {}", part_number)))
    }

    pub async fn complete(
        client: &BunnyClient,
        _bucket: &str,
        upload_id: &str,
        key: &str,
        parts: &[(i32, String)],
    ) -> Result<String> {
        let fresh_client = client.fresh();

        tracing::debug!("CompleteMultipartUpload: checking if upload exists");
        if !Self::exists(&fresh_client, upload_id).await? {
            return Err(ProxyError::MultipartNotFound(upload_id.to_string()));
        }

        let mut total_size: u64 = 0;
        let mut parts_with_etags = Vec::with_capacity(parts.len());

        tracing::debug!("CompleteMultipartUpload: describing {} parts", parts.len());
        for (part_number, expected_etag) in parts {
            let path = Self::part_path(upload_id, *part_number);
            let obj = fresh_client.describe(&path).await.map_err(|e| {
                tracing::error!("Failed to describe part {}: {:?}", part_number, e);
                ProxyError::InvalidPart(format!("Part {} not found", part_number))
            })?;

            total_size += obj.length.max(0) as u64;
            parts_with_etags.push((*part_number, expected_etag.clone()));
        }

        tracing::debug!(
            "CompleteMultipartUpload: total size {} bytes, starting upload",
            total_size
        );

        use md5::Digest;
        let combined_md5: Vec<u8> = parts
            .iter()
            .flat_map(|(_, etag)| hex::decode(etag.trim_matches('"')).unwrap_or_default())
            .collect();
        let final_etag = format!("{:x}-{}", md5::Md5::digest(&combined_md5), parts.len());

        let stream = PartConcatStream::new(
            fresh_client.clone(),
            upload_id.to_string(),
            parts_with_etags,
        );

        if let Err(e) = fresh_client
            .upload_stream(key, stream, Some(total_size))
            .await
        {
            tracing::error!("CompleteMultipartUpload: upload_stream failed: {:?}", e);
            return Err(e);
        }

        tracing::debug!("CompleteMultipartUpload: upload complete, cleaning up");

        Self::cleanup(&fresh_client, upload_id).await?;

        Ok(final_etag)
    }

    pub async fn abort(client: &BunnyClient, upload_id: &str) -> Result<()> {
        if !Self::exists(client, upload_id).await? {
            return Err(ProxyError::MultipartNotFound(upload_id.to_string()));
        }
        Self::cleanup(client, upload_id).await
    }

    pub async fn list_parts(
        client: &BunnyClient,
        upload_id: &str,
    ) -> Result<Vec<(i32, String, i64, DateTime<Utc>)>> {
        if !Self::exists(client, upload_id).await? {
            return Err(ProxyError::MultipartNotFound(upload_id.to_string()));
        }

        let dir = Self::upload_dir(upload_id);
        let objects = client.list(&dir).await?;

        let mut parts = Vec::new();
        for obj in objects {
            if obj.object_name == "_meta" || obj.object_name.ends_with(".etag") {
                continue;
            }
            if let Ok(part_number) = obj.object_name.parse::<i32>() {
                let etag = Self::read_part_etag(client, upload_id, part_number)
                    .await
                    .unwrap_or_else(|_| "unknown".to_string());
                parts.push((part_number, etag, obj.length.max(0), obj.last_changed));
            }
        }

        parts.sort_by_key(|(n, _, _, _)| *n);
        Ok(parts)
    }

    pub async fn list_uploads(
        client: &BunnyClient,
        _bucket: &str,
    ) -> Result<Vec<(String, String, DateTime<Utc>)>> {
        let objects = client.list(MULTIPART_PREFIX).await?;
        let mut uploads = Vec::new();

        for obj in objects {
            if !obj.is_directory {
                continue;
            }
            let upload_id = obj.object_name.clone();
            let meta_path = Self::meta_path(&upload_id);

            if let Ok(download) = client.download(&meta_path).await
                && let Ok(data) = download.bytes().await
                && let Ok(meta) = String::from_utf8(data.to_vec())
                && let Some((key, initiated)) = meta.split_once('|')
                && let Ok(dt) = DateTime::parse_from_rfc3339(initiated)
            {
                uploads.push((key.to_string(), upload_id, dt.with_timezone(&Utc)));
            }
        }

        Ok(uploads)
    }

    async fn exists(client: &BunnyClient, upload_id: &str) -> Result<bool> {
        let meta_path = Self::meta_path(upload_id);
        match client.describe(&meta_path).await {
            Ok(_) => Ok(true),
            Err(ProxyError::NotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn cleanup(client: &BunnyClient, upload_id: &str) -> Result<()> {
        let dir = Self::upload_dir(upload_id);
        let objects = client.list(&dir).await?;

        for obj in objects {
            let path = format!("{}/{}", dir, obj.object_name);
            let _ = client.delete(&path).await;
        }

        let _ = client.delete(&format!("{}/", dir)).await;
        Ok(())
    }
}

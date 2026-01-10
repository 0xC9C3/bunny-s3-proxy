use async_stream::try_stream;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::StreamExt;

use crate::bunny::client::BunnyClient;
use crate::error::{ProxyError, Result};

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
        if !Self::exists(client, upload_id).await? {
            return Err(ProxyError::MultipartNotFound(upload_id.to_string()));
        }

        let mut total_size: u64 = 0;
        let mut part_etags = Vec::new();

        for (part_number, expected_etag) in parts {
            let path = Self::part_path(upload_id, *part_number);
            let obj = client
                .describe(&path)
                .await
                .map_err(|_| ProxyError::InvalidPart(format!("Part {} not found", part_number)))?;

            let actual_etag = Self::read_part_etag(client, upload_id, *part_number).await?;
            let expected = expected_etag.trim_matches('"');

            if actual_etag != expected {
                return Err(ProxyError::InvalidPart(format!(
                    "Part {} ETag mismatch: expected {}, got {}",
                    part_number, expected, actual_etag
                )));
            }

            total_size += obj.length.max(0) as u64;
            part_etags.push(actual_etag);
        }

        let parts_for_stream: Vec<(i32, String)> = parts.to_vec();
        let client_clone = client.clone();
        let upload_id_clone = upload_id.to_string();

        let stream = try_stream! {
            for (part_number, _) in parts_for_stream {
                let path = Self::part_path(&upload_id_clone, part_number);
                let download = client_clone.download(&path).await.map_err(|e| std::io::Error::other(e.to_string()))?;
                let mut byte_stream = download.bytes_stream();
                while let Some(chunk) = byte_stream.next().await {
                    let data = chunk.map_err(|e| std::io::Error::other(e.to_string()))?;
                    yield data;
                }
            }
        };

        client.upload_stream(key, stream, Some(total_size)).await?;

        use md5::Digest;
        let combined_md5: Vec<u8> = part_etags
            .iter()
            .flat_map(|etag| hex::decode(etag).unwrap_or_default())
            .collect();
        let final_etag = format!("{:x}-{}", md5::Md5::digest(&combined_md5), parts.len());

        Self::cleanup(client, upload_id).await?;

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

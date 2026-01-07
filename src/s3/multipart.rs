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

    fn meta_path(upload_id: &str) -> String {
        format!("{}/{}/_meta", MULTIPART_PREFIX, upload_id)
    }

    fn upload_dir(upload_id: &str) -> String {
        format!("{}/{}", MULTIPART_PREFIX, upload_id)
    }

    pub async fn create(client: &BunnyClient, _bucket: &str, key: &str) -> Result<String> {
        let upload_id = uuid::Uuid::new_v4().to_string();
        let meta = format!("{}|{}", key, Utc::now().to_rfc3339());
        client.upload(&Self::meta_path(&upload_id), Bytes::from(meta), Default::default()).await?;
        Ok(upload_id)
    }

    pub async fn upload_part(client: &BunnyClient, upload_id: &str, part_number: i32, data: Bytes) -> Result<String> {
        if !Self::exists(client, upload_id).await? {
            return Err(ProxyError::MultipartNotFound(upload_id.to_string()));
        }

        use md5::Digest;
        let etag = format!("{:x}", md5::Md5::digest(&data));
        let path = Self::part_path(upload_id, part_number);
        client.upload(&path, data, Default::default()).await?;
        Ok(etag)
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

        // Verify all parts exist and ETags match
        let mut total_size: u64 = 0;
        let mut part_infos = Vec::new();

        for (part_number, expected_etag) in parts {
            let path = Self::part_path(upload_id, *part_number);
            let obj = client.describe(&path).await
                .map_err(|_| ProxyError::InvalidPart(format!("Part {} not found", part_number)))?;

            use md5::Digest;
            let download = client.download(&path).await?;
            let data = download.bytes().await?;
            let actual_etag = format!("{:x}", md5::Md5::digest(&data));

            let expected = expected_etag.trim_matches('"');
            if actual_etag != expected {
                return Err(ProxyError::InvalidPart(format!("Part {} ETag mismatch", part_number)));
            }

            total_size += obj.length.max(0) as u64;
            part_infos.push((*part_number, actual_etag, obj.length.max(0) as u64));
        }

        // Stream all parts concatenated to final destination
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

        // Calculate final ETag (MD5 of concatenated part MD5s + part count)
        use md5::Digest;
        let combined_md5: Vec<u8> = part_infos.iter()
            .flat_map(|(_, etag, _)| hex::decode(etag).unwrap_or_default())
            .collect();
        let final_etag = format!("{:x}-{}", md5::Md5::digest(&combined_md5), parts.len());

        // Cleanup temp parts
        Self::cleanup(client, upload_id).await?;

        Ok(final_etag)
    }

    pub async fn abort(client: &BunnyClient, upload_id: &str) -> Result<()> {
        if !Self::exists(client, upload_id).await? {
            return Err(ProxyError::MultipartNotFound(upload_id.to_string()));
        }
        Self::cleanup(client, upload_id).await
    }

    pub async fn list_parts(client: &BunnyClient, upload_id: &str) -> Result<Vec<(i32, String, i64, DateTime<Utc>)>> {
        if !Self::exists(client, upload_id).await? {
            return Err(ProxyError::MultipartNotFound(upload_id.to_string()));
        }

        let dir = Self::upload_dir(upload_id);
        let objects = client.list(&dir).await?;

        let mut parts = Vec::new();
        for obj in objects {
            if obj.object_name == "_meta" {
                continue;
            }
            if let Ok(part_number) = obj.object_name.parse::<i32>() {
                use md5::Digest;
                let path = Self::part_path(upload_id, part_number);
                let download = client.download(&path).await?;
                let data = download.bytes().await?;
                let etag = format!("{:x}", md5::Md5::digest(&data));
                parts.push((part_number, etag, obj.length.max(0), obj.last_changed));
            }
        }

        parts.sort_by_key(|(n, _, _, _)| *n);
        Ok(parts)
    }

    pub async fn list_uploads(client: &BunnyClient, _bucket: &str) -> Result<Vec<(String, String, DateTime<Utc>)>> {
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
                            && let Ok(dt) = DateTime::parse_from_rfc3339(initiated) {
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

        // Delete the directory itself
        let _ = client.delete(&format!("{}/", dir)).await;
        Ok(())
    }
}

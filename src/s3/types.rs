use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct S3Object {
    pub key: String,
    pub last_modified: DateTime<Utc>,
    pub etag: String,
    pub size: i64,
    pub storage_class: String,
    pub owner: Option<S3Owner>,
}

#[derive(Debug, Clone)]
pub struct S3Owner {
    pub id: String,
    pub display_name: String,
}

#[derive(Debug, Clone)]
pub struct S3Bucket {
    pub name: String,
    pub creation_date: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct S3CommonPrefix {
    pub prefix: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ListObjectsV2Query {
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    #[serde(rename = "max-keys")]
    pub max_keys: Option<u32>,
    #[serde(rename = "continuation-token")]
    pub continuation_token: Option<String>,
    #[serde(rename = "start-after")]
    pub start_after: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteRequest {
    pub quiet: Option<bool>,
    pub object: Vec<DeleteObject>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteObject {
    pub key: String,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Part {
    pub part_number: i32,
    #[serde(rename = "ETag")]
    pub etag: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct CompleteMultipartUpload {
    pub part: Vec<Part>,
}

#[derive(Debug, Clone)]
pub struct CopySource {
    pub bucket: String,
    pub key: String,
}

impl CopySource {
    pub fn parse(header: &str) -> Option<Self> {
        let path = header.trim_start_matches('/');
        let parts: Vec<&str> = path.splitn(2, '/').collect();
        if parts.len() < 2 {
            return None;
        }
        let key = parts[1]
            .split_once("?versionId=")
            .map(|(k, _)| k)
            .unwrap_or(parts[1]);
        Some(Self {
            bucket: parts[0].to_string(),
            key: key.to_string(),
        })
    }
}

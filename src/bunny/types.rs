use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct StorageObject {
    pub guid: String,
    pub user_id: String,
    #[serde(with = "bunny_datetime")]
    pub last_changed: DateTime<Utc>,
    #[serde(with = "bunny_datetime")]
    pub date_created: DateTime<Utc>,
    pub storage_zone_name: String,
    pub path: String,
    pub object_name: String,
    pub length: i64,
    pub storage_zone_id: i64,
    pub is_directory: bool,
    pub server_id: i64,
    pub checksum: Option<String>,
    pub replicated_zones: Option<String>,
    pub content_type: String,
}

impl StorageObject {
    pub fn full_path(&self) -> String {
        if self.path.ends_with('/') {
            format!("{}{}", self.path, self.object_name)
        } else {
            format!("{}/{}", self.path, self.object_name)
        }
    }

    pub fn s3_key(&self) -> String {
        let full = self.full_path();
        let trimmed = full.trim_start_matches('/');
        if let Some(rest) = trimmed.strip_prefix(&self.storage_zone_name) {
            rest.trim_start_matches('/').to_string()
        } else {
            trimmed.to_string()
        }
    }

    pub fn etag(&self) -> String {
        self.checksum
            .clone()
            .unwrap_or_else(|| md5_hash(&self.guid))
    }
}

fn md5_hash(s: &str) -> String {
    use md5::Digest;
    format!("{:x}", md5::Md5::digest(s.as_bytes()))
}

mod bunny_datetime {
    use chrono::{DateTime, NaiveDateTime, Utc};
    use serde::{self, Deserialize, Deserializer, Serializer};

    const FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.f";

    pub fn serialize<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&date.format(FORMAT).to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if let Ok(dt) = NaiveDateTime::parse_from_str(&s, FORMAT) {
            return Ok(dt.and_utc());
        }
        if let Ok(dt) = NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S") {
            return Ok(dt.and_utc());
        }
        if let Ok(dt) = DateTime::parse_from_rfc3339(&s) {
            return Ok(dt.with_timezone(&Utc));
        }
        Err(serde::de::Error::custom(format!("Invalid datetime: {}", s)))
    }
}

#[derive(Debug, Clone, Default)]
pub struct UploadOptions {
    pub sha256_checksum: Option<String>,
    pub content_type: Option<String>,
}

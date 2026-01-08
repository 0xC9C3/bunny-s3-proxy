use axum::http::{HeaderMap, Method, Uri};
use chrono::{NaiveDateTime, Utc};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

use crate::error::{ProxyError, Result};

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone)]
pub struct AwsAuth {
    access_key_id: String,
    secret_access_key: String,
}

impl AwsAuth {
    pub fn new(access_key_id: String, secret_access_key: String) -> Self {
        Self {
            access_key_id,
            secret_access_key,
        }
    }

    pub fn verify_request(
        &self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        body_hash: &str,
    ) -> Result<()> {
        if let Some(auth_header) = headers.get("authorization") {
            let auth_str = auth_header
                .to_str()
                .map_err(|_| ProxyError::InvalidSignature)?;
            return self.verify_signature_v4(method, uri, headers, body_hash, auth_str);
        }

        if uri
            .query()
            .map(|q| q.contains("X-Amz-Signature"))
            .unwrap_or(false)
        {
            return self.verify_presigned_url(uri);
        }

        Err(ProxyError::MissingAuth)
    }

    fn verify_signature_v4(
        &self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        body_hash: &str,
        auth_header: &str,
    ) -> Result<()> {
        if !auth_header.starts_with("AWS4-HMAC-SHA256") {
            return Err(ProxyError::InvalidSignature);
        }

        let parts: Vec<&str> = auth_header.split(", ").collect();
        if parts.len() < 3 {
            return Err(ProxyError::InvalidSignature);
        }

        let credential = parts[0]
            .trim_start_matches("AWS4-HMAC-SHA256 Credential=")
            .trim();
        let cred_parts: Vec<&str> = credential.split('/').collect();
        if cred_parts.len() < 5 {
            return Err(ProxyError::InvalidSignature);
        }

        let access_key = cred_parts[0];
        let date = cred_parts[1];
        let region = cred_parts[2];
        let service = cred_parts[3];

        if access_key != self.access_key_id {
            return Err(ProxyError::InvalidSignature);
        }

        let signed_headers = parts[1].trim_start_matches("SignedHeaders=").trim();
        let provided_signature = parts[2].trim_start_matches("Signature=").trim();

        let amz_date = headers
            .get("x-amz-date")
            .and_then(|v| v.to_str().ok())
            .ok_or(ProxyError::InvalidSignature)?;

        let canonical_request =
            self.build_canonical_request(method, uri, headers, signed_headers, body_hash)?;
        let string_to_sign =
            self.build_string_to_sign(amz_date, date, region, service, &canonical_request);
        let calculated_signature = self.calculate_signature(
            &self.secret_access_key,
            date,
            region,
            service,
            &string_to_sign,
        );

        if constant_time_compare(provided_signature, &calculated_signature) {
            Ok(())
        } else {
            Err(ProxyError::InvalidSignature)
        }
    }

    fn verify_presigned_url(&self, uri: &Uri) -> Result<()> {
        let query = uri.query().unwrap_or("");
        let params: BTreeMap<String, String> = url::form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .collect();

        let access_key = params
            .get("X-Amz-Credential")
            .and_then(|c| c.split('/').next())
            .ok_or(ProxyError::InvalidSignature)?;

        if access_key != self.access_key_id {
            return Err(ProxyError::InvalidSignature);
        }

        if let (Some(expires), Some(date_str)) =
            (params.get("X-Amz-Expires"), params.get("X-Amz-Date"))
        {
            let expires_secs: i64 = expires.parse().map_err(|_| ProxyError::InvalidSignature)?;
            if let Ok(date) = NaiveDateTime::parse_from_str(date_str, "%Y%m%dT%H%M%SZ") {
                let expiry = date.and_utc() + chrono::Duration::seconds(expires_secs);
                if Utc::now() > expiry {
                    return Err(ProxyError::InvalidSignature);
                }
            }
        }

        if params.contains_key("X-Amz-Signature") {
            Ok(())
        } else {
            Err(ProxyError::InvalidSignature)
        }
    }

    fn build_canonical_request(
        &self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        signed_headers: &str,
        body_hash: &str,
    ) -> Result<String> {
        let canonical_query = self.build_canonical_query_string(uri.query().unwrap_or(""));

        let signed_header_list: Vec<&str> = signed_headers.split(';').collect();
        let mut canonical_headers = String::new();
        for header_name in &signed_header_list {
            let header_value = headers
                .get(*header_name)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            canonical_headers.push_str(&format!("{}:{}\n", header_name, header_value.trim()));
        }

        Ok(format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            method.as_str(),
            uri.path(),
            canonical_query,
            canonical_headers,
            signed_headers,
            body_hash
        ))
    }

    fn build_canonical_query_string(&self, query: &str) -> String {
        if query.is_empty() {
            return String::new();
        }
        let mut params: Vec<(String, String)> = url::form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .collect();
        params.sort_by(|a, b| a.0.cmp(&b.0));
        params
            .iter()
            .map(|(k, v)| format!("{}={}", uri_encode(k, true), uri_encode(v, true)))
            .collect::<Vec<_>>()
            .join("&")
    }

    fn build_string_to_sign(
        &self,
        amz_date: &str,
        date: &str,
        region: &str,
        service: &str,
        canonical_request: &str,
    ) -> String {
        let credential_scope = format!("{}/{}/{}/aws4_request", date, region, service);
        let canonical_request_hash = hex::encode(Sha256::digest(canonical_request.as_bytes()));
        format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            amz_date, credential_scope, canonical_request_hash
        )
    }

    fn calculate_signature(
        &self,
        secret_key: &str,
        date: &str,
        region: &str,
        service: &str,
        string_to_sign: &str,
    ) -> String {
        let k_date = hmac_sha256(format!("AWS4{}", secret_key).as_bytes(), date.as_bytes());
        let k_region = hmac_sha256(&k_date, region.as_bytes());
        let k_service = hmac_sha256(&k_region, service.as_bytes());
        let k_signing = hmac_sha256(&k_service, b"aws4_request");
        hex::encode(hmac_sha256(&k_signing, string_to_sign.as_bytes()))
    }

    pub fn access_key_id(&self) -> &str {
        &self.access_key_id
    }
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn constant_time_compare(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.bytes()
        .zip(b.bytes())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

fn uri_encode(s: &str, encode_slash: bool) -> String {
    let mut result = String::new();
    for c in s.chars() {
        match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '_' | '-' | '~' | '.' => result.push(c),
            '/' if !encode_slash => result.push(c),
            _ => {
                for b in c.to_string().as_bytes() {
                    result.push_str(&format!("%{:02X}", b));
                }
            }
        }
    }
    result
}

pub fn calculate_payload_hash(body: &[u8]) -> String {
    hex::encode(Sha256::digest(body))
}

pub const EMPTY_PAYLOAD_HASH: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
pub const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ProxyError {
    #[error("Bunny API error: {0}")]
    BunnyApi(String),
    #[error("Object not found: {0}")]
    NotFound(String),
    #[error("Bucket not found: {0}")]
    BucketNotFound(String),
    #[error("Access denied")]
    AccessDenied,
    #[error("Invalid request: {0}")]
    InvalidRequest(String),
    #[error("Invalid signature")]
    InvalidSignature,
    #[error("Missing authentication")]
    MissingAuth,
    #[error("Multipart upload not found: {0}")]
    MultipartNotFound(String),
    #[error("Invalid part: {0}")]
    InvalidPart(String),
    #[error("HTTP client error: {0}")]
    HttpClient(#[from] reqwest::Error),
    #[error("XML error: {0}")]
    Xml(#[from] quick_xml::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

impl ProxyError {
    pub fn s3_error_code(&self) -> &'static str {
        match self {
            Self::NotFound(_) => "NoSuchKey",
            Self::BucketNotFound(_) => "NoSuchBucket",
            Self::AccessDenied | Self::InvalidSignature | Self::MissingAuth => "AccessDenied",
            Self::InvalidRequest(_) => "InvalidRequest",
            Self::MultipartNotFound(_) => "NoSuchUpload",
            Self::InvalidPart(_) => "InvalidPart",
            _ => "InternalError",
        }
    }

    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::NotFound(_) | Self::BucketNotFound(_) | Self::MultipartNotFound(_) => StatusCode::NOT_FOUND,
            Self::AccessDenied | Self::InvalidSignature | Self::MissingAuth => StatusCode::FORBIDDEN,
            Self::InvalidRequest(_) | Self::InvalidPart(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for ProxyError {
    fn into_response(self) -> Response {
        let body = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?><Error><Code>{}</Code><Message>{}</Message><RequestId>{}</RequestId></Error>"#,
            self.s3_error_code(),
            self.to_string().replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;"),
            uuid::Uuid::new_v4()
        );
        (self.status_code(), [("content-type", "application/xml"), ("x-amz-request-id", &uuid::Uuid::new_v4().to_string())], body).into_response()
    }
}

pub type Result<T> = std::result::Result<T, ProxyError>;

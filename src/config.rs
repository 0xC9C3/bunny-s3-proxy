use clap::Parser;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum StorageRegion {
    #[clap(name = "de")]
    #[default]
    Falkenstein,
    #[clap(name = "uk")]
    London,
    #[clap(name = "ny")]
    NewYork,
    #[clap(name = "la")]
    LosAngeles,
    #[clap(name = "sg")]
    Singapore,
    #[clap(name = "se")]
    Stockholm,
    #[clap(name = "br")]
    SaoPaulo,
    #[clap(name = "jh")]
    Johannesburg,
    #[clap(name = "syd")]
    Sydney,
}

impl StorageRegion {
    pub fn base_url(&self) -> &'static str {
        match self {
            Self::Falkenstein => "https://storage.bunnycdn.com",
            Self::London => "https://uk.storage.bunnycdn.com",
            Self::NewYork => "https://ny.storage.bunnycdn.com",
            Self::LosAngeles => "https://la.storage.bunnycdn.com",
            Self::Singapore => "https://sg.storage.bunnycdn.com",
            Self::Stockholm => "https://se.storage.bunnycdn.com",
            Self::SaoPaulo => "https://br.storage.bunnycdn.com",
            Self::Johannesburg => "https://jh.storage.bunnycdn.com",
            Self::Sydney => "https://syd.storage.bunnycdn.com",
        }
    }

    pub fn code(&self) -> &'static str {
        match self {
            Self::Falkenstein => "de",
            Self::London => "uk",
            Self::NewYork => "ny",
            Self::LosAngeles => "la",
            Self::Singapore => "sg",
            Self::Stockholm => "se",
            Self::SaoPaulo => "br",
            Self::Johannesburg => "jh",
            Self::Sydney => "syd",
        }
    }
}

impl fmt::Display for StorageRegion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.code())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum LogLevel {
    Error,
    Warn,
    #[default]
    Info,
    Debug,
    Trace,
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Error => write!(f, "error"),
            Self::Warn => write!(f, "warn"),
            Self::Info => write!(f, "info"),
            Self::Debug => write!(f, "debug"),
            Self::Trace => write!(f, "trace"),
        }
    }
}

#[derive(Debug, Clone, Parser)]
#[command(name = "bunny-s3-proxy")]
#[command(about = "S3-compatible proxy for Bunny.net storage")]
pub struct Config {
    #[arg(short = 'z', long, env = "BUNNY_STORAGE_ZONE")]
    pub storage_zone: String,

    #[arg(short = 'k', long, env = "BUNNY_ACCESS_KEY")]
    pub access_key: String,

    #[arg(short = 'r', long, env = "BUNNY_REGION", default_value = "de")]
    pub region: StorageRegion,

    #[arg(long, env = "S3_ACCESS_KEY_ID", default_value = "bunny")]
    pub s3_access_key_id: String,

    #[arg(long, env = "S3_SECRET_ACCESS_KEY", default_value = "bunny")]
    pub s3_secret_access_key: String,

    #[arg(
        short = 'l',
        long,
        env = "LISTEN_ADDR",
        default_value = "127.0.0.1:9000"
    )]
    pub listen_addr: SocketAddr,

    #[arg(short = 's', long, env = "SOCKET_PATH")]
    pub socket_path: Option<PathBuf>,

    #[arg(short = 'L', long, env = "LOG_LEVEL", default_value = "info")]
    pub log_level: LogLevel,

    #[arg(long, env = "REDIS_URL")]
    pub redis_url: Option<String>,

    #[arg(long, env = "REDIS_LOCK_TTL_MS", default_value = "30000")]
    pub redis_lock_ttl_ms: u64,
}

#[derive(Debug, Clone)]
pub struct StorageZoneConfig {
    pub name: String,
    pub access_key: String,
    pub region: StorageRegion,
}

impl From<&Config> for StorageZoneConfig {
    fn from(config: &Config) -> Self {
        Self {
            name: config.storage_zone.clone(),
            access_key: config.access_key.clone(),
            region: config.region,
        }
    }
}

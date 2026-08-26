//! Configuration and CLI parsing for the canal-rust binary.
//! Split out of main.rs so it can be covered by integration tests.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanalConfig {
    pub canal: CanalSection,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanalSection {
    #[serde(default = "default_server_id")]
    pub server_id: u64,
    #[serde(default = "default_start_journal")]
    pub start_journal_name: String,
    #[serde(default = "default_start_position")]
    pub start_position: u64,
    #[serde(default)]
    pub auth_token: Option<String>,
    pub mysql: MysqlConfig,
    pub store: StoreSection,
    pub server: ServerSection,
    #[serde(default)]
    pub filter: FilterSection,
    pub logging: LogSection,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilterSection {
    #[serde(default = "default_filter_pattern")]
    pub pattern: String,
    #[serde(default)]
    pub black_list: String,
}

impl Default for FilterSection {
    fn default() -> Self {
        Self {
            pattern: default_filter_pattern(),
            black_list: String::new(),
        }
    }
}

pub fn default_filter_pattern() -> String {
    ".*\\..*".to_string()
}

pub fn default_server_id() -> u64 {
    1001
}

pub fn default_start_journal() -> String {
    "mysql-bin.000001".to_string()
}

pub fn default_start_position() -> u64 {
    4
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MysqlConfig {
    pub host: String,
    #[serde(default = "default_mysql_port")]
    pub port: u16,
    pub username: String,
    pub password: String,
}

impl std::fmt::Debug for MysqlConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MysqlConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

pub fn default_mysql_port() -> u16 {
    3306
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreSection {
    #[serde(default = "default_buffer_size")]
    pub buffer_size: usize,
}

pub fn default_buffer_size() -> usize {
    16384
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerSection {
    #[serde(default = "default_bind")]
    pub bind: String,
    #[serde(default = "default_metrics_bind")]
    pub metrics_bind: String,
    #[serde(default = "default_idle_timeout")]
    pub idle_timeout_secs: u64,
}

pub fn default_idle_timeout() -> u64 {
    3600
}

pub fn default_metrics_bind() -> String {
    "127.0.0.1:9090".to_string()
}

pub fn default_bind() -> String {
    "127.0.0.1:11111".to_string()
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogSection {
    #[serde(default = "default_log_level")]
    pub level: String,
    #[serde(default = "default_log_format")]
    pub format: String,
}

pub fn default_log_level() -> String {
    "info".to_string()
}

pub fn default_log_format() -> String {
    "json".to_string()
}

// -- CLI --

#[derive(Parser)]
#[command(
    name = "canal-rust",
    version = env!("CARGO_PKG_VERSION"),
    about = "MySQL binlog subscription tool"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    Server {
        #[arg(short, long, default_value = "canal.yaml")]
        config: PathBuf,
    },
    Dump {
        #[arg(short, long, default_value = "canal.yaml")]
        config: PathBuf,
    },
}

// -- Config loading --

pub fn load_config(config_path: &Path) -> Result<CanalConfig> {
    let content = std::fs::read_to_string(config_path)
        .with_context(|| format!("Failed to read config: {}", config_path.display()))?;
    if content.len() > 10 * 1024 * 1024 {
        anyhow::bail!(
            "Config file exceeds maximum size (10MB): {} bytes",
            content.len()
        );
    }
    serde_yaml::from_str(&content)
        .with_context(|| format!("Failed to parse config: {}", config_path.display()))
}

pub fn setup_logging(logging: &LogSection) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        tracing_subscriber::EnvFilter::try_new(&logging.level).unwrap_or_else(|_| {
            tracing::warn!(
                "Invalid log level '{}', falling back to 'info'",
                logging.level
            );
            tracing_subscriber::EnvFilter::new("info")
        })
    });

    match logging.format.as_str() {
        "json" => {
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .json()
                .init();
        }
        _ => {
            tracing_subscriber::fmt().with_env_filter(filter).init();
        }
    }
}

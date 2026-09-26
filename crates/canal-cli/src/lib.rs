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
    #[serde(default, deserialize_with = "deserialize_auth_token")]
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

/// Deserialize `auth_token`, treating an empty (or whitespace-only) string as
/// "not set".
///
/// `canal.yaml.example` documents `auth_token: ""` as "留空 = 不启用认证", but a
/// bare `Option<String>` deserializes that to `Some("")` — which would enable
/// authentication with an empty secret. Normalizing here, at the trust
/// boundary, keeps the parsed config honest for every caller.
fn deserialize_auth_token<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let token = Option::<String>::deserialize(deserializer)?;
    Ok(token.filter(|t| !t.trim().is_empty()))
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
    /// Explicit Admin API bind address. When absent, the Admin API stays on
    /// loopback at the Canal port + 1 (see `run_server` in main.rs).
    #[serde(default)]
    pub admin_bind: Option<String>,
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

// The mascot itself lives in `canal-common`, which the admin server also links;
// re-exported here so `canal_cli::CANAL_CRAB` keeps working for existing users.
pub use canal_common::pet::{banner as pet_banner, CANAL_CRAB, PET_NAME, PET_SVG};

#[derive(Parser)]
#[command(
    name = "canal-rust",
    version = env!("CARGO_PKG_VERSION"),
    about = "MySQL binlog subscription tool",
    before_help = CANAL_CRAB
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
    /// Meet 小运, the project mascot (and print where the artwork lives)
    Pet,
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

/// Resolve the Admin API bind address.
///
/// An explicit `server.admin_bind` is used verbatim (that is what makes the
/// Admin API reachable from outside the container in Docker); when it is absent
/// the Admin API stays on loopback at the Canal port + 1, as before.
pub fn resolve_admin_bind(
    configured: Option<&str>,
    canal_bind: std::net::SocketAddr,
) -> Result<String> {
    let admin_bind = match configured {
        Some(explicit) => explicit.to_string(),
        None => {
            let admin_port = canal_bind
                .port()
                .checked_add(1)
                .context("Admin port overflow: main port 65535 has no room for admin")?;
            format!("127.0.0.1:{}", admin_port)
        }
    };
    let addr: std::net::SocketAddr = admin_bind
        .parse()
        .with_context(|| format!("Invalid admin bind address: {}", admin_bind))?;
    if addr.ip().is_unspecified() {
        tracing::warn!(
            "Admin API binding to {} -- ensure firewall protection",
            admin_bind
        );
    }
    Ok(admin_bind)
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

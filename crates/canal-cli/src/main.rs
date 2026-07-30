use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use serde::Deserialize;
use tracing_subscriber::{fmt, EnvFilter};

// ─── Configuration ───────────────────────────────────────

#[derive(Debug, Deserialize)]
struct CanalConfig {
    canal: CanalSection,
}

#[derive(Debug, Deserialize)]
struct CanalSection {
    mysql: MysqlConfig,
    store: StoreSection,
    server: ServerSection,
    logging: LogSection,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct MysqlConfig {
    host: String,
    #[serde(default = "default_mysql_port")]
    port: u16,
    username: String,
    password: String,
}

fn default_mysql_port() -> u16 {
    3306
}

#[derive(Debug, Deserialize)]
struct StoreSection {
    #[serde(default = "default_buffer_size")]
    buffer_size: usize,
}

fn default_buffer_size() -> usize {
    16384
}

#[derive(Debug, Deserialize)]
struct ServerSection {
    #[serde(default = "default_bind")]
    bind: String,
}

fn default_bind() -> String {
    "0.0.0.0:11111".to_string()
}

#[derive(Debug, Deserialize)]
struct LogSection {
    #[serde(default = "default_log_level")]
    level: String,
    #[serde(default = "default_log_format")]
    format: String,
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_log_format() -> String {
    "json".to_string()
}

// ─── CLI ─────────────────────────────────────────────────

/// Canal Rust — MySQL binlog incremental subscription & consumption
#[derive(Parser)]
#[command(
    name = "canal-rust",
    version = "0.1.0",
    about = "MySQL binlog subscription tool"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the Canal server (MySQL binlog → clients)
    Server {
        /// Path to configuration file
        #[arg(short, long, default_value = "canal.yaml")]
        config: PathBuf,
    },
    /// Dump binlog events to stdout (debugging)
    Dump {
        /// Path to configuration file
        #[arg(short, long, default_value = "canal.yaml")]
        config: PathBuf,
    },
}

// ─── Main ────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Server { config } => run_server(config).await,
        Commands::Dump { config } => run_dump(config).await,
    }
}

async fn run_server(config_path: PathBuf) -> Result<()> {
    let config: CanalConfig = {
        let content = std::fs::read_to_string(&config_path)
            .with_context(|| format!("Failed to read config: {}", config_path.display()))?;
        serde_yaml::from_str(&content)
            .with_context(|| format!("Failed to parse config: {}", config_path.display()))?
    };

    setup_logging(&config.canal.logging);

    let bind_addr: SocketAddr = config
        .canal
        .server
        .bind
        .parse()
        .with_context(|| format!("Invalid bind address: {}", config.canal.server.bind))?;

    tracing::info!("Starting canal-rust server v{}", env!("CARGO_PKG_VERSION"));
    tracing::info!(
        "MySQL source: {}:{}",
        config.canal.mysql.host,
        config.canal.mysql.port
    );
    tracing::info!(
        "Store: memory, buffer_size={}",
        config.canal.store.buffer_size
    );
    tracing::info!("Listening on {}", bind_addr);

    let store = canal_store::memory::MemoryEventStore::new(config.canal.store.buffer_size);
    let server = canal_server::server::CanalServer::new(bind_addr, store);

    server.serve().await?;

    Ok(())
}

async fn run_dump(config_path: PathBuf) -> Result<()> {
    let _config: CanalConfig = {
        let content = std::fs::read_to_string(&config_path)
            .with_context(|| format!("Failed to read config: {}", config_path.display()))?;
        serde_yaml::from_str(&content)
            .with_context(|| format!("Failed to parse config: {}", config_path.display()))?
    };

    println!("Dump mode: connecting to MySQL and printing binlog events...");
    println!("(binlog crate integration pending)");
    Ok(())
}

fn setup_logging(logging: &LogSection) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&logging.level));

    match logging.format.as_str() {
        "json" => {
            fmt().with_env_filter(filter).json().init();
        }
        _ => {
            fmt().with_env_filter(filter).init();
        }
    }
}

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use canal_admin::AdminServer;
use canal_binlog::BinlogConnector;
use canal_common::FilterPattern;
use canal_instance::instance::{CanalInstance, InstanceConfig, InstanceManager};
use canal_prometheus::{CanalMetrics, MetricsServer};
use clap::{Parser, Subcommand};
use serde::Deserialize;
use tracing_subscriber::{fmt, EnvFilter};

// -- Configuration --

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CanalConfig {
    canal: CanalSection,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CanalSection {
    #[serde(default = "default_server_id")]
    server_id: u64,
    #[serde(default = "default_start_journal")]
    start_journal_name: String,
    #[serde(default = "default_start_position")]
    start_position: u64,
    #[serde(default)]
    auth_token: Option<String>,
    mysql: MysqlConfig,
    store: StoreSection,
    server: ServerSection,
    #[serde(default)]
    filter: FilterSection,
    logging: LogSection,
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct FilterSection {
    #[serde(default = "default_filter_pattern")]
    pattern: String,
    #[serde(default)]
    black_list: String,
}

fn default_filter_pattern() -> String {
    ".*\\..*".to_string()
}

fn default_server_id() -> u64 {
    1001
}

fn default_start_journal() -> String {
    "mysql-bin.000001".to_string()
}

fn default_start_position() -> u64 {
    4
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MysqlConfig {
    host: String,
    #[serde(default = "default_mysql_port")]
    port: u16,
    username: String,
    password: String,
    #[serde(default)]
    charset: Option<String>,
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

fn default_mysql_port() -> u16 {
    3306
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoreSection {
    #[serde(default = "default_buffer_size")]
    buffer_size: usize,
}

fn default_buffer_size() -> usize {
    16384
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ServerSection {
    #[serde(default = "default_bind")]
    bind: String,
    #[serde(default = "default_metrics_bind")]
    metrics_bind: String,
    #[serde(default = "default_idle_timeout")]
    idle_timeout_secs: u64,
}

fn default_idle_timeout() -> u64 {
    3600
}

fn default_metrics_bind() -> String {
    "127.0.0.1:9090".to_string()
}

fn default_bind() -> String {
    "127.0.0.1:11111".to_string()
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
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

// -- CLI --

#[derive(Parser)]
#[command(
    name = "canal-rust",
    version = env!("CARGO_PKG_VERSION"),
    about = "MySQL binlog subscription tool"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Server {
        #[arg(short, long, default_value = "canal.yaml")]
        config: PathBuf,
    },
    Dump {
        #[arg(short, long, default_value = "canal.yaml")]
        config: PathBuf,
    },
}

// -- Main --

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Server { config } => run_server(config).await,
        Commands::Dump { config } => run_dump(config).await,
    }
}

fn load_config(config_path: &std::path::Path) -> Result<CanalConfig> {
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

async fn run_server(config_path: PathBuf) -> Result<()> {
    let config = load_config(&config_path)?;
    setup_logging(&config.canal.logging);

    let bind_addr: SocketAddr = config
        .canal
        .server
        .bind
        .parse()
        .with_context(|| format!("Invalid bind address: {}", config.canal.server.bind))?;

    if bind_addr.ip().is_unspecified() {
        tracing::warn!("Server binding to 0.0.0.0 -- ensure firewall protection");
    }

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

    let metrics = Arc::new(CanalMetrics::new());
    let metrics_bind: SocketAddr = config.canal.server.metrics_bind.parse().with_context(|| {
        format!(
            "Invalid metrics bind address: {}",
            config.canal.server.metrics_bind
        )
    })?;
    let metrics_server = MetricsServer::new(metrics_bind, metrics.clone());
    let _metrics_task = metrics_server
        .start()
        .await
        .context("Failed to start metrics server")?;
    tracing::info!("Metrics server listening on {}", metrics_bind);

    // -- InstanceManager setup --
    let instance_mgr = Arc::new(InstanceManager::new());

    let instance_config = InstanceConfig {
        destination: "default".to_string(),
        mysql_host: config.canal.mysql.host.clone(),
        mysql_port: config.canal.mysql.port,
        mysql_username: config.canal.mysql.username.clone(),
        mysql_password: config.canal.mysql.password.clone(),
        mysql_server_id: config.canal.server_id,
        start_position: canal_common::LogPosition::new(
            &config.canal.start_journal_name,
            config.canal.start_position,
        ),
        filter: FilterPattern {
            pattern: config.canal.filter.pattern.clone(),
            black_list: config.canal.filter.black_list.clone(),
        },
        store_buffer_size: config.canal.store.buffer_size,
        connector_names: vec![],
    };
    let instance =
        CanalInstance::new(instance_config, vec![]).context("Failed to create instance")?;
    let store = instance.store();
    instance_mgr.register(instance);

    // Start instances via manager
    instance_mgr
        .start_all()
        .await
        .context("Failed to start instances")?;

    // Start admin API
    let admin_port = bind_addr.port().saturating_add(1);
    if admin_port == 0 {
        anyhow::bail!("Admin port overflow: main port {} is 65535", bind_addr.port());
    }
    let admin_bind = format!("127.0.0.1:{}", admin_port);
    let admin_server = AdminServer::new(&admin_bind, instance_mgr.clone());
    let _admin_task = admin_server
        .start()
        .await
        .context("Failed to start admin API")?;
    tracing::info!("Admin API listening on {}", admin_bind);

    let mut server = canal_server::server::CanalServer::new(bind_addr, store.clone());
    if let Some(ref token) = config.canal.auth_token {
        server = server.with_auth(token.clone());
    }
    let shutdown_token = server.shutdown_token();

    // Graceful shutdown on Ctrl-C
    let shutdown_for_signal = shutdown_token.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        tracing::info!("Received SIGINT, initiating graceful shutdown...");
        shutdown_for_signal.cancel();
    });

    // Spawn binlog connector
    let instance_for_binlog = instance_mgr
        .get("default")
        .context("Instance 'default' was not found after registration")?;
    let mysql_cfg = config.canal.mysql;
    let server_id = config.canal.server_id;
    let shutdown_for_binlog = shutdown_token.clone();

    let metrics_for_binlog = metrics.clone();
    let mut binlog_handle = tokio::spawn(async move {
        let pos = canal_common::LogPosition::new(
            &config.canal.start_journal_name,
            config.canal.start_position,
        );
        let (mut connector, mut rx) = match canal_binlog::connector::DefaultBinlogConnector::new(
            &mysql_cfg.host,
            mysql_cfg.port,
            &mysql_cfg.username,
            &mysql_cfg.password,
            server_id,
        ) {
            Ok(c) => c.with_channel(),
            Err(e) => {
                tracing::error!("Failed to create binlog connector: {}", e);
                shutdown_for_binlog.cancel();
                return;
            }
        };

        if let Err(e) = connector.connect(&pos).await {
            tracing::error!("Binlog connector failed to connect: {}", e);
            shutdown_for_binlog.cancel();
            return;
        }
        tracing::info!("Binlog connector started");

        let mut batch = Vec::new();
        let mut flush_interval = tokio::time::interval(Duration::from_millis(200));
        loop {
            tokio::select! {
                result = rx.recv() => {
                    match result {
                        Some(Ok(event)) => {
                            metrics_for_binlog.inc_parsed(1);
                            batch.push(event);
                            if batch.len() >= 256 {
                                if let Err(e) = instance_for_binlog.feed(batch.split_off(0)).await {
                                    tracing::error!("Failed to feed events: {}", e);
                                }
                            }
                        }
                        Some(Err(e)) => {
                            metrics_for_binlog.inc_parsed(1);
                            tracing::error!("Binlog event error: {}", e)
                        }
                        None => {
                            tracing::warn!("Binlog stream ended, triggering shutdown");
                            break;
                        }
                    }
                }
                _ = flush_interval.tick() => {
                    if !batch.is_empty() {
                        if let Err(e) = instance_for_binlog.feed(batch.split_off(0)).await {
                            tracing::error!("Failed to flush batch: {}", e);
                        }
                    }
                }
            }
        }

        // Flush remaining events before shutdown
        if !batch.is_empty() {
            if let Err(e) = instance_for_binlog.feed(batch).await {
                tracing::error!("Failed to flush remaining batch on shutdown: {}", e);
            }
        }

        // Trigger graceful server shutdown
        shutdown_for_binlog.cancel();
    });

    // Run server (blocks until shutdown_token is cancelled)
    server.serve().await?;

    // Give the binlog task time to flush remaining events gracefully
    match tokio::time::timeout(Duration::from_secs(5), &mut binlog_handle).await {
        Ok(Ok(())) => tracing::info!("Binlog connector task completed"),
        Ok(Err(e)) => tracing::error!("Binlog connector task panicked: {}", e),
        Err(_) => {
            tracing::warn!("Binlog connector task did not finish within timeout, aborting");
            binlog_handle.abort();
            let _ = binlog_handle.await;
        }
    }

    Ok(())
}

async fn run_dump(config_path: PathBuf) -> Result<()> {
    let config = load_config(&config_path)?;
    setup_logging(&config.canal.logging);

    tracing::info!(
        "Connecting to MySQL {}:{}",
        config.canal.mysql.host,
        config.canal.mysql.port
    );

    let pos = canal_common::LogPosition::new(
        &config.canal.start_journal_name,
        config.canal.start_position,
    );
    let server_id = config.canal.server_id;
    let (mut connector, mut rx) = canal_binlog::connector::DefaultBinlogConnector::new(
        &config.canal.mysql.host,
        config.canal.mysql.port,
        &config.canal.mysql.username,
        &config.canal.mysql.password,
        server_id,
    )
    .context("Failed to create binlog connector")?
    .with_channel();

    connector
        .connect(&pos)
        .await
        .context("Failed to connect to MySQL")?;
    eprintln!("Connected. Streaming binlog events...\n");

    let mut count: u64 = 0;
    while let Some(result) = rx.recv().await {
        match result {
            Ok(event) => {
                count += 1;
                println!(
                    "[{}] pos={}:{} schema={}.{} type={:?}",
                    count,
                    event.journal_name,
                    event.position,
                    event.schema_name,
                    event.table_name,
                    event.entry_type,
                );
                if let Some(ref sql) = event.ddl_sql {
                    println!("  DDL: {}", sql);
                }
                if let Some(ref rc) = event.row_change {
                    println!("  DML: {:?}", rc.dml_type);
                }
            }
            Err(e) => eprintln!("Error: {}", e),
        }
    }

    eprintln!("\nDone. {} events received.", count);
    Ok(())
}

fn setup_logging(logging: &LogSection) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::try_new(&logging.level).unwrap_or_else(|_| {
            tracing::warn!(
                "Invalid log level '{}', falling back to 'info'",
                logging.level
            );
            EnvFilter::new("info")
        })
    });

    match logging.format.as_str() {
        "json" => {
            fmt().with_env_filter(filter).json().init();
        }
        _ => {
            fmt().with_env_filter(filter).init();
        }
    }
}

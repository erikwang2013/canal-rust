use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use canal_admin::AdminServer;
use canal_binlog::BinlogConnector;
use canal_common::FilterPattern;
use canal_cli::{load_config, setup_logging, Cli, Commands};
use canal_instance::instance::{CanalInstance, InstanceConfig, InstanceManager};
use canal_prometheus::{CanalMetrics, MetricsServer};
use clap::Parser;

// -- Main --

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Server { config } => run_server(config).await,
        Commands::Dump { config } => run_dump(config).await,
    }
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
    metrics.set_instances_active(instance_mgr.running_count() as u64);

    // Start admin API
    let admin_port = bind_addr
        .port()
        .checked_add(1)
        .context("Admin port overflow: main port 65535 has no room for admin")?;
    let admin_bind = format!("127.0.0.1:{}", admin_port);
    let admin_server = AdminServer::new(&admin_bind, instance_mgr.clone());
    let _admin_task = admin_server
        .start()
        .await
        .context("Failed to start admin API")?;
    tracing::info!("Admin API listening on {}", admin_bind);

    let idle_timeout = config.canal.server.idle_timeout_secs;
    let mut server = canal_server::server::CanalServer::new(bind_addr, store.clone())
        .with_idle_timeout(idle_timeout);
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
                            batch.push(event);
                            if batch.len() >= 256 {
                                if let Err(e) = instance_for_binlog.feed(batch.split_off(0)).await {
                                    tracing::error!("Failed to feed events: {}", e);
                                }
                            }
                        }
                        Some(Err(e)) => {
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

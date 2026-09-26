//! Integration tests for canal-cli config loading and CLI parsing.
//! All tests are purely local: config parsing from in-memory YAML strings
//! and temp files. `run_server`/`run_dump` are not covered because they
//! require a live MySQL server; `setup_logging` is not covered because
//! installing a global tracing subscriber can only happen once per process.

use std::net::SocketAddr;
use std::path::PathBuf;

use canal_cli::{load_config, resolve_admin_bind, Cli, Commands, MysqlConfig};
use clap::Parser;

const FULL_CONFIG: &str = r#"
canal:
  server_id: 42
  start_journal_name: mysql-bin.000777
  start_position: 12345
  auth_token: s3cret
  mysql:
    host: db.example.com
    port: 3307
    username: repl
    password: pw
  store:
    buffer_size: 4096
  server:
    bind: 0.0.0.0:11111
    metrics_bind: 0.0.0.0:9090
    admin_bind: 0.0.0.0:11112
    idle_timeout_secs: 7200
  filter:
    pattern: 'testdb\..*'
    black_list: 'testdb\.secret'
  logging:
    level: debug
    format: text
"#;

const MINIMAL_CONFIG: &str = r#"
canal:
  mysql:
    host: localhost
    username: root
    password: pass
  store: {}
  server: {}
  logging: {}
"#;

fn temp_config_path(name: &str, content: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "canal-cli-test-{}-{}.yaml",
        std::process::id(),
        name
    ));
    std::fs::write(&path, content).unwrap();
    path
}

// ── YAML config parsing ─────────────────────────────────────

#[test]
fn parse_full_config() {
    let config: canal_cli::CanalConfig = serde_yaml::from_str(FULL_CONFIG).unwrap();
    let c = &config.canal;
    assert_eq!(c.server_id, 42);
    assert_eq!(c.start_journal_name, "mysql-bin.000777");
    assert_eq!(c.start_position, 12345);
    assert_eq!(c.auth_token.as_deref(), Some("s3cret"));
    assert_eq!(c.mysql.host, "db.example.com");
    assert_eq!(c.mysql.port, 3307);
    assert_eq!(c.mysql.username, "repl");
    assert_eq!(c.mysql.password, "pw");
    assert_eq!(c.store.buffer_size, 4096);
    assert_eq!(c.server.bind, "0.0.0.0:11111");
    assert_eq!(c.server.metrics_bind, "0.0.0.0:9090");
    assert_eq!(c.server.admin_bind.as_deref(), Some("0.0.0.0:11112"));
    assert_eq!(c.server.idle_timeout_secs, 7200);
    assert_eq!(c.filter.pattern, "testdb\\..*");
    assert_eq!(c.filter.black_list, "testdb\\.secret");
    assert_eq!(c.logging.level, "debug");
    assert_eq!(c.logging.format, "text");
}

#[test]
fn parse_minimal_config_applies_defaults() {
    let config: canal_cli::CanalConfig = serde_yaml::from_str(MINIMAL_CONFIG).unwrap();
    let c = &config.canal;
    assert_eq!(c.server_id, 1001);
    assert_eq!(c.start_journal_name, "mysql-bin.000001");
    assert_eq!(c.start_position, 4);
    assert_eq!(c.auth_token, None);
    assert_eq!(c.mysql.port, 3306);
    assert_eq!(c.store.buffer_size, 16384);
    assert_eq!(c.server.bind, "127.0.0.1:11111");
    assert_eq!(c.server.metrics_bind, "127.0.0.1:9090");
    assert_eq!(c.server.admin_bind, None);
    assert_eq!(c.server.idle_timeout_secs, 3600);
    assert_eq!(c.filter.pattern, ".*\\..*");
    assert!(c.filter.black_list.is_empty());
    assert_eq!(c.logging.level, "info");
    assert_eq!(c.logging.format, "json");
}

#[test]
fn parse_rejects_unknown_top_level_field() {
    let err = serde_yaml::from_str::<canal_cli::CanalConfig>(
        "unknown_field: 1\ncanal:\n  mysql:\n    host: h\n    username: u\n    password: p\n  store: {}\n  server: {}\n  logging: {}\n",
    )
    .unwrap_err();
    assert!(err.to_string().contains("unknown field"), "got: {err}");
}

#[test]
fn parse_rejects_unknown_nested_field() {
    let err = serde_yaml::from_str::<canal_cli::CanalConfig>(
        "canal:\n  mysql:\n    host: h\n    username: u\n    password: p\n    driver: mysql\n  store: {}\n  server: {}\n  logging: {}\n",
    )
    .unwrap_err();
    assert!(err.to_string().contains("unknown field"), "got: {err}");
}

#[test]
fn parse_rejects_missing_mysql_section() {
    let err = serde_yaml::from_str::<canal_cli::CanalConfig>(
        "canal:\n  store: {}\n  server: {}\n  logging: {}\n",
    )
    .unwrap_err();
    assert!(err.to_string().contains("mysql"), "got: {err}");
}

#[test]
fn parse_rejects_missing_canal_root() {
    let err =
        serde_yaml::from_str::<canal_cli::CanalConfig>("store:\n  buffer_size: 1\n").unwrap_err();
    assert!(err.to_string().contains("canal"), "got: {err}");
}

#[test]
fn parse_rejects_invalid_yaml() {
    let err = serde_yaml::from_str::<canal_cli::CanalConfig>("canal: 'unterminated").unwrap_err();
    assert!(!err.to_string().is_empty());
}

#[test]
fn parse_rejects_wrong_value_type() {
    let err = serde_yaml::from_str::<canal_cli::CanalConfig>(
        "canal:\n  server_id: not-a-number\n  mysql:\n    host: h\n    username: u\n    password: p\n  store: {}\n  server: {}\n  logging: {}\n",
    )
    .unwrap_err();
    assert!(err.to_string().contains("invalid type"), "got: {err}");
}

#[test]
fn parse_treats_empty_auth_token_as_disabled() {
    // canal.yaml.example documents `auth_token: ""` as "留空 = 不启用认证":
    // an empty (or whitespace-only) token must parse as "no auth", never as
    // `Some("")` — which would turn on auth with an empty secret.
    for empty in ["''", "'   '"] {
        let config: canal_cli::CanalConfig = serde_yaml::from_str(&format!(
            "canal:\n  auth_token: {empty}\n  mysql:\n    host: h\n    username: u\n    password: p\n  store: {{}}\n  server: {{}}\n  logging: {{}}\n",
        ))
        .unwrap();
        assert_eq!(config.canal.auth_token, None, "token was {empty}");
    }
}

#[test]
fn parse_accepts_non_empty_auth_token() {
    let config: canal_cli::CanalConfig = serde_yaml::from_str(
        "canal:\n  auth_token: ' s3cret '\n  mysql:\n    host: h\n    username: u\n    password: p\n  store: {}\n  server: {}\n  logging: {}\n",
    )
    .unwrap();
    assert_eq!(config.canal.auth_token.as_deref(), Some(" s3cret "));
}

// ── admin bind resolution ────────────────────────────────────

#[test]
fn admin_bind_defaults_to_loopback_next_to_canal_port() {
    let bind: SocketAddr = "127.0.0.1:11111".parse().unwrap();
    assert_eq!(resolve_admin_bind(None, bind).unwrap(), "127.0.0.1:11112");
    // The derived address is always loopback, whatever the main bind is.
    let public: SocketAddr = "0.0.0.0:11111".parse().unwrap();
    assert_eq!(resolve_admin_bind(None, public).unwrap(), "127.0.0.1:11112");
}

#[test]
fn admin_bind_explicit_value_is_used_verbatim() {
    let bind: SocketAddr = "127.0.0.1:11111".parse().unwrap();
    assert_eq!(
        resolve_admin_bind(Some("0.0.0.0:11112"), bind).unwrap(),
        "0.0.0.0:11112"
    );
    assert_eq!(
        resolve_admin_bind(Some("[::1]:8081"), bind).unwrap(),
        "[::1]:8081"
    );
}

#[test]
fn admin_bind_malformed_value_fails_clearly() {
    let bind: SocketAddr = "127.0.0.1:11111".parse().unwrap();
    let err = resolve_admin_bind(Some("not-an-address"), bind).unwrap_err();
    assert!(
        err.to_string().contains("Invalid admin bind address"),
        "got: {err}"
    );
    assert!(err.to_string().contains("not-an-address"), "got: {err}");
}

#[test]
fn admin_bind_overflow_on_max_port() {
    let bind: SocketAddr = "127.0.0.1:65535".parse().unwrap();
    let err = resolve_admin_bind(None, bind).unwrap_err();
    assert!(
        err.to_string().contains("Admin port overflow"),
        "got: {err}"
    );
}

// ── mysql config debug redaction ─────────────────────────────

#[test]
fn mysql_config_debug_redacts_password() {
    let config: canal_cli::CanalConfig = serde_yaml::from_str(
        "canal:\n  mysql:\n    host: h\n    username: u\n    password: topsecret\n  store: {}\n  server: {}\n  logging: {}\n",
    )
    .unwrap();
    let debug_str = format!("{:?}", config.canal.mysql);
    assert!(debug_str.contains("h"));
    assert!(debug_str.contains("u"));
    assert!(!debug_str.contains("topsecret"));
}

#[test]
fn mysql_config_type_is_public() {
    // Ensures the MysqlConfig type is usable from tests (already used above);
    // also exercises its Default-free construction contract via full config.
    let config: canal_cli::CanalConfig = serde_yaml::from_str(FULL_CONFIG).unwrap();
    let _: MysqlConfig = config.canal.mysql;
}

// ── load_config file handling ────────────────────────────────

#[test]
fn load_config_from_file() {
    let path = temp_config_path("valid", MINIMAL_CONFIG);
    let config = load_config(&path).unwrap();
    assert_eq!(config.canal.mysql.host, "localhost");
    std::fs::remove_file(&path).ok();
}

#[test]
fn load_config_missing_file() {
    let err = load_config(&PathBuf::from("/nonexistent/canal-cli-test.yaml")).unwrap_err();
    assert!(
        err.to_string().contains("Failed to read config"),
        "got: {err}"
    );
}

#[test]
fn load_config_rejects_oversized_file() {
    let path = std::env::temp_dir().join(format!(
        "canal-cli-test-{}-oversized.yaml",
        std::process::id()
    ));
    // One byte over the 10MB limit.
    std::fs::write(&path, vec![b'x'; 10 * 1024 * 1024 + 1]).unwrap();
    let err = load_config(&path).unwrap_err();
    assert!(
        err.to_string().contains("exceeds maximum size"),
        "got: {err}"
    );
    std::fs::remove_file(&path).ok();
}

#[test]
fn load_config_invalid_content() {
    let path = temp_config_path("invalid", "not: [valid: yaml");
    let err = load_config(&path).unwrap_err();
    assert!(
        err.to_string().contains("Failed to parse config"),
        "got: {err}"
    );
    std::fs::remove_file(&path).ok();
}

// ── CLI parsing ──────────────────────────────────────────────

#[test]
fn cli_parses_server_with_custom_config() {
    let cli = Cli::parse_from(["canal-rust", "server", "-c", "/tmp/my.yaml"]);
    match cli.command {
        Commands::Server { config } => assert_eq!(config, PathBuf::from("/tmp/my.yaml")),
        _ => panic!("expected Server command"),
    }
}

#[test]
fn cli_parses_dump_with_default_config() {
    let cli = Cli::parse_from(["canal-rust", "dump"]);
    match cli.command {
        Commands::Dump { config } => assert_eq!(config, PathBuf::from("canal.yaml")),
        _ => panic!("expected Dump command"),
    }
}

#[test]
fn cli_parses_server_long_flag() {
    let cli = Cli::parse_from(["canal-rust", "server", "--config", "/etc/canal.yaml"]);
    match cli.command {
        Commands::Server { config } => assert_eq!(config, PathBuf::from("/etc/canal.yaml")),
        _ => panic!("expected Server command"),
    }
}

#[test]
fn cli_rejects_missing_subcommand() {
    match Cli::try_parse_from(["canal-rust"]) {
        Err(e) => assert!(e.to_string().contains("subcommand"), "got: {e}"),
        Ok(_) => panic!("expected parse error"),
    }
}

#[test]
fn cli_rejects_unknown_subcommand() {
    match Cli::try_parse_from(["canal-rust", "frobnicate"]) {
        Err(e) => assert!(e.to_string().contains("unrecognized"), "got: {e}"),
        Ok(_) => panic!("expected parse error"),
    }
}

#[test]
fn cli_rejects_unknown_flag() {
    match Cli::try_parse_from(["canal-rust", "server", "--bogus"]) {
        Err(e) => assert!(e.to_string().contains("unexpected"), "got: {e}"),
        Ok(_) => panic!("expected parse error"),
    }
}

#[test]
fn cli_help_carries_the_mascot() {
    use clap::CommandFactory;
    let help = Cli::command().render_help().to_string();
    assert!(help.contains(canal_cli::CANAL_CRAB), "help lost the crab");
    assert!(help.contains("server"), "help lost the subcommands");
}

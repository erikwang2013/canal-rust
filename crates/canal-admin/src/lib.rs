use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Json, Router,
};
use canal_common::lifecycle::CanalLifecycle;
use canal_instance::instance::InstanceManager;
use serde::Serialize;
use std::sync::Arc;
use std::time::Instant;
use tracing::info;

#[derive(Debug, Clone, Serialize)]
pub struct InstanceSummary {
    pub name: String,
    pub destination: String,
    pub running: bool,
}

#[derive(Clone)]
pub struct AdminState {
    pub instance_manager: Arc<InstanceManager>,
    pub started_at: Instant,
    pub admin_token: Option<String>,
}

impl std::fmt::Debug for AdminState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdminState")
            .field("started_at", &self.started_at)
            .field("has_auth", &self.admin_token.is_some())
            .finish()
    }
}

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub uptime_seconds: u64,
}

#[derive(Debug, Serialize)]
pub struct InstanceListResponse {
    pub instances: Vec<InstanceSummary>,
}

#[derive(Debug, Serialize)]
pub struct StatusMessage {
    pub status: String,
    pub message: String,
}

pub struct AdminServer {
    pub bind_addr: String,
    state: AdminState,
}

impl std::fmt::Debug for AdminServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdminServer")
            .field("bind_addr", &self.bind_addr)
            .finish()
    }
}

impl AdminServer {
    pub fn new(bind_addr: &str, instance_manager: Arc<InstanceManager>) -> Self {
        Self {
            bind_addr: bind_addr.to_string(),
            state: AdminState {
                instance_manager,
                started_at: Instant::now(),
                admin_token: None,
            },
        }
    }

    pub fn with_auth(mut self, token: String) -> Self {
        self.state.admin_token = Some(token);
        self
    }

    pub async fn start(self) -> std::io::Result<tokio::task::JoinHandle<()>> {
        let addr = self.bind_addr.clone();
        info!("Admin API starting on {}", addr);

        let app = Router::new()
            .route("/health", get(health_handler))
            .route("/api/instances", get(list_instances))
            .route("/api/instances/:name/start", post(start_instance))
            .route("/api/instances/:name/stop", post(stop_instance))
            .with_state(self.state);

        let listener = tokio::net::TcpListener::bind(&addr).await?;
        let task = tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, app).await {
                tracing::error!("Admin server error: {}", e);
            }
        });
        Ok(task)
    }
}

fn check_auth(headers: &HeaderMap, expected: &Option<String>) -> Result<(), StatusCode> {
    match expected {
        None => Ok(()),
        Some(token) => {
            // An empty configured token would otherwise match a missing/empty
            // Authorization header via the raw-token path below, silently
            // disabling auth. Reject it up front.
            if token.is_empty() {
                return Err(StatusCode::UNAUTHORIZED);
            }
            let auth = headers
                .get("Authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            let expected_bearer = format!("Bearer {}", token);
            if constant_time_eq(auth.as_bytes(), expected_bearer.as_bytes())
                || constant_time_eq(auth.as_bytes(), token.as_bytes())
            {
                Ok(())
            } else {
                Err(StatusCode::UNAUTHORIZED)
            }
        }
    }
}

/// Constant-time byte comparison to prevent timing side-channel attacks
/// on authentication tokens.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    // Constant-time comparison — no early return on length mismatch,
    // always runs the full XOR fold to avoid timing side-channel
    let max_len = a.len().max(b.len());
    let mut acc: u16 = (a.len() ^ b.len()) as u16;
    for i in 0..max_len {
        let x = a.get(i).copied().unwrap_or(0);
        let y = b.get(i).copied().unwrap_or(0);
        acc |= (x ^ y) as u16;
    }
    acc == 0
}

async fn health_handler(State(state): State<AdminState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "UP".into(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_seconds: state.started_at.elapsed().as_secs(),
    })
}

async fn list_instances(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> Result<Json<InstanceListResponse>, StatusCode> {
    check_auth(&headers, &state.admin_token)?;
    let dests = state.instance_manager.list();
    let mut instances = Vec::new();
    for d in dests {
        let running = state
            .instance_manager
            .get(&d)
            .map(|i| i.is_running())
            .unwrap_or(false);
        instances.push(InstanceSummary {
            name: d.clone(),
            destination: d,
            running,
        });
    }
    Ok(Json(InstanceListResponse { instances }))
}

async fn start_instance(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<Json<StatusMessage>, StatusCode> {
    check_auth(&headers, &state.admin_token)?;
    match state.instance_manager.get(&name) {
        Some(instance) => match instance.start().await {
            Ok(()) => Ok(Json(StatusMessage {
                status: "ok".into(),
                message: format!("Instance '{}' started", name),
            })),
            Err(e) => Ok(Json(StatusMessage {
                status: "error".into(),
                message: format!("Failed to start '{}': {}", name, e),
            })),
        },
        None => Ok(Json(StatusMessage {
            status: "not_found".into(),
            message: format!("Instance '{}' not found", name),
        })),
    }
}

async fn stop_instance(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<Json<StatusMessage>, StatusCode> {
    check_auth(&headers, &state.admin_token)?;
    match state.instance_manager.get(&name) {
        Some(instance) => match instance.stop().await {
            Ok(()) => Ok(Json(StatusMessage {
                status: "ok".into(),
                message: format!("Instance '{}' stopped", name),
            })),
            Err(e) => Ok(Json(StatusMessage {
                status: "error".into(),
                message: format!("Failed to stop '{}': {}", name, e),
            })),
        },
        None => Ok(Json(StatusMessage {
            status: "not_found".into(),
            message: format!("Instance '{}' not found", name),
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use canal_common::{FilterPattern, LogPosition};
    use canal_instance::instance::{CanalInstance, InstanceConfig};

    fn make_config(destination: &str) -> InstanceConfig {
        InstanceConfig {
            destination: destination.to_string(),
            mysql_host: "localhost".into(),
            mysql_port: 3306,
            mysql_username: "root".into(),
            mysql_password: "pass".into(),
            mysql_server_id: 1001,
            start_position: LogPosition::new("mysql-bin.000001", 4),
            filter: FilterPattern::default(),
            store_buffer_size: 1024,
            connector_names: vec![],
        }
    }

    fn register_instance(mgr: &Arc<InstanceManager>, destination: &str) {
        let instance = CanalInstance::new(make_config(destination), vec![]).unwrap();
        mgr.register(instance);
    }
    fn state_with(mgr: Arc<InstanceManager>, token: Option<String>) -> AdminState {
        AdminState {
            instance_manager: mgr,
            started_at: Instant::now(),
            admin_token: token,
        }
    }
    #[test]
    fn test_check_auth_no_token_required() {
        let headers = HeaderMap::new();
        assert_eq!(check_auth(&headers, &None), Ok(()));
    }

    #[test]
    fn test_check_auth_missing_header() {
        let headers = HeaderMap::new();
        assert_eq!(
            check_auth(&headers, &Some("secret".into())),
            Err(StatusCode::UNAUTHORIZED)
        );
    }

    #[test]
    fn test_check_auth_bearer_token_valid() {
        let mut headers = HeaderMap::new();
        headers.insert("Authorization", "Bearer secret".parse().unwrap());
        assert_eq!(check_auth(&headers, &Some("secret".into())), Ok(()));
    }

    #[test]
    fn test_check_auth_raw_token_valid() {
        let mut headers = HeaderMap::new();
        headers.insert("Authorization", "secret".parse().unwrap());
        assert_eq!(check_auth(&headers, &Some("secret".into())), Ok(()));
    }

    #[test]
    fn test_check_auth_wrong_token() {
        let mut headers = HeaderMap::new();
        headers.insert("Authorization", "Bearer wrong".parse().unwrap());
        assert_eq!(
            check_auth(&headers, &Some("secret".into())),
            Err(StatusCode::UNAUTHORIZED)
        );
    }

    #[test]
    fn test_check_auth_empty_header_value() {
        let mut headers = HeaderMap::new();
        headers.insert("Authorization", "".parse().unwrap());
        assert_eq!(
            check_auth(&headers, &Some("secret".into())),
            Err(StatusCode::UNAUTHORIZED)
        );
    }

    #[tokio::test]
    async fn test_register_and_list_instances() {
        let mgr = Arc::new(InstanceManager::new());
        let server = AdminServer::new("127.0.0.1:0", mgr);
        assert!(server.bind_addr.starts_with("127.0.0.1"));
    }

    #[tokio::test]
    async fn test_health_endpoint() {
        let mgr = Arc::new(InstanceManager::new());
        let server = AdminServer::new("127.0.0.1:0", mgr);
        let task = server.start().await.unwrap();
        task.abort();
    }

    #[tokio::test]
    async fn test_admin_server_with_auth() {
        let mgr = Arc::new(InstanceManager::new());
        let server = AdminServer::new("127.0.0.1:0", mgr).with_auth("test-token".into());
        assert!(server.state.admin_token.is_some());
    }

    #[test]
    fn test_admin_state_debug_masks_token() {
        let mgr = Arc::new(InstanceManager::new());
        let state = AdminState {
            instance_manager: mgr,
            started_at: std::time::Instant::now(),
            admin_token: Some("secret".into()),
        };
        let debug_str = format!("{:?}", state);
        assert!(debug_str.contains("has_auth"));
        assert!(!debug_str.contains("secret"));
    }

    // ── constant_time_eq edge cases ──────────────────────────

    #[test]
    fn test_constant_time_eq_cases() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(constant_time_eq(b"", b""));
        assert!(constant_time_eq("密钥".as_bytes(), "密钥".as_bytes()));
        assert!(constant_time_eq(&[0u8, 255, 1], &[0u8, 255, 1]));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
        assert!(!constant_time_eq(b"abc", b""));
        assert!(!constant_time_eq(b"", b"a"));
        assert!(!constant_time_eq(b"a", b"b"));
        assert!(!constant_time_eq("密钥".as_bytes(), "密码".as_bytes()));
    }

    // ── check_auth edge cases ────────────────────────────────

    #[test]
    fn test_check_auth_invalid_and_case_sensitive_headers() {
        let mut h1 = HeaderMap::new();
        h1.insert(
            "Authorization",
            HeaderValue::from_bytes(b"\xff\xfe").unwrap(),
        );
        assert_eq!(
            check_auth(&h1, &Some("secret".into())),
            Err(StatusCode::UNAUTHORIZED)
        );
        let mut h2 = HeaderMap::new();
        h2.insert("Authorization", "bearer secret".parse().unwrap());
        assert_eq!(
            check_auth(&h2, &Some("secret".into())),
            Err(StatusCode::UNAUTHORIZED)
        );
    }

    #[test]
    fn test_check_auth_empty_token_and_prefix_rejected() {
        assert_eq!(
            check_auth(&HeaderMap::new(), &Some(String::new())),
            Err(StatusCode::UNAUTHORIZED)
        );
        let mut headers = HeaderMap::new();
        headers.insert("Authorization", "Bearer sec".parse().unwrap());
        assert_eq!(
            check_auth(&headers, &Some("secret".into())),
            Err(StatusCode::UNAUTHORIZED)
        );
    }

    // ── handlers (direct invocation, no real port) ───────────

    #[tokio::test]
    async fn test_health_handler_fields() {
        let mgr = Arc::new(InstanceManager::new());
        let state = state_with(mgr, None);
        let resp = health_handler(State(state)).await;
        assert_eq!(resp.status, "UP");
        assert_eq!(resp.version, env!("CARGO_PKG_VERSION"));
        assert_eq!(resp.uptime_seconds, 0);
    }

    #[tokio::test]
    async fn test_list_instances_empty() {
        let mgr = Arc::new(InstanceManager::new());
        let state = state_with(mgr, None);
        let resp = list_instances(State(state), HeaderMap::new())
            .await
            .unwrap()
            .0;
        assert!(resp.instances.is_empty());
    }

    #[tokio::test]
    async fn test_list_instances_reports_running_state() {
        let mgr = Arc::new(InstanceManager::new());
        register_instance(&mgr, "one");
        register_instance(&mgr, "two");
        mgr.get("one").unwrap().start().await.unwrap();
        let state = state_with(mgr, None);
        let resp = list_instances(State(state), HeaderMap::new())
            .await
            .unwrap()
            .0;
        assert_eq!(resp.instances.len(), 2);
        let by_name = |n: &str| resp.instances.iter().find(|i| i.name == n).unwrap();
        assert!(by_name("one").running);
        assert_eq!(by_name("one").destination, "one");
        assert!(!by_name("two").running);
    }

    #[tokio::test]
    async fn test_handlers_require_auth_when_configured() {
        let mgr = Arc::new(InstanceManager::new());
        register_instance(&mgr, "one");
        let state = state_with(mgr, Some("token".into()));
        assert_eq!(
            list_instances(State(state.clone()), HeaderMap::new())
                .await
                .unwrap_err(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            start_instance(State(state.clone()), HeaderMap::new(), Path("one".into()))
                .await
                .unwrap_err(),
            StatusCode::UNAUTHORIZED
        );
        let mut headers = HeaderMap::new();
        headers.insert("Authorization", "Bearer token".parse().unwrap());
        let resp = list_instances(State(state), headers).await.unwrap().0;
        assert_eq!(resp.instances.len(), 1);
    }

    #[tokio::test]
    async fn test_start_instance_unknown_destination() {
        let mgr = Arc::new(InstanceManager::new());
        let state = state_with(mgr, None);
        let resp = start_instance(State(state), HeaderMap::new(), Path("ghost".into()))
            .await
            .unwrap()
            .0;
        assert_eq!(resp.status, "not_found");
        assert!(resp.message.contains("ghost"));
    }

    #[tokio::test]
    async fn test_start_instance_success_and_idempotent() {
        let mgr = Arc::new(InstanceManager::new());
        register_instance(&mgr, "db1");
        let state = state_with(mgr.clone(), None);
        let resp = start_instance(State(state), HeaderMap::new(), Path("db1".into()))
            .await
            .unwrap()
            .0;
        assert_eq!(resp.status, "ok");
        assert!(resp.message.contains("started"));
        assert!(mgr.get("db1").unwrap().is_running());
        // Starting an already-running instance is a no-op success.
        let state = state_with(mgr.clone(), None);
        let resp = start_instance(State(state), HeaderMap::new(), Path("db1".into()))
            .await
            .unwrap()
            .0;
        assert_eq!(resp.status, "ok");
        assert!(mgr.get("db1").unwrap().is_running());
    }

    #[tokio::test]
    async fn test_stop_instance_success_and_not_found() {
        let mgr = Arc::new(InstanceManager::new());
        register_instance(&mgr, "db1");
        mgr.get("db1").unwrap().start().await.unwrap();
        let state = state_with(mgr.clone(), None);
        let resp = stop_instance(State(state), HeaderMap::new(), Path("db1".into()))
            .await
            .unwrap()
            .0;
        assert_eq!(resp.status, "ok");
        assert!(!mgr.get("db1").unwrap().is_running());
        let state = state_with(mgr, None);
        let resp = stop_instance(State(state), HeaderMap::new(), Path("nope".into()))
            .await
            .unwrap()
            .0;
        assert_eq!(resp.status, "not_found");
    }
}

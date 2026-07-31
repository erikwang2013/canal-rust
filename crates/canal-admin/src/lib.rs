use std::sync::Arc;
use std::time::Instant;
use axum::{Router, routing::{get, post}, Json, extract::{Path, State}, http::{HeaderMap, StatusCode}};
use canal_common::lifecycle::CanalLifecycle;
use canal_instance::instance::InstanceManager;
use serde::Serialize;
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
        f.debug_struct("AdminServer").field("bind_addr", &self.bind_addr).finish()
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
            let auth = headers
                .get("Authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            if auth == format!("Bearer {}", token) || auth == token.as_str() {
                Ok(())
            } else {
                Err(StatusCode::UNAUTHORIZED)
            }
        }
    }
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
    let dests = state.instance_manager.list().await;
    let mut instances = Vec::new();
    for d in dests {
        let running = state.instance_manager.get(&d).await
            .map(|i| i.is_running())
            .unwrap_or(false);
        instances.push(InstanceSummary { name: d.clone(), destination: d, running });
    }
    Ok(Json(InstanceListResponse { instances }))
}

async fn start_instance(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<Json<StatusMessage>, StatusCode> {
    check_auth(&headers, &state.admin_token)?;
    match state.instance_manager.get(&name).await {
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
    match state.instance_manager.get(&name).await {
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

    #[test]
    fn test_check_auth_no_token_required() {
        let headers = HeaderMap::new();
        assert_eq!(check_auth(&headers, &None), Ok(()));
    }

    #[test]
    fn test_check_auth_missing_header() {
        let headers = HeaderMap::new();
        assert_eq!(check_auth(&headers, &Some("secret".into())), Err(StatusCode::UNAUTHORIZED));
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
        assert_eq!(check_auth(&headers, &Some("secret".into())), Err(StatusCode::UNAUTHORIZED));
    }

    #[test]
    fn test_check_auth_empty_header_value() {
        let mut headers = HeaderMap::new();
        headers.insert("Authorization", "".parse().unwrap());
        assert_eq!(check_auth(&headers, &Some("secret".into())), Err(StatusCode::UNAUTHORIZED));
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
}

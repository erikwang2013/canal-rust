use std::sync::Arc;
use axum::{Router, routing::{get, post}, Json, extract::{Path, State}};
use serde::Serialize;
use tokio::sync::RwLock;
use tracing::info;

/// Summary of a Canal instance exposed via the admin API.
#[derive(Clone, Serialize)]
pub struct InstanceSummary {
    pub name: String,
    pub destination: String,
    pub running: bool,
}

/// Shared application state for the admin API.
/// Holds raw String registrations; in production this would hold an
/// Arc<InstanceManager> from canal-instance and call its methods directly.
#[derive(Clone, Default)]
pub struct AdminState {
    pub instances: Arc<RwLock<std::collections::HashMap<String, InstanceSummary>>>,
}

/// Health check response.
#[derive(Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub uptime_seconds: u64,
}

/// Instance list response.
#[derive(Serialize)]
pub struct InstanceListResponse {
    pub instances: Vec<InstanceSummary>,
}

/// Generic status message.
#[derive(Serialize)]
pub struct StatusMessage {
    pub status: String,
    pub message: String,
}

/// Admin API server.
pub struct AdminServer {
    bind_addr: String,
    state: AdminState,
}

impl AdminServer {
    pub fn new(bind_addr: &str) -> Self {
        Self {
            bind_addr: bind_addr.to_string(),
            state: AdminState::default(),
        }
    }

    /// Register an instance for admin visibility.
    pub async fn register_instance(&self, name: &str, destination: &str) {
        let mut instances = self.state.instances.write().await;
        instances.insert(name.to_string(), InstanceSummary {
            name: name.to_string(),
            destination: destination.to_string(),
            running: false,
        });
    }

    /// Update an instance's running status.
    pub async fn update_instance_status(&self, name: &str, running: bool) {
        let mut instances = self.state.instances.write().await;
        if let Some(inst) = instances.get_mut(name) {
            inst.running = running;
        }
    }

    /// Start the admin HTTP server. Returns immediately.
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

async fn health_handler() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "UP".into(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_seconds: 0, // TODO: track actual uptime
    })
}

async fn list_instances(State(state): State<AdminState>) -> Json<InstanceListResponse> {
    let instances: Vec<InstanceSummary> = state.instances.read().await.values().cloned().collect();
    Json(InstanceListResponse { instances })
}

async fn start_instance(
    State(state): State<AdminState>,
    Path(name): Path<String>,
) -> Json<StatusMessage> {
    let mut instances = state.instances.write().await;
    if let Some(inst) = instances.get_mut(&name) {
        inst.running = true;
        Json(StatusMessage { status: "ok".into(), message: format!("Instance '{}' started", name) })
    } else {
        Json(StatusMessage { status: "not_found".into(), message: format!("Instance '{}' not found", name) })
    }
}

async fn stop_instance(
    State(state): State<AdminState>,
    Path(name): Path<String>,
) -> Json<StatusMessage> {
    let mut instances = state.instances.write().await;
    if let Some(inst) = instances.get_mut(&name) {
        inst.running = false;
        Json(StatusMessage { status: "ok".into(), message: format!("Instance '{}' stopped", name) })
    } else {
        Json(StatusMessage { status: "not_found".into(), message: format!("Instance '{}' not found", name) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_register_and_list_instances() {
        let server = AdminServer::new("127.0.0.1:0");
        server.register_instance("i1", "dest1").await;
        server.register_instance("i2", "dest2").await;

        let instances = server.state.instances.read().await;
        assert_eq!(instances.len(), 2);
        assert_eq!(instances.get("i1").unwrap().destination, "dest1");
    }

    #[tokio::test]
    async fn test_update_status() {
        let server = AdminServer::new("127.0.0.1:0");
        server.register_instance("i1", "dest1").await;
        server.update_instance_status("i1", true).await;

        {
            let instances = server.state.instances.read().await;
            assert!(instances.get("i1").unwrap().running);
        }

        server.update_instance_status("i1", false).await;
        {
            let instances = server.state.instances.read().await;
            assert!(!instances.get("i1").unwrap().running);
        }
    }

    #[tokio::test]
    async fn test_health_endpoint() {
        let server = AdminServer::new("127.0.0.1:0");
        let task = server.start().await.unwrap();
        task.abort();
    }
}

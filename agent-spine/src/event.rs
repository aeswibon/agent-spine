use axum::{
    Json, Router,
    extract::{Path, Query, State},
    response::sse::{Event as SseEvent, KeepAlive, Sse},
};
use chrono::{DateTime, Utc};
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{RwLock, broadcast};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub id: String,
    pub source: String,
    pub subject: String,
    pub payload: Value,
    pub timestamp: DateTime<Utc>,
    pub metadata: HashMap<String, String>,
}

impl Event {
    pub fn new(source: impl Into<String>, subject: impl Into<String>, payload: Value) -> Self {
        Self {
            id: Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)).to_string(),
            source: source.into(),
            subject: subject.into(),
            payload,
            timestamp: Utc::now(),
            metadata: HashMap::new(),
        }
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentInfo {
    pub name: String,
    pub version: String,
    pub capabilities: Vec<String>,
    pub last_seen: DateTime<Utc>,
    pub metadata: HashMap<String, String>,
}

impl AgentInfo {
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
            capabilities: Vec::new(),
            last_seen: Utc::now(),
            metadata: HashMap::new(),
        }
    }

    pub fn with_capability(mut self, cap: impl Into<String>) -> Self {
        self.capabilities.push(cap.into());
        self
    }
}

#[derive(Debug, Error)]
pub enum EventBusError {
    #[error("send failed: {0}")]
    Send(String),
    #[error("nats error: {0}")]
    Nats(String),
}

#[derive(Debug)]
pub struct EventSubscription {
    pub subject: String,
    pub rx: broadcast::Receiver<Event>,
}

#[async_trait::async_trait]
pub trait EventBus: Send + Sync {
    async fn publish(&self, event: Event) -> Result<(), EventBusError>;
    async fn subscribe(&self, subject: &str) -> broadcast::Receiver<Event>;
}

pub struct InMemoryEventBus {
    channels: Arc<RwLock<HashMap<String, broadcast::Sender<Event>>>>,
    capacity: usize,
}

impl InMemoryEventBus {
    pub fn new(capacity: usize) -> Self {
        Self {
            channels: Arc::new(RwLock::new(HashMap::new())),
            capacity,
        }
    }

    async fn ensure_channel(&self, subject: &str) -> broadcast::Sender<Event> {
        let mut channels = self.channels.write().await;
        channels
            .entry(subject.to_string())
            .or_insert_with(|| {
                let (tx, _) = broadcast::channel(self.capacity);
                tx
            })
            .clone()
    }
}

#[async_trait::async_trait]
impl EventBus for InMemoryEventBus {
    async fn publish(&self, event: Event) -> Result<(), EventBusError> {
        let tx = self.ensure_channel(&event.subject).await;
        tx.send(event)
            .map_err(|e| EventBusError::Send(e.to_string()))?;
        Ok(())
    }

    async fn subscribe(&self, subject: &str) -> broadcast::Receiver<Event> {
        let tx = self.ensure_channel(subject).await;
        tx.subscribe()
    }
}

pub struct AgentRegistry {
    agents: Arc<RwLock<HashMap<String, AgentInfo>>>,
}

impl AgentRegistry {
    pub fn new() -> Self {
        Self {
            agents: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn register(&self, info: AgentInfo) {
        self.agents.write().await.insert(info.name.clone(), info);
    }

    pub async fn unregister(&self, name: &str) {
        self.agents.write().await.remove(name);
    }

    pub async fn list(&self) -> Vec<AgentInfo> {
        self.agents.read().await.values().cloned().collect()
    }

    pub async fn get(&self, name: &str) -> Option<AgentInfo> {
        self.agents.read().await.get(name).cloned()
    }

    pub async fn heartbeat(&self, name: &str) {
        if let Some(agent) = self.agents.write().await.get_mut(name) {
            agent.last_seen = Utc::now();
        }
    }
}

impl Default for AgentRegistry {
    fn default() -> Self {
        Self::new()
    }
}

pub struct EventApi {
    pub bus: Arc<dyn EventBus>,
    pub registry: Arc<AgentRegistry>,
}

impl EventApi {
    pub fn new(bus: Arc<dyn EventBus>, registry: Arc<AgentRegistry>) -> Self {
        Self { bus, registry }
    }

    pub fn router(&self) -> Router {
        use axum::routing::{get, post};

        let state = Arc::new(self.clone());

        Router::new()
            .route("/api/v1/events", post(Self::publish_event))
            .route("/api/v1/events/subscribe", get(Self::subscribe_events))
            .route(
                "/api/v1/agents",
                post(Self::register_agent).get(Self::list_agents),
            )
            .route(
                "/api/v1/agents/{name}",
                get(Self::get_agent).post(Self::heartbeat_agent),
            )
            .with_state(state)
    }

    async fn publish_event(
        State(state): State<Arc<EventApi>>,
        Json(msg): Json<EventPublishRequest>,
    ) -> Result<Json<EventPublishResponse>, (axum::http::StatusCode, String)> {
        let mut event = Event::new(&msg.source, &msg.subject, msg.payload);
        for (k, v) in &msg.metadata {
            let _ = event.metadata.insert(k.clone(), v.clone());
        }
        state
            .bus
            .publish(event.clone())
            .await
            .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        Ok(Json(EventPublishResponse { event_id: event.id }))
    }

    async fn subscribe_events(
        State(state): State<Arc<EventApi>>,
        Query(params): Query<EventSubscribeParams>,
    ) -> Sse<impl Stream<Item = Result<SseEvent, Infallible>>> {
        let subject = params.subject.unwrap_or_else(|| ">".to_string());
        let mut rx = state.bus.subscribe(&subject).await;

        let stream = async_stream::stream! {
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        let json = serde_json::to_string(&event).unwrap_or_default();
                        yield Ok(SseEvent::default().data(json));
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        let warn = serde_json::json!({"warning": format!("lagged by {} events", n)});
                        yield Ok(SseEvent::default().data(warn.to_string()));
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        };

        Sse::new(stream).keep_alive(KeepAlive::default())
    }

    async fn register_agent(
        State(state): State<Arc<EventApi>>,
        Json(info): Json<AgentInfo>,
    ) -> Json<AgentInfo> {
        state.registry.register(info.clone()).await;
        Json(info)
    }

    async fn list_agents(State(state): State<Arc<EventApi>>) -> Json<Vec<AgentInfo>> {
        Json(state.registry.list().await)
    }

    async fn get_agent(
        State(state): State<Arc<EventApi>>,
        Path(name): Path<String>,
    ) -> Result<Json<AgentInfo>, (axum::http::StatusCode, String)> {
        state
            .registry
            .get(&name)
            .await
            .ok_or_else(|| {
                (
                    axum::http::StatusCode::NOT_FOUND,
                    format!("agent '{name}' not found"),
                )
            })
            .map(Json)
    }

    async fn heartbeat_agent(
        State(state): State<Arc<EventApi>>,
        Path(name): Path<String>,
    ) -> Result<Json<AgentInfo>, (axum::http::StatusCode, String)> {
        state.registry.heartbeat(&name).await;
        state
            .registry
            .get(&name)
            .await
            .ok_or_else(|| {
                (
                    axum::http::StatusCode::NOT_FOUND,
                    format!("agent '{name}' not found"),
                )
            })
            .map(Json)
    }
}

impl Clone for EventApi {
    fn clone(&self) -> Self {
        Self {
            bus: Arc::clone(&self.bus),
            registry: Arc::clone(&self.registry),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct EventPublishRequest {
    pub source: String,
    pub subject: String,
    pub payload: Value,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
pub struct EventPublishResponse {
    pub event_id: String,
}

#[derive(Debug, Deserialize)]
pub struct EventSubscribeParams {
    pub subject: Option<String>,
}

pub fn start_event_server(
    bus: Arc<dyn EventBus>,
    registry: Arc<AgentRegistry>,
    port: u16,
) -> tokio::task::JoinHandle<()> {
    let api = EventApi::new(bus, registry);
    let router = api.router();
    tokio::spawn(async move {
        let addr: std::net::SocketAddr = format!("0.0.0.0:{port}").parse().unwrap();
        tracing::info!("Event bus listening on http://{}", addr);
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        axum::serve(listener, router).await.unwrap();
    })
}

#[cfg(feature = "nats")]
pub struct NatsEventBus {
    client: async_nats::Client,
}

#[cfg(feature = "nats")]
impl NatsEventBus {
    pub async fn connect(url: &str) -> Result<Self, EventBusError> {
        let client = async_nats::connect(url)
            .await
            .map_err(|e| EventBusError::Nats(e.to_string()))?;
        Ok(Self { client })
    }
}

#[cfg(feature = "nats")]
#[async_trait::async_trait]
impl EventBus for NatsEventBus {
    async fn publish(&self, event: Event) -> Result<(), EventBusError> {
        let subject = event.subject.clone();
        let data = serde_json::to_vec(&event).map_err(|e| EventBusError::Nats(e.to_string()))?;
        self.client
            .publish(subject, data.into())
            .await
            .map_err(|e| EventBusError::Nats(e.to_string()))?;
        Ok(())
    }

    async fn subscribe(&self, subject: &str) -> broadcast::Receiver<Event> {
        let (tx, rx) = broadcast::channel(256);
        let sub = self.client.subscribe(subject.to_string()).await;

        tokio::spawn(async move {
            let mut sub = sub;
            while let Some(msg) = sub.next().await {
                if let Ok(event) = serde_json::from_slice::<Event>(&msg.payload) {
                    let _ = tx.send(event);
                }
            }
        });

        rx
    }
}

#[cfg(feature = "nats")]
use futures::StreamExt;

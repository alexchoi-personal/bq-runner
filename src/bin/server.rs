use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::{State, WebSocketUpgrade},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use clap::{Parser, ValueEnum};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::signal;
use tokio::sync::watch;
use tower::ServiceBuilder;
use tower_governor::{governor::GovernorConfigBuilder, GovernorLayer};
use tracing::{error, info, warn, Level};
use tracing_subscriber::EnvFilter;

use bq_runner::executor::ExecutorMode;
use bq_runner::rpc::{handle_websocket, process_message, RpcMethods};
use bq_runner::{AuthConfig, Config, LogFormat, SessionManager};

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
enum Backend {
    #[default]
    Mock,
    Bigquery,
}

impl From<Backend> for ExecutorMode {
    fn from(backend: Backend) -> Self {
        match backend {
            Backend::Mock => ExecutorMode::Mock,
            Backend::Bigquery => ExecutorMode::BigQuery,
        }
    }
}

#[derive(Debug, Clone)]
enum Transport {
    Stdio,
    WebSocket { port: u16 },
    Unix { path: PathBuf },
}

fn parse_transport(s: &str) -> Result<Transport, String> {
    if s == "stdio" {
        return Ok(Transport::Stdio);
    }

    if let Some(rest) = s.strip_prefix("ws://") {
        let port_str = rest
            .strip_prefix("localhost:")
            .or_else(|| rest.strip_prefix("0.0.0.0:"))
            .or_else(|| rest.strip_prefix("127.0.0.1:"))
            .ok_or_else(|| format!("Invalid ws URL: {}. Expected ws://localhost:<port>", s))?;

        let port_str = port_str.split('/').next().unwrap_or(port_str);

        let port: u16 = port_str
            .parse()
            .map_err(|_| format!("Invalid port in URL: {}", port_str))?;

        return Ok(Transport::WebSocket { port });
    }

    if let Some(path) = s.strip_prefix("unix://") {
        return Ok(Transport::Unix {
            path: PathBuf::from(path),
        });
    }

    if s.starts_with('/') || s.starts_with('.') {
        return Ok(Transport::Unix {
            path: PathBuf::from(s),
        });
    }

    Err(format!(
        "Invalid transport: {}. Use 'stdio', 'ws://localhost:<port>', or 'unix:///path/to/socket'",
        s
    ))
}

#[derive(Parser)]
#[command(name = "bq-runner-server")]
#[command(about = "BigQuery runner server with mock and real BigQuery backends")]
struct Args {
    #[arg(long, value_parser = parse_transport, default_value = "ws://localhost:3000", help = "Transport: stdio, ws://localhost:<port>, or unix:///path/to/socket")]
    transport: Transport,

    #[arg(
        long,
        value_enum,
        default_value = "mock",
        help = "Execution backend: mock (YachtSQL) or bigquery (real BigQuery)"
    )]
    backend: Backend,

    #[arg(long, help = "Path to configuration file (TOML)")]
    config: Option<PathBuf>,
}

#[derive(Clone)]
struct AppState {
    methods: Arc<RpcMethods>,
    metrics_handle: PrometheusHandle,
    auth: AuthConfig,
    audit_enabled: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Load configuration
    let config = Config::load(args.config.as_deref())?;

    // Initialize tracing based on config
    init_tracing(&config);

    let executor_mode: ExecutorMode = args.backend.into();
    let session_manager = Arc::new(SessionManager::with_full_config(
        executor_mode,
        config.security.clone(),
        config.session.clone(),
    ));
    let methods = Arc::new(RpcMethods::with_config(
        Arc::clone(&session_manager),
        config.logging.audit_enabled,
        config.rpc.clone(),
    ));

    // Create shutdown channel for cleanup task
    let (cleanup_shutdown_tx, mut cleanup_shutdown_rx) = watch::channel(false);

    // Start session cleanup background task with cancellation support
    let cleanup_interval = Duration::from_secs(config.session.cleanup_interval_secs);
    let cleanup_manager = Arc::clone(&session_manager);
    let cleanup_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(cleanup_interval);
        loop {
            tokio::select! {
                _ = cleanup_shutdown_rx.changed() => {
                    info!("Cleanup task shutting down");
                    break;
                }
                _ = interval.tick() => {
                    cleanup_manager.cleanup_expired_sessions();
                }
            }
        }
    });

    let result = match args.transport {
        Transport::Stdio => run_stdio_server(methods).await,
        Transport::WebSocket { port } => {
            log_backend(executor_mode);
            log_security_config(&config);
            let metrics_handle = PrometheusBuilder::new()
                .install_recorder()
                .map_err(|e| anyhow::anyhow!("Failed to install Prometheus recorder: {}", e))?;
            run_http_server(port, methods, metrics_handle, &config).await
        }
        Transport::Unix { path } => {
            log_backend(executor_mode);
            log_security_config(&config);
            run_unix_server(&path, methods, &config).await
        }
    };

    // Signal cleanup task to stop and wait for it to finish
    let _ = cleanup_shutdown_tx.send(true);
    let _ = cleanup_handle.await;

    result
}

fn init_tracing(config: &Config) {
    let filter = EnvFilter::builder()
        .with_default_directive(Level::INFO.into())
        .from_env_lossy();

    match config.logging.format {
        LogFormat::Json => {
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .json()
                .init();
        }
        LogFormat::Text => {
            tracing_subscriber::fmt().with_env_filter(filter).init();
        }
    }
}

fn log_backend(mode: ExecutorMode) {
    match mode {
        ExecutorMode::Mock => info!("Starting with mock backend (YachtSQL)"),
        ExecutorMode::BigQuery => info!("Starting with BigQuery backend"),
    }
}

fn log_security_config(config: &Config) {
    if config.security.allowed_paths.is_empty() {
        info!("Security: No allowed_paths configured - all file loading blocked");
    } else {
        info!(
            "Security: Path validation enabled for {} directories",
            config.security.allowed_paths.len()
        );
    }
    if config.logging.audit_enabled {
        info!("Audit logging: enabled");
    }
    if config.auth.enabled {
        info!("API key authentication: enabled");
    }
}

fn extract_bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
}

fn validate_auth(auth: &AuthConfig, headers: &HeaderMap, audit_enabled: bool) -> bool {
    if !auth.enabled {
        return true;
    }
    match extract_bearer_token(headers) {
        Some(token) => {
            let valid = auth.validate_key(token);
            if !valid && audit_enabled {
                warn!(
                    target: "audit",
                    event = "auth_failure",
                    reason = "invalid_api_key",
                    "Authentication failed: invalid API key"
                );
            }
            valid
        }
        None => {
            if audit_enabled {
                warn!(
                    target: "audit",
                    event = "auth_failure",
                    reason = "missing_authorization_header",
                    "Authentication failed: missing Authorization header"
                );
            }
            false
        }
    }
}

async fn unix_socket_authenticate(
    lines: &mut tokio::io::Lines<BufReader<tokio::net::unix::OwnedReadHalf>>,
    auth: &AuthConfig,
    audit_enabled: bool,
) -> bool {
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                if line.trim().is_empty() {
                    continue;
                }
                #[derive(serde::Deserialize)]
                struct AuthMessage {
                    api_key: String,
                }
                match serde_json::from_str::<AuthMessage>(&line) {
                    Ok(msg) => {
                        let valid = auth.validate_key(&msg.api_key);
                        if !valid && audit_enabled {
                            warn!(
                                target: "audit",
                                event = "auth_failure",
                                reason = "invalid_api_key",
                                transport = "unix",
                                "Authentication failed: invalid API key"
                            );
                        }
                        return valid;
                    }
                    Err(_) => {
                        if audit_enabled {
                            warn!(
                                target: "audit",
                                event = "auth_failure",
                                reason = "invalid_auth_message",
                                transport = "unix",
                                "Authentication failed: first message must be auth handshake"
                            );
                        }
                        return false;
                    }
                }
            }
            Ok(None) => return false,
            Err(_) => return false,
        }
    }
}

async fn run_stdio_server(methods: Arc<RpcMethods>) -> anyhow::Result<()> {
    let stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let mut reader = BufReader::new(stdin).lines();

    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

    // Spawn shutdown signal handler
    tokio::spawn(async move {
        shutdown_signal().await;
        let _ = shutdown_tx.send(true);
    });

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                info!("Stdio server shutting down");
                break;
            }
            result = reader.next_line() => {
                match result {
                    Ok(Some(line)) => {
                        if line.trim().is_empty() {
                            continue;
                        }

                        let response = process_message(&line, &methods).await;

                        match serde_json::to_string(&response) {
                            Ok(response_text) => {
                                if let Err(e) = stdout.write_all(response_text.as_bytes()).await {
                                    error!("Failed to write response: {}", e);
                                    break;
                                }
                                if let Err(e) = stdout.write_all(b"\n").await {
                                    error!("Failed to write newline: {}", e);
                                    break;
                                }
                                if let Err(e) = stdout.flush().await {
                                    error!("Failed to flush stdout: {}", e);
                                    break;
                                }
                            }
                            Err(e) => {
                                error!("Failed to serialize response: {}", e);
                            }
                        }
                    }
                    Ok(None) => break, // EOF
                    Err(e) => {
                        error!("Failed to read line: {}", e);
                        break;
                    }
                }
            }
        }
    }

    info!("Stdio server shutdown complete");
    Ok(())
}

async fn run_unix_server(
    path: &PathBuf,
    methods: Arc<RpcMethods>,
    config: &Config,
) -> anyhow::Result<()> {
    use governor::{Quota, RateLimiter};
    use std::num::NonZeroU32;

    if path.exists() {
        std::fs::remove_file(path)?;
    }

    let listener = UnixListener::bind(path)?;
    let rate_limit_per_second = config.rpc.rate_limit_per_second;
    let rate_limit_burst = config.rpc.rate_limit_burst;
    info!(
        "Listening on unix://{} (rate limit: {} req/s, burst: {})",
        path.display(),
        rate_limit_per_second,
        rate_limit_burst
    );

    let rps = match NonZeroU32::new(rate_limit_per_second as u32) {
        Some(v) => v,
        None => {
            tracing::warn!("rate_limit_per_second is 0, defaulting to 100");
            NonZeroU32::new(100).unwrap()
        }
    };
    let burst = match NonZeroU32::new(rate_limit_burst) {
        Some(v) => v,
        None => {
            tracing::warn!("rate_limit_burst is 0, defaulting to 200");
            NonZeroU32::new(200).unwrap()
        }
    };
    let quota = Quota::per_second(rps).allow_burst(burst);
    let rate_limiter = Arc::new(RateLimiter::direct(quota));

    let auth = config.auth.clone();
    let audit_enabled = config.logging.audit_enabled;

    let (shutdown_tx, _) = watch::channel(false);
    let active_connections = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    // Clone for the shutdown handler
    let shutdown_tx_clone = shutdown_tx.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        let _ = shutdown_tx_clone.send(true);
    });

    let mut shutdown_rx = shutdown_tx.subscribe();

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                info!("Unix server received shutdown signal");
                break;
            }
            result = listener.accept() => {
                match result {
                    Ok((stream, _)) => {
                        let methods = Arc::clone(&methods);
                        let mut conn_shutdown_rx = shutdown_tx.subscribe();
                        let conn_counter = Arc::clone(&active_connections);
                        conn_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                        let rate_limiter = Arc::clone(&rate_limiter);
                        let auth_config = auth.clone();
                        let audit = audit_enabled;

                        tokio::spawn(async move {
                            let (reader, mut writer) = stream.into_split();
                            let mut lines = BufReader::new(reader).lines();

                            if auth_config.enabled {
                                let auth_result = tokio::time::timeout(
                                    Duration::from_secs(30),
                                    unix_socket_authenticate(&mut lines, &auth_config, audit)
                                ).await;
                                match auth_result {
                                    Ok(true) => {
                                        let ok_response = serde_json::json!({
                                            "jsonrpc": "2.0",
                                            "result": {"authenticated": true},
                                            "id": null
                                        });
                                        if let Ok(text) = serde_json::to_string(&ok_response) {
                                            let _ = writer.write_all(text.as_bytes()).await;
                                            let _ = writer.write_all(b"\n").await;
                                            let _ = writer.flush().await;
                                        }
                                    }
                                    Ok(false) => {
                                        let error_response = serde_json::json!({
                                            "jsonrpc": "2.0",
                                            "error": {
                                                "code": -32001,
                                                "message": "Unauthorized"
                                            },
                                            "id": null
                                        });
                                        if let Ok(text) = serde_json::to_string(&error_response) {
                                            let _ = writer.write_all(text.as_bytes()).await;
                                            let _ = writer.write_all(b"\n").await;
                                            let _ = writer.flush().await;
                                        }
                                        conn_counter.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                                        return;
                                    }
                                    Err(_) => {
                                        if audit {
                                            warn!(
                                                target: "audit",
                                                event = "auth_failure",
                                                reason = "auth_timeout",
                                                "Authentication failed: timeout waiting for auth message"
                                            );
                                        }
                                        conn_counter.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                                        return;
                                    }
                                }
                            }

                            loop {
                                tokio::select! {
                                    _ = conn_shutdown_rx.changed() => {
                                        break;
                                    }
                                    result = lines.next_line() => {
                                        match result {
                                            Ok(Some(line)) => {
                                                if line.trim().is_empty() {
                                                    continue;
                                                }

                                                if rate_limiter.check().is_err() {
                                                    let error_response = serde_json::json!({
                                                        "jsonrpc": "2.0",
                                                        "error": {
                                                            "code": -32000,
                                                            "message": "Rate limit exceeded"
                                                        },
                                                        "id": null
                                                    });
                                                    if let Ok(text) = serde_json::to_string(&error_response) {
                                                        let _ = writer.write_all(text.as_bytes()).await;
                                                        let _ = writer.write_all(b"\n").await;
                                                        let _ = writer.flush().await;
                                                    }
                                                    continue;
                                                }

                                                let response = process_message(&line, &methods).await;

                                                match serde_json::to_string(&response) {
                                                    Ok(response_text) => {
                                                        if writer.write_all(response_text.as_bytes()).await.is_err() {
                                                            break;
                                                        }
                                                        if writer.write_all(b"\n").await.is_err() {
                                                            break;
                                                        }
                                                        if writer.flush().await.is_err() {
                                                            break;
                                                        }
                                                    }
                                                    Err(e) => {
                                                        error!("Failed to serialize response: {}", e);
                                                    }
                                                }
                                            }
                                            Ok(None) => break, // EOF
                                            Err(_) => break,
                                        }
                                    }
                                }
                            }

                            conn_counter.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                        });
                    }
                    Err(e) => {
                        error!("Failed to accept connection: {}", e);
                    }
                }
            }
        }
    }

    // Wait for active connections to finish (with timeout)
    let wait_start = std::time::Instant::now();
    let max_wait = Duration::from_secs(30);
    while active_connections.load(std::sync::atomic::Ordering::SeqCst) > 0 {
        if wait_start.elapsed() > max_wait {
            info!("Timeout waiting for connections, forcing shutdown");
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Clean up socket file
    if path.exists() {
        let _ = std::fs::remove_file(path);
    }

    info!("Unix server shutdown complete");
    Ok(())
}

async fn run_http_server(
    port: u16,
    methods: Arc<RpcMethods>,
    metrics_handle: PrometheusHandle,
    config: &Config,
) -> anyhow::Result<()> {
    let state = AppState {
        methods,
        metrics_handle,
        auth: config.auth.clone(),
        audit_enabled: config.logging.audit_enabled,
    };

    let rate_limit_per_second = config.rpc.rate_limit_per_second;
    let rate_limit_burst = config.rpc.rate_limit_burst;

    let governor_conf = Arc::new(
        GovernorConfigBuilder::default()
            .per_second(rate_limit_per_second)
            .burst_size(rate_limit_burst)
            .finish()
            .ok_or_else(|| anyhow::anyhow!("Failed to build governor config"))?,
    );
    let governor_limiter = governor_conf.limiter().clone();

    let app = Router::new()
        .route("/", get(ws_handler))
        .route("/health", get(health_handler))
        .route("/ready", get(readiness_handler))
        .route("/live", get(liveness_handler))
        .route("/metrics", get(metrics_handler))
        .layer(ServiceBuilder::new().layer(GovernorLayer {
            config: Arc::clone(&governor_conf),
        }))
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    info!(
        "Listening on ws://{} (rate limit: {} req/s, burst: {})",
        addr, rate_limit_per_second, rate_limit_burst
    );

    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
            governor_limiter.retain_recent();
        }
    });

    let listener = tokio::net::TcpListener::bind(addr).await?;

    // Graceful shutdown on SIGTERM/SIGINT
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("Server shutdown complete");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = signal::ctrl_c().await {
            error!("Failed to install Ctrl+C handler: {}", e);
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(e) => {
                error!("Failed to install SIGTERM handler: {}", e);
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!("Received Ctrl+C, initiating graceful shutdown");
        }
        _ = terminate => {
            info!("Received SIGTERM, initiating graceful shutdown");
        }
    }
}

// Maximum WebSocket message size: 16MB
const MAX_WS_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

async fn ws_handler(
    headers: HeaderMap,
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> Response {
    if !validate_auth(&state.auth, &headers, state.audit_enabled) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "Unauthorized"})),
        )
            .into_response();
    }
    ws.max_message_size(MAX_WS_MESSAGE_SIZE)
        .on_upgrade(move |socket| handle_websocket(socket, state.methods))
}

async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    let session_count = state.methods.session_manager().session_count();
    let uptime = state.methods.session_manager().uptime_seconds();
    Json(json!({
        "status": "ok",
        "session_count": session_count,
        "uptime_seconds": uptime
    }))
}

async fn readiness_handler(State(state): State<AppState>) -> impl IntoResponse {
    match state
        .methods
        .session_manager()
        .check_executor_health()
        .await
    {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({
                "ready": true,
                "status": "ready",
                "checks": [{
                    "name": "executor",
                    "status": "pass"
                }]
            })),
        ),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "ready": false,
                "status": "not_ready",
                "checks": [{
                    "name": "executor",
                    "status": "fail",
                    "message": e.to_string()
                }]
            })),
        ),
    }
}

async fn liveness_handler(State(state): State<AppState>) -> impl IntoResponse {
    Json(json!({
        "alive": true,
        "uptime_seconds": state.methods.session_manager().uptime_seconds()
    }))
}

async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    state.metrics_handle.render()
}

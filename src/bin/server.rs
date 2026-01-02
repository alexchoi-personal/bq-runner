use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::{
    extract::{State, WebSocketUpgrade},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use clap::{Parser, ValueEnum};
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tracing::{error, info, Level};
use tracing_subscriber::EnvFilter;

use bq_runner::executor::ExecutorMode;
use bq_runner::rpc::{handle_websocket, process_message, RpcMethods};
use bq_runner::SessionManager;

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
}

#[derive(Clone)]
struct AppState {
    methods: Arc<RpcMethods>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let executor_mode: ExecutorMode = args.backend.into();
    let session_manager = Arc::new(SessionManager::with_mode(executor_mode));
    let methods = Arc::new(RpcMethods::new(session_manager));

    match args.transport {
        Transport::Stdio => run_stdio_server(methods).await,
        Transport::WebSocket { port } => {
            init_tracing();
            log_backend(executor_mode);
            run_http_server(port, methods).await
        }
        Transport::Unix { path } => {
            init_tracing();
            log_backend(executor_mode);
            run_unix_server(&path, methods).await
        }
    }
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(Level::INFO.into())
                .from_env_lossy(),
        )
        .init();
}

fn log_backend(mode: ExecutorMode) {
    match mode {
        ExecutorMode::Mock => info!("Starting with mock backend (YachtSQL)"),
        ExecutorMode::BigQuery => info!("Starting with BigQuery backend"),
    }
}

async fn run_stdio_server(methods: Arc<RpcMethods>) -> anyhow::Result<()> {
    let stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let mut reader = BufReader::new(stdin).lines();

    while let Ok(Some(line)) = reader.next_line().await {
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

    Ok(())
}

async fn run_unix_server(path: &PathBuf, methods: Arc<RpcMethods>) -> anyhow::Result<()> {
    if path.exists() {
        std::fs::remove_file(path)?;
    }

    let listener = UnixListener::bind(path)?;
    info!("Listening on unix://{}", path.display());

    loop {
        let (stream, _) = listener.accept().await?;
        let methods = Arc::clone(&methods);

        tokio::spawn(async move {
            let (reader, mut writer) = stream.into_split();
            let mut lines = BufReader::new(reader).lines();

            while let Ok(Some(line)) = lines.next_line().await {
                if line.trim().is_empty() {
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
        });
    }
}

async fn run_http_server(port: u16, methods: Arc<RpcMethods>) -> anyhow::Result<()> {
    let state = AppState { methods };

    let app = Router::new()
        .route("/", get(ws_handler))
        .route("/health", get(health_handler))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("Listening on ws://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.max_message_size(usize::MAX)
        .on_upgrade(move |socket| handle_websocket(socket, state.methods))
}

async fn health_handler() -> impl IntoResponse {
    Json(json!({"status": "ok", "message": "pong"}))
}

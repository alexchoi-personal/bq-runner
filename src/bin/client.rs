use std::sync::atomic::{AtomicU64, Ordering};

use clap::{Parser, Subcommand};
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio_tungstenite::{connect_async, tungstenite::Message};

static REQUEST_ID: AtomicU64 = AtomicU64::new(1);

fn next_id() -> u64 {
    REQUEST_ID.fetch_add(1, Ordering::SeqCst)
}

fn is_unix_socket(server: &str) -> Option<&str> {
    if let Some(path) = server.strip_prefix("unix://") {
        return Some(path);
    }
    if server.starts_with('/') || server.starts_with('.') {
        return Some(server);
    }
    None
}

#[derive(Parser)]
#[command(name = "bq-runner-client")]
#[command(about = "BigQuery runner client")]
struct Args {
    #[arg(long, default_value = "ws://localhost:3000")]
    server: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Ping,
    CreateSession,
    DestroySession {
        #[arg(long)]
        session_id: String,
    },
    Query {
        #[arg(long)]
        session_id: String,
        #[arg(long)]
        sql: String,
    },
    CreateTable {
        #[arg(long)]
        session_id: String,
        #[arg(long)]
        table_name: String,
        #[arg(long)]
        schema: String,
    },
    Insert {
        #[arg(long)]
        session_id: String,
        #[arg(long)]
        table_name: String,
        #[arg(long)]
        rows: String,
    },
    ListTables {
        #[arg(long)]
        session_id: String,
    },
    DescribeTable {
        #[arg(long)]
        session_id: String,
        #[arg(long)]
        table_name: String,
    },
    RegisterDag {
        #[arg(long)]
        session_id: String,
        #[arg(long)]
        tables: String,
    },
    RunDag {
        #[arg(long)]
        session_id: String,
        #[arg(long)]
        table_names: Option<String>,
    },
    GetDag {
        #[arg(long)]
        session_id: String,
    },
    ClearDag {
        #[arg(long)]
        session_id: String,
    },
    LoadDagFromDirectory {
        #[arg(long)]
        session_id: String,
        #[arg(long)]
        root_path: String,
    },
    Call {
        #[arg(long)]
        method: String,
        #[arg(long, default_value = "{}")]
        params: String,
    },
}

async fn send_request(server: &str, method: &str, params: Value) -> anyhow::Result<Value> {
    if let Some(path) = is_unix_socket(server) {
        return send_request_unix(path, method, params).await;
    }
    send_request_ws(server, method, params).await
}

async fn send_request_unix(path: &str, method: &str, params: Value) -> anyhow::Result<Value> {
    let stream = UnixStream::connect(path).await?;
    let (reader, mut writer) = stream.into_split();

    let request = json!({
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
        "id": next_id()
    });

    writer.write_all(request.to_string().as_bytes()).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;

    let mut lines = BufReader::new(reader).lines();
    if let Some(line) = lines.next_line().await? {
        let response: Value = serde_json::from_str(&line)?;
        return Ok(response);
    }

    anyhow::bail!("No response received")
}

async fn send_request_ws(server: &str, method: &str, params: Value) -> anyhow::Result<Value> {
    let (ws_stream, _) = connect_async(server).await?;
    let (mut write, mut read) = ws_stream.split();

    let request = json!({
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
        "id": next_id()
    });

    write.send(Message::Text(request.to_string())).await?;

    if let Some(msg) = read.next().await {
        match msg? {
            Message::Text(text) => {
                let response: Value = serde_json::from_str(&text)?;
                return Ok(response);
            }
            _ => anyhow::bail!("Unexpected message type"),
        }
    }

    anyhow::bail!("No response received")
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let (method, params): (String, Value) = match args.command {
        Command::Ping => ("bq.ping".into(), json!({})),
        Command::CreateSession => ("bq.createSession".into(), json!({})),
        Command::DestroySession { session_id } => {
            ("bq.destroySession".into(), json!({"sessionId": session_id}))
        }
        Command::Query { session_id, sql } => (
            "bq.query".into(),
            json!({"sessionId": session_id, "sql": sql}),
        ),
        Command::CreateTable {
            session_id,
            table_name,
            schema,
        } => {
            let schema: Value = serde_json::from_str(&schema)?;
            (
                "bq.createTable".into(),
                json!({"sessionId": session_id, "tableName": table_name, "schema": schema}),
            )
        }
        Command::Insert {
            session_id,
            table_name,
            rows,
        } => {
            let rows: Value = serde_json::from_str(&rows)?;
            (
                "bq.insert".into(),
                json!({"sessionId": session_id, "tableName": table_name, "rows": rows}),
            )
        }
        Command::ListTables { session_id } => {
            ("bq.listTables".into(), json!({"sessionId": session_id}))
        }
        Command::DescribeTable {
            session_id,
            table_name,
        } => (
            "bq.describeTable".into(),
            json!({"sessionId": session_id, "tableName": table_name}),
        ),
        Command::RegisterDag { session_id, tables } => {
            let tables: Value = serde_json::from_str(&tables)?;
            (
                "bq.registerDag".into(),
                json!({"sessionId": session_id, "tables": tables}),
            )
        }
        Command::RunDag {
            session_id,
            table_names,
        } => {
            let mut params = json!({"sessionId": session_id});
            if let Some(names) = table_names {
                let names: Value = serde_json::from_str(&names)?;
                params["tableNames"] = names;
            }
            ("bq.runDag".into(), params)
        }
        Command::GetDag { session_id } => ("bq.getDag".into(), json!({"sessionId": session_id})),
        Command::ClearDag { session_id } => {
            ("bq.clearDag".into(), json!({"sessionId": session_id}))
        }
        Command::LoadDagFromDirectory {
            session_id,
            root_path,
        } => (
            "bq.loadDagFromDirectory".into(),
            json!({"sessionId": session_id, "rootPath": root_path}),
        ),
        Command::Call { method, params } => {
            let params: Value = serde_json::from_str(&params)?;
            (method, params)
        }
    };
    let response = send_request(&args.server, &method, params).await?;

    if let Some(error) = response.get("error") {
        eprintln!("Error: {}", serde_json::to_string_pretty(error)?);
        std::process::exit(1);
    }

    if let Some(result) = response.get("result") {
        println!("{}", serde_json::to_string_pretty(result)?);
    }

    Ok(())
}

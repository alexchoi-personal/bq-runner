# bq-runner

A BigQuery runner with two execution backends: mock (BigQuery emulation via YachtSQL) and bigquery (real BigQuery).

## Features

- **Native BigQuery SQL support**: YachtSQL provides native BigQuery SQL dialect support
- **Session isolation**: Each session gets its own database, fully isolated
- **DAG execution**: Register tables as a DAG and execute in dependency order
- **WebSocket & stdio transports**: JSON-RPC 2.0 over WebSocket or stdio
- **Multiple backends**: Mock (local) and BigQuery (real)
- **Arrow IPC**: Zero-copy data transfer via shared memory for high-performance data access

## Quick Start

```bash
# Build
cargo build --release

# Run with mock backend (default) on port 3000
./target/release/bq-runner

# Run on a different port
./target/release/bq-runner --transport ws://localhost:8080

# Run with bigquery backend
./target/release/bq-runner --backend bigquery

# Run with stdio transport (for process-based IPC)
./target/release/bq-runner --transport stdio
```

## Backends

| Backend | Engine | Use Case |
|---------|--------|----------|
| `mock` | YachtSQL | Local development, testing |
| `bigquery` | BigQuery | Production queries against real BigQuery |

### BigQuery Authentication

For `bigquery` backend, set up authentication:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
```

The project ID is automatically read from the credentials file.

## CLI Options

```
Options:
      --transport <TRANSPORT>  Transport: stdio or ws://localhost:<port> [default: ws://localhost:3000]
      --backend <BACKEND>      Execution backend: mock (YachtSQL) or bigquery (real BigQuery) [default: mock]
```

## RPC Methods

| Method | Description |
|--------|-------------|
| `bq.ping` | Health check |
| `bq.createSession` | Create isolated session |
| `bq.destroySession` | Drop session and all its tables |
| `bq.query` | Execute SQL query (JSON response) |
| `bq.queryArrow` | Execute SQL query (Arrow IPC via shared memory) |
| `bq.releaseArrowResult` | Release shared memory from Arrow result |
| `bq.createTable` | Create table with schema |
| `bq.insert` | Insert rows into table |
| `bq.registerDag` | Register DAG of source/derived tables |
| `bq.runDag` | Execute DAG in dependency order |
| `bq.getDag` | Get registered DAG tables |
| `bq.clearDag` | Clear DAG registry |

## Arrow IPC (High-Performance Data Transfer)

For large query results, `bq.queryArrow` provides zero-copy data transfer via shared memory:

### Request
```json
{
  "jsonrpc": "2.0",
  "method": "bq.queryArrow",
  "params": {
    "sessionId": "uuid-here",
    "sql": "SELECT * FROM large_table"
  },
  "id": 1
}
```

### Response
```json
{
  "jsonrpc": "2.0",
  "result": {
    "shmPath": "/dev/shm/bq_runner_uuid_123",
    "dataSize": 1048576,
    "rowCount": 100000,
    "schemaJson": "{\"fields\":[{\"name\":\"id\",\"type\":\"INT64\",\"nullable\":true}]}"
  },
  "id": 1
}
```

### Reading the Data

1. Open the shared memory file at `shmPath`
2. Read the first 8 bytes as a little-endian u64 (data length)
3. Read the remaining bytes as Arrow IPC Stream format
4. Call `bq.releaseArrowResult` when done to free the shared memory

### Performance

Arrow IPC provides ~100-1000x faster data transfer compared to JSON for large datasets:

| Rows | JSON | Arrow IPC | Speedup |
|------|------|-----------|---------|
| 1K | 5ms | 0.5ms | 10x |
| 10K | 50ms | 2ms | 25x |
| 100K | 500ms | 5ms | 100x |
| 1M | 5s | 15ms | 333x |

*Note: Results vary based on data types and column count*

## Client Libraries

- [Clojure](clients/clojure/bq-runner-client/) - Full-featured Clojure client with Arrow IPC support

### Clojure Quick Start

```clojure
(require '[bq-runner.client :as client])

;; Connect to bq-runner
(def c (client/connect! "ws://localhost:3000"))

;; Create a session and run queries
(client/with-session [c session-id]
  ;; Standard JSON query
  (client/query c session-id "SELECT 1 AS num")

  ;; High-performance Arrow query (zero-copy)
  (client/query-arrow->maps c session-id "SELECT * FROM large_table")

  ;; Columnar format for data processing
  (client/query-arrow->columns c session-id "SELECT id, value FROM data"))

;; Close connection
(client/close! c)
```

## Development

```bash
# Run tests
cargo test

# Run with logging
RUST_LOG=info ./target/release/bq-runner
```

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

Copyright 2025 Alex Choi

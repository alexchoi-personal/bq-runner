# BigQuery SDK Mockability Spike Report

## Investigation Summary

This spike investigates how to test the BigQuery executor in `src/executor/bigquery.rs`.

## Current State

The `BigQueryExecutor` uses the `google-cloud-bigquery` crate (version 0.15) with:
- `Client` - the main BigQuery client
- `ClientConfig` - configuration builder with authentication

Current initialization pattern:
```rust
let (config, project_id) = ClientConfig::new_with_auth().await?;
let client = Client::new(config).await?;
```

## Findings

### 1. SDK Supports Custom Endpoint and HTTP Client

The `google-cloud-bigquery` crate's `ClientConfig` provides excellent mockability hooks:

| Method | Purpose |
|--------|---------|
| `with_endpoint(url)` | Override the BigQuery API endpoint |
| `with_http_client(client)` | Inject a custom `reqwest_middleware::ClientWithMiddleware` |
| `new_with_emulator(grpc_host, http_addr)` | Constructor specifically for emulator testing |

### 2. BigQuery Emulator Exists

The [goccy/bigquery-emulator](https://github.com/goccy/bigquery-emulator) is an open-source Go implementation:
- Docker image: `ghcr.io/goccy/bigquery-emulator:latest`
- Supports most BigQuery APIs (except IAM)
- Uses SQLite for storage (memory or file-backed)
- Supports BigQuery Storage API with Avro/Arrow formats

Run command:
```bash
docker run -it -p 9050:9050 ghcr.io/goccy/bigquery-emulator:latest --project=test
```

### 3. Wiremock is Viable for Unit Tests

[wiremock-rs](https://github.com/LukeMathWalker/wiremock-rs) can mock HTTP responses at the transport layer:
- Starts a local mock HTTP server on a random port
- Request matchers verify correct API calls
- Expectations validate call counts
- Pool-based design for parallel test execution

## Approach Comparison

| Approach | Pros | Cons | Effort |
|----------|------|------|--------|
| **A: Trait Extraction** | Full control, fast tests, no external deps | Significant refactoring, duplication of mock impl | High (3-5 days) |
| **B: Wiremock** | Tests real HTTP layer, minimal code changes | Need to mock BigQuery JSON responses, brittle | Medium (2-3 days) |
| **C: BigQuery Emulator** | Tests real SDK behavior, high fidelity | Requires Docker, slower tests, CI complexity | Medium (2-3 days) |
| **D: Integration Tests Only** | Simplest, tests real BigQuery | Slow, requires credentials, non-deterministic | Low (1 day) |

## Recommended Approach: B + C (Hybrid)

**Primary: Wiremock for unit tests**
- Modify `BigQueryExecutor::new()` to accept optional `ClientConfig`
- In tests, use `ClientConfig::new_with_emulator()` or custom endpoint
- Mock specific API responses with wiremock

**Secondary: BigQuery Emulator for integration tests**
- Use Docker-based emulator in CI for high-fidelity tests
- Run as separate test target (e.g., `cargo nextest run --features=integration`)

### Implementation Sketch

```rust
impl BigQueryExecutor {
    pub async fn new() -> Result<Self> {
        Self::with_config(None, None).await
    }

    pub async fn with_config(
        config: Option<ClientConfig>,
        project_id: Option<String>,
    ) -> Result<Self> {
        let (config, detected_project) = match config {
            Some(c) => (c, project_id),
            None => {
                let (c, p) = ClientConfig::new_with_auth().await?;
                (c, p)
            }
        };
        // ... rest of initialization
    }
}

#[cfg(test)]
mod tests {
    use wiremock::{MockServer, Mock, ResponseTemplate};
    use wiremock::matchers::method;

    #[tokio::test]
    async fn test_execute_query() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(/* BQ response */))
            .mount(&mock_server)
            .await;

        let config = ClientConfig::new_with_emulator("", &mock_server.uri());
        let executor = BigQueryExecutor::with_config(Some(config), Some("test".into())).await?;

        // Test queries...
    }
}
```

## Effort Estimate

| Task | Estimate |
|------|----------|
| Add `with_config` constructor | 0.5 day |
| Create wiremock test helpers | 1 day |
| Write unit tests for query/statement/load_parquet | 1.5 days |
| Set up emulator in CI | 0.5 day |
| Write emulator-based integration tests | 0.5 day |
| **Total** | **4 days** |

## Dependencies to Add

```toml
[dev-dependencies]
wiremock = "0.6"
```

## Conclusion

The `google-cloud-bigquery` crate is well-designed for testing. The `with_endpoint()` and `new_with_emulator()` methods provide clean injection points. A hybrid approach using wiremock for fast unit tests and the BigQuery emulator for integration tests offers the best balance of coverage and speed.

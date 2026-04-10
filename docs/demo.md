# Wruntime: Design Deep Dive

A distributed runtime that networks WASM modules via transparent HTTP interception. This document walks through the most interesting design decisions and powerful capabilities.

---

## 1. Transparent HTTP Interception

WASM modules make plain HTTP calls — `http://ecommerce.inventory/items` — and the runtime transparently intercepts, rewrites, and routes them through the proxy. No special SDK calls, no service discovery client.

The `WasiHttpHooks` trait intercepts every outbound request at the WASI layer:

```rust
// wr-engine/src/state.rs

impl WasiHttpHooks for ModuleHttpHooks {
    fn send_request(
        &mut self,
        mut request: hyper::Request<HyperOutgoingBody>,
        config: OutgoingRequestConfig,
    ) -> HttpResult<HostFutureIncomingResponse> {
        let original_uri = request.uri().to_string();

        // Tag the request with routing metadata
        request.headers_mut().insert(
            HeaderName::from_static("x-wr-destination"),
            HeaderValue::from_str(&original_uri)?,
        );
        request.headers_mut().insert(
            HeaderName::from_static("x-wr-source"),
            HeaderValue::from_str(&self.module_name)?,
        );

        // Inject distributed trace context so downstream services
        // join this trace instead of starting a new one
        {
            let _guard = outbound_span.enter();
            wr_common::telemetry::inject_context(request.headers_mut());
        }

        // Rewrite URI to point at the local proxy — preserve path + query
        let path_and_query = request.uri().path_and_query()
            .map(|pq| pq.as_str()).unwrap_or("/");
        let new_uri: hyper::Uri =
            format!("{scheme}://{authority}{path_and_query}").parse()?;
        *request.uri_mut() = new_uri;

        // Send through the HTTP/2 client pool
        let client = self.http_pool.get().clone();
        // ...
    }
}
```

The module code has zero awareness of the proxy, routing table, or mTLS — it just makes HTTP calls. Observability (traces, metrics) is injected automatically.

---

## 2. Pre-Indexed Routing with Semver

The proxy rebuilds its routing index every ~2s from the manager. Instead of linearly scanning all rules and re-parsing semver on every request, rules are grouped and sorted at sync time:

```rust
// wr-proxy/src/indexed_routing.rs

pub fn from_proto(table: &RoutingTable) -> Self {
    let mut by_module: HashMap<(Arc<str>, Arc<str>), Vec<ParsedRule>> = HashMap::new();

    for rule in &table.rules {
        if !rule.healthy { continue; }  // Drop unhealthy at sync time
        let key = (
            Arc::from(rule.destination_namespace.as_str()),
            Arc::from(rule.destination_module.as_str()),
        );
        let parsed_version = semver::Version::parse(&rule.destination_version).ok();
        by_module.entry(key).or_default().push(ParsedRule {
            rule: rule.clone(),
            parsed_version,
        });
    }

    // Best version at the front — O(1) lookup at request time
    for rules in by_module.values_mut() {
        rules.sort_by(|a, b| match (&b.parsed_version, &a.parsed_version) {
            (Some(bv), Some(av)) => bv.cmp(av),
            _ => b.rule.destination_version.cmp(&a.rule.destination_version),
        });
    }
    // ...
}
```

This turns per-request routing from **O(n) scan + repeated semver parsing** into **O(1) HashMap lookup** against pre-parsed, pre-sorted rules.

---

## 3. WIT-Defined Host Interfaces

The host-guest contract is defined in WIT (WebAssembly Interface Types). Modules get typed access to Postgres, blob storage, LLM inference, and distributed tracing — all through clean interface definitions:

```wit
// wit/db.wit
package wruntime:db@0.4.0;

interface database {
    variant pg-value {
        null, boolean(bool), int4(s32), int8(s64), text(string),
        timestamptz(s64), uuid(tuple<u64, u64>), jsonb(string),
        // ... 20+ typed variants including arrays
    }

    query: func(sql: string, params: list<pg-value>) -> result<list<row>, db-error>;
    execute: func(sql: string, params: list<pg-value>) -> result<u64, db-error>;

    resource transaction {
        query: func(sql: string, params: list<pg-value>) -> result<list<row>, db-error>;
        execute: func(sql: string, params: list<pg-value>) -> result<u64, db-error>;
        query-stream: func(sql: string, params: list<pg-value>) -> result<row-cursor, db-error>;
        commit: func() -> result<_, db-error>;
        rollback: func() -> result<_, db-error>;
    }

    resource row-cursor {
        next-batch: func(max: u32) -> result<list<row>, db-error>;
    }
}
```

```wit
// wit/llm.wit
package wruntime:llm@0.1.0;

interface inference {
    record tool-def {
        name: string,
        description: string,
        input-schema: string,   // JSON Schema
    }

    variant completion {
        text(string),
        tool-calls(list<tool-use>),
    }

    complete: func(req: completion-request) -> result<completion-response, llm-error>;

    resource completion-stream {
        next: func() -> result<option<string>, llm-error>;
        usage: func() -> option<token-usage>;
    }

    complete-stream: func(req: completion-request) -> result<completion-stream, llm-error>;
}
```

The host implements these interfaces with real async Rust — `bindgen!` generates the trait with `imports: { default: async }`:

```rust
// wr-engine/src/db.rs

wasmtime::component::bindgen!({
    path:               "../wit/db.wit",
    world:              "db-access",
    with: {
        "wruntime:db/database.transaction": TxState,
        "wruntime:db/database.row-cursor":  CursorState,
    },
    imports: { default: async },
});
```

---

## 4. Per-Namespace Database Isolation

Each namespace gets its own Postgres role and schema. When a module acquires a connection, the host sets the search path and applies safety limits in a single round-trip:

```rust
// wr-engine/src/db.rs

async fn prepare_connection(
    client: &deadpool_postgres::Object,
    schema: &Option<Arc<str>>,
    timeouts: &DbTimeouts,
) -> Result<(), DbError> {
    use std::fmt::Write;
    let mut sql = String::new();
    if let Some(s) = schema {
        write!(sql, "SET search_path = \"{s}\"; ").unwrap();
    }
    write!(
        sql,
        "SET statement_timeout = '{}s'; \
         SET idle_in_transaction_session_timeout = '{}s';",
        timeouts.statement_timeout_secs,
        timeouts.idle_in_transaction_timeout_secs
    ).unwrap();
    client.batch_execute(&sql).await
        .map_err(|e| DbError::Connection(e.to_string()))?;
    Ok(())
}
```

Guest roles are never granted access to the `wr_system` schema — WASM modules cannot read manager system tables. This provides strong tenant isolation without container overhead.

---

## 5. Postgres-Backed Job Queue with LISTEN/NOTIFY

Background jobs use PostgreSQL as the queue. Jobs are claimed atomically with `FOR UPDATE SKIP LOCKED`, and workers are woken via `LISTEN` instead of polling:

```rust
// wr-engine/src/worker.rs

pub async fn claim_job(
    pool: &Pool, namespace: &str, name: &str, engine_id: &str,
) -> anyhow::Result<Option<ClaimedJob>> {
    let client = pool.get().await?;
    let row = client.query_opt(
        "UPDATE wr__jobs.jobs SET status = 'running', claimed_at = now(), \
         claimed_by = $3, attempt = attempt + 1, updated_at = now() \
         WHERE job_id = ( \
           SELECT job_id FROM wr__jobs.jobs \
           WHERE worker_namespace = $1 AND worker_name = $2 \
             AND status = 'pending' \
           ORDER BY created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED \
         ) RETURNING job_id, job_type, payload",
        &[&namespace, &name, &engine_id],
    ).await?;
    Ok(row.map(|r| ClaimedJob { /* ... */ }))
}
```

The LISTEN connection manually polls the connection for notifications, waking worker loops via `tokio::sync::Notify`:

```rust
// wr-engine/src/worker.rs

async fn listen_loop(db_url: &str, channel: &str, notify: &Arc<Notify>) -> anyhow::Result<()> {
    let (client, mut connection) = tokio_postgres::connect(db_url, NoTls).await?;

    let notify = Arc::clone(notify);
    tokio::spawn(async move {
        loop {
            match std::future::poll_fn(|cx| connection.poll_message(cx)).await {
                Some(Ok(AsyncMessage::Notification(_))) => {
                    notify.notify_waiters();  // Wake all worker tasks
                }
                Some(Err(e)) => { warn!(error = %e, "LISTEN error"); break; }
                None => break,
            }
        }
    });

    client.batch_execute(&format!("LISTEN \"{channel}\"")).await?;
    // ...
}
```

Jobs are dispatched to WASM modules as HTTP requests — `POST /{job_type}` with the payload body. Failed jobs auto-retry up to `max_attempts`.

---

## 6. Circuit Breaker per Engine

The proxy tracks consecutive failures per engine address. After a threshold, the circuit opens and requests get an immediate `503` instead of timing out against a dead backend:

```rust
// wr-proxy/src/circuit_breaker.rs

pub struct CircuitBreakerRegistry {
    inner: Arc<Mutex<HashMap<Arc<str>, EngineBreaker>>>,
    config: CircuitBreakerConfig,
}

impl CircuitBreakerRegistry {
    pub fn get_or_create(&self, addr: &str) -> EngineBreaker {
        let mut map = self.inner.lock().unwrap();
        map.entry(Arc::from(addr))
            .or_insert_with(|| self.build_breaker())
            .clone()
    }

    /// Removes breakers for engines no longer in the routing table
    pub fn evict_missing(&self, active: &HashSet<&str>) {
        self.inner.lock().unwrap()
            .retain(|k, _| active.contains(&**k));
    }

    fn build_breaker(&self) -> EngineBreaker {
        Config::new()
            .failure_policy(failure_policy::consecutive_failures(
                self.config.failure_threshold,
                backoff::constant(Duration::from_secs(self.config.open_duration_secs)),
            ))
            .build()
    }
}
```

The `evict_missing` call runs on every routing table sync, preventing memory leaks from departed engines.

---

## 7. Public Ingress with Header Spoofing Prevention

External traffic enters through the ingress layer, which maps public paths to internal modules. All `x-wr-*` headers are stripped to prevent callers from spoofing internal routing identity:

```rust
// wr-proxy/src/layers/ingress.rs

fn call(&mut self, req: Request<ProxyBody>) -> Self::Future {
    // ...
    Box::pin(async move {
        let (mut parts, body) = req.into_parts();

        // Strip ALL internal routing headers from external requests
        for name in &[
            "x-wr-destination", "x-wr-source", "x-wr-source-ns",
            "x-wr-module", "x-wr-namespace", "x-wr-version", "x-wr-via-proxy",
        ] {
            parts.headers.remove(*name);
        }

        // Match against configured public routes (trie-based)
        let matched = router.at(&path)?;
        let route = matched.value.iter()
            .find(|&&idx| routes[idx].methods.contains(&method))?;

        // Set internal routing headers from the route config
        let dest = format!("http://{namespace}.{module}/");
        parts.headers.insert("x-wr-destination", dest.parse()?);
        parts.headers.insert("x-wr-source", "external".parse()?);
        inner.call(Request::from_parts(parts, body)).await
    })
}
```

---

## 8. mTLS Client Pool with Connection Spreading

All inter-service traffic uses mutual TLS. A pool of HTTP/2 clients spreads requests across multiple TCP connections to avoid frame contention and head-of-line blocking:

```rust
// wr-common/src/tls.rs

pub struct HttpsClientPool<B> {
    clients: Arc<Vec<Client<HttpsConnector<HttpConnector>, B>>>,
    next: Arc<AtomicUsize>,
}

impl<B> HttpsClientPool<B> {
    pub fn new(size: usize, tls_config: ClientConfig) -> Self {
        let clients: Vec<_> = (0..size)
            .map(|_| {
                let connector = HttpsConnectorBuilder::new()
                    .with_tls_config(tls_config.clone())
                    .https_only()
                    .enable_http2()
                    .build();
                Client::builder(TokioExecutor::new())
                    .http2_only(true)
                    .build(connector)
            })
            .collect();
        Self { clients: Arc::new(clients), next: Arc::new(AtomicUsize::new(0)) }
    }

    pub fn get(&self) -> &Client<HttpsConnector<HttpConnector>, B> {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        &self.clients[idx]
    }
}
```

Each client maintains its own HTTP/2 connection. Round-robin selection is a single atomic increment.

---

## 9. SDK: Zero-Boilerplate Module Export

Modules implement one trait and call one macro. The runtime handles health checks, initialization, and WASI binding automatically:

```rust
// Guest module code — this is all you write:

struct MyModule;
wr_sdk::export!(MyModule);

impl wr_sdk::ServiceGuest for MyModule {
    fn init() {
        wr_sdk::db::enable_tracing();
    }

    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let rows = wr_sdk::db::query(
            "SELECT id, name FROM items WHERE active = $1",
            &[PgValue::Boolean(true)],
        ).unwrap();
        // ...
    }

    fn health_check() -> bool {
        // Return false to remove this instance from routing
        true
    }
}
```

Under the hood, the export macro intercepts `GET /__health` before it reaches your handler and routes it to `health_check()`. Initialization runs exactly once via `std::sync::Once`:

```rust
// wr-sdk/src/lib.rs

pub unsafe fn _export_handle_cabi<T: ServiceGuest>(arg0: i32, arg1: i32) {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(T::init);

    let request = unsafe { IncomingRequest::from_handle(arg0 as u32) };
    let response_out = unsafe { ResponseOutparam::from_handle(arg1 as u32) };

    let is_health_check = matches!(request.method(), Method::Get)
        && request.path_with_query().as_deref() == Some("/__health");

    if is_health_check {
        let status = if T::health_check() { 200 } else { 503 };
        crate::io::send_response(response_out, status, vec![]);
    } else {
        T::handle(request, response_out);
    }
}
```

---

## 10. Guest Module: Stock Exchange Order Matching

The stockmarket exchange module demonstrates how a guest WASM module uses transactions, row-level locking, cross-service calls, and distributed tracing together. This is a real order-matching engine running inside the sandbox.

Service setup — protobuf-generated routing, one-line export, DB tracing enabled at init:

```rust
// examples/stockmarket/exchange/src/lib.rs

use proto::LedgerServiceClient;
use wr_sdk::prelude::*;

struct Component;
wr_sdk::export!(Component with_types_in wr_sdk::bindings);

impl wr_sdk::ServiceGuest for Component {
    fn init() {
        wr_sdk::db::enable_tracing();
    }

    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        proto::exchange_service_handle(&Component, request, response_out);
    }
}
```

Order placement opens a transaction, inserts the order, and finds matches on the opposite side with `FOR UPDATE` row locks to prevent race conditions across concurrent requests:

```rust
// examples/stockmarket/exchange/src/lib.rs

fn place_order(&self, req: proto::PlaceOrderRequest)
    -> Result<proto::PlaceOrderResponse, ServiceError>
{
    let sp = wr_sdk::span!(
        "exchange.place_order",
        "order.trader_id" => req.trader_id.as_str(),
        "order.symbol" => req.symbol.as_str(),
        "order.is_buy" => if req.is_buy { "true" } else { "false" },
        "order.quantity" => req.quantity,
        "order.price" => req.price
    );

    let tx = wr_sdk::db::transaction()?;

    // Insert the new order, get back the assigned ID
    let order_id: i64 = tx.query_scalar(
        "INSERT INTO orders (trader_id, symbol, is_buy, price, quantity, remaining) \
         VALUES ($1, $2, $3, $4, $5, $5) RETURNING order_id",
        &[
            PgValue::Text(req.trader_id.clone()),
            PgValue::Text(req.symbol.clone()),
            PgValue::Boolean(req.is_buy),
            PgValue::Int8(req.price),
            PgValue::Int8(req.quantity),
        ],
    )?;

    // Find matching orders on the opposite side with row-level locks
    let (side_filter, order_clause) = if req.is_buy {
        ("is_buy = false AND price <= $2", "price ASC, created_at ASC")
    } else {
        ("is_buy = true AND price >= $2", "price DESC, created_at ASC")
    };

    let matches = tx.query(
        &format!(
            "SELECT order_id, trader_id, price, remaining \
             FROM orders \
             WHERE symbol = $1 AND {side_filter} AND remaining > 0 \
               AND order_id != $3 \
             ORDER BY {order_clause} \
             FOR UPDATE"
        ),
        &[
            PgValue::Text(req.symbol.clone()),
            PgValue::Int8(req.price),
            PgValue::Int8(order_id),
        ],
    )?;

    // ... match orders, update positions atomically ...

    tx.commit()?;
```

After the transaction commits, the module calls the ledger service over HTTP — transparently routed through the proxy to another WASM module on a different engine:

```rust
    // Record trades on the ledger (after commit, so DB state is consistent)
    let ledger = LedgerServiceClient::new("stockmarket.ledger");
    for (buyer_id, seller_id, qty, price, oid) in &trade_records {
        if let Err(e) = ledger.record_trade(proto::RecordTradeRequest {
            buyer_id: buyer_id.clone(),
            seller_id: seller_id.clone(),
            symbol: req.symbol.clone(),
            quantity: *qty,
            price: *price,
            order_id: *oid,
        }) {
            wr_sdk::log::log(&format!("ledger record_trade error: {e}"));
        }
    }

    tracing::set_attr(&sp, "order.trades_matched", trades_matched);
    tracing::set_attr(&sp, "order.total_filled", total_filled);

    Ok(proto::PlaceOrderResponse {
        order_id,
        trades_matched,
        quantity_filled: total_filled,
        quantity_remaining: my_remaining,
    })
}
```

`LedgerServiceClient::new("stockmarket.ledger")` constructs an HTTP client pointing at `http://stockmarket.ledger/` — the runtime intercepts the call, attaches routing headers, and streams it through the proxy to whichever engine is running the ledger module. The guest code has no knowledge of engine addresses, mTLS, or load balancing.

---

## 11. Guest Module: LLM-Powered Code Generation Agent

The codegen agent module shows how a WASM guest can call LLMs, read from blob storage, persist session state to Postgres, and implement multi-turn reasoning loops — all within the sandbox.

The agent loads documentation from the blobstore, scores files by relevance, and builds a context window with a budget:

```rust
// examples/codegen/agent/src/lib.rs

fn build_context(doc_prefixes: &[String], task_description: &str) -> Result<String, ServiceError> {
    let task_lower = task_description.to_lowercase();
    let mut context = String::new();
    let mut total_size: usize = 0;

    for prefix in doc_prefixes {
        let manifest_data = store::get_object(BUCKET, &format!("{prefix}/manifest.json"))?;
        let manifest: Manifest = serde_json::from_slice(&manifest_data)?;

        // Score files by relevance to the task
        let mut scored: Vec<(i32, &ManifestEntry)> = manifest.files.iter()
            .map(|entry| {
                let mut score: i32 = 0;
                if entry.key.contains("readme") { score += 10; }
                if entry.key.ends_with("lib.rs") { score += 8; }
                // Boost files whose names match task keywords
                for word in task_lower.split_whitespace() {
                    if word.len() > 2 && entry.key.to_lowercase().contains(word) {
                        score += 3;
                    }
                }
                (score, entry)
            })
            .collect();
        scored.sort_by(|a, b| b.0.cmp(&a.0));

        for (_, entry) in scored {
            if total_size + entry.size as usize > MAX_CONTEXT_BYTES { continue; }
            let data = store::get_object(BUCKET, &entry.key)?;
            if let Ok(text) = String::from_utf8(data.clone()) {
                context.push_str(&format!("<file: {label}>\n{text}\n</file>\n\n"));
                total_size += data.len();
            }
        }
    }
    Ok(context)
}
```

The main loop calls Claude with rate-limit retries, then self-reviews the diff in subsequent turns until the model says "LGTM":

```rust
// examples/codegen/agent/src/lib.rs

fn run_task(&self, req: proto::RunTaskRequest) -> Result<proto::RunTaskResponse, ServiceError> {
    let span = wr_sdk::span!("agent.run_task",
        "session.id" => session_id.as_str(),
        "agent.max_turns" => max_turns
    );

    // Persist session to DB
    database::execute(
        "INSERT INTO sessions (session_id, status) VALUES ($1, 'active') \
         ON CONFLICT (session_id) DO UPDATE SET status = 'active', updated_at = now()",
        &[PgValue::Text(session_id.clone())],
    )?;

    // Turn 1: initial generation
    let resp = complete_with_retry(|| {
        CompletionBuilder::sonnet()
            .system(&system_prompt)
            .max_tokens(8192)
            .user(&user_prompt)
    })?;

    // Subsequent turns: self-review and refine
    while turn < max_turns {
        let resp = complete_with_retry(|| {
            CompletionBuilder::sonnet()
                .max_tokens(8192)
                .user(format!("Here is a unified diff I produced:\n\n{prev_assistant}"))
                .user("Review the diff. If improvements are needed, produce an updated \
                       unified diff. If correct, respond with exactly: LGTM")
        })?;

        if text.trim() == "LGTM" { break; }
        latest_diff = text.clone();
    }

    // Persist final result
    database::execute(
        "UPDATE sessions SET status = 'complete', latest_diff = $2 WHERE session_id = $1",
        &[PgValue::Text(session_id.clone()), PgValue::Text(latest_diff.clone())],
    )?;

    tracing::set_attr(&span, "agent.turns_used", turn);
    tracing::set_attr(&span, "agent.total_input_tokens", total_input);
    // ...
}
```

Rate-limited LLM calls use the WASI monotonic clock to sleep without blocking the runtime:

```rust
// examples/codegen/agent/src/lib.rs

fn complete_with_retry(
    mut build: impl FnMut() -> CompletionBuilder,
) -> Result<CompletionResponse, LlmError> {
    for attempt in 0..=MAX_RETRIES {
        match build().complete() {
            Ok(resp) => return Ok(resp),
            Err(LlmError::RateLimited(retry_after)) if attempt < MAX_RETRIES => {
                let secs = retry_after.unwrap_or(30);
                let nanos = secs as u64 * 1_000_000_000;
                monotonic_clock::subscribe_duration(nanos).block();
            }
            other => return other,
        }
    }
    unreachable!()
}
```

This module combines five host capabilities — LLM inference, blobstore reads, database writes, distributed tracing, and logging — in a single WASM component with no direct network access.

---

## 12. Hermetic Integration Tests

The test harness spins up all three services in-process on ephemeral ports with an in-memory PKI — no Docker, no certificates on disk:

```rust
// wr-tests/tests/helpers.rs

pub struct TestPki {
    pub ca_cert_der: Vec<CertificateDer<'static>>,
    pub node_cert_der: Vec<CertificateDer<'static>>,
    pub node_key_der: PrivateKeyDer<'static>,
}

/// Generate a CA + node cert entirely in memory. No files on disk.
pub fn generate_test_pki() -> TestPki {
    use rcgen::{CertificateParams, IsCa, KeyPair, SanType};
    // ...
}
```

A single test can stand up the full system:
- `start_manager()` — gRPC server on a random port
- `start_proxy()` — proxy with live routing table sync
- `spawn_stub_engine()` — minimal HTTP server for assertions
- `manager_pool()` — isolated Postgres schema per test with automatic cleanup

Every integration test is fully isolated, runs in parallel, and completes in milliseconds.

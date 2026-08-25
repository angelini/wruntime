# Runtime Invariants

For each change, **preserve** the contract, **inspect** the listed implementation boundary, and **prove** it with focused tests.

## Lifecycle and readiness

- **Preserve:** manager, proxy, and engine startup/shutdown ordering; registrations begin with unhealthy routes; engine provisioning, module migrations, pool creation, and component loading finish before the immediate readiness heartbeat; periodic engine/module heartbeats continue afterward.
- **Inspect:** manager service/state/database, engine `main.rs`/`engine.rs`/`registry.rs`, shutdown tasks, and health recomputation.
- **Prove:** manager, health, migration, and multi-manager tests; warning-free ecommerce validation when flow changes.

## Routing and circuit breaking

- **Preserve:** routing-table versions increase with durable state updates; persisted state and proxy indexes converge; exact versions, semver ranges, and unpinned requests retain distinct selection semantics; unhealthy routes are excluded. Continuously active forwarding addresses retain their prepared breaker across refreshes; completed publication-plus-eviction is the reset boundary, and in-flight old handles remain isolated from later re-adds.
- **Inspect:** manager routing persistence, proxy `routing.rs`, `indexed_routing.rs`, routing/forward layers, and circuit-breaker membership eviction.
- **Prove:** version, proxy, concurrent-routing, circuit-breaker refresh/lifetime, and cross-node tests.

- **Preserve:** local-engine, peer-proxy, public-ingress, and external-egress branches remain explicit. Egress cannot be mistaken for internal module routing, and circuit state applies to the correct destination.
- **Inspect:** ingress, routing, forward, and egress layers plus node service.
- **Prove:** ingress, egress, proxy, and cross-node tests.

## Trust and transport boundaries

- **Preserve:** untrusted ingress cannot supply reserved `x-wr-*` headers; trusted layers set routing metadata; source metadata is observability/routing context, never authorization.
- **Inspect:** ingress sanitization, engine outbound interception, proxy forwarding, and tests for header spoofing.
- **Prove:** ingress, egress, namespace, and proxy tests.

- **Preserve:** loopback engine/proxy listeners may use plain HTTP only on their documented boundary; manager gRPC and peer-proxy traffic use mTLS with identity validation; manager liveness gossip uses its separately configured UDP listener.
- **Inspect:** manager/proxy/engine listener setup, TLS helpers/config, peer clients, and chitchat gossip setup.
- **Prove:** config, cross-node, multi-manager, and certificate/identity tests.

## Database, secrets, and capabilities

- **Preserve:** the manager generates/stores namespace credentials; the engine uses target-database admin credentials to provision roles, admin-owned schemas, and grants; guest pools use clean-recycled namespace-role sessions; `search_path` selects a module default but namespace grants are the authorization boundary; namespace roles cannot drop module schemas; other namespace roles and guest roles cannot access unrelated schemas, `wr__jobs`, or `wr_system`; direct database access is limited to documented control-plane and host-capability exceptions.
- **Preserve topology:** configured `[database]` creates one eager admin pool capped by its `max_connections`; every DB-enabled namespace has one guest pool sized by the checked sum of all configured instance contributions; every worker entry has one non-pooled `LISTEN` session. Admin policy is `Fast`, guest policy is clean recycle followed by module checkout setup.
- **Inspect:** manager DB/migrations/crypto, engine startup manifest/database runtime/pool/migration/provisioning/DB host modules, and namespace tests.
- **Prove:** DB, namespace, migration, and secrets tests.

- **Preserve:** secret values never appear in manager APIs, logs, generated config, or guest metadata. Guests receive only resolved environment values for explicitly referenced secrets.
- **Inspect:** manager secret storage/RPCs, engine registration/environment construction, CLI secret commands.
- **Prove:** secrets tests and log/diff review.

- **Preserve:** a guest's WIT imports and module capability opt-ins are validated before startup; host implementations still enforce authorization, scope, input, and resource limits as defense in depth.
- **Inspect:** engine component import validation, config, state, and each capability host implementation.
- **Prove:** split WASM capability tests, including negative fixtures.

## Workers and schedules

- **Preserve:** job claims atomically persist a fence plus fixed `lease_expires_at`; stale completion/failure/recovery transitions require the active fence and clear claim metadata; retries honor attempt/timeout policy; delivery is at least once, so handlers must be idempotent. One recovery coordinator runs per engine after queue migration, remains safe alongside other engines, and progresses independently of module worker loops.
- **Inspect:** engine database runtime/job migrations/`worker.rs`, manager `scheduler.rs`, control-plane proto, SDK jobs, and worker client generator.
- **Prove:** worker, scheduler, and schedules tests.

- **Preserve:** a non-empty ad-hoc worker version is claimed exactly; an empty ad-hoc version is name-only; manager schedules remain version-pinned. Canonical job types use `/{package}.{Service}/{Method}`.
- **Inspect:** proxy version headers, SDK jobs, `WrWorkerClientGenerator`, scheduler persistence.
- **Prove:** worker, schedule, and version tests.

## Migrations and generated contracts

- **Preserve:** manager migrations are embedded control-plane migrations under their advisory-lock policy. Engine job-queue migrations are embedded, use a distinct detached-session lock/history in `wr__jobs`, and complete before module migrations, workers, recovery, or readiness. Module migrations are trusted guest-owned SQL run with engine admin credentials, use the module schema as their default `search_path`, hold cancellation-safe per-schema serialization locks, and complete before readiness. Duplicate configured instances sharing `(namespace, module)` migrate once and must agree on one canonical migration source.
- **Inspect:** `wr-manager/src/migrate.rs`, manager migrations, `wr-engine/src/{startup_db,job_migration,migration}.rs`, engine job migrations, and guest configs/migrations.
- **Prove:** migration and startup/health tests plus affected example.

- **Preserve:** canonical protobuf/WIT sources fan out consistently; generated `OUT_DIR` Rust is never edited; WIT mirrors and checked-in descriptors stay synchronized.
- **Inspect:** [generated contracts](generated_contracts.md).
- **Prove:** compile checks, generator unit tests, `just test-wasm`, and affected example builds.

## Telemetry and operations

- **Preserve:** trace context propagates across guest, proxy, peer, and engine boundaries; stable attribute names retain meaning and avoid secrets/high-cardinality surprises.
- **Inspect:** proxy tracing layer, engine tracing host/interception, SDK tracing helpers.
- **Prove:** tracing host and integration tests; inspect emitted telemetry when semantics change.

- **Preserve:** ecommerce E2E emits no warnings. Deployment generation is deterministic for the same inputs, and systemd/Docker outputs preserve equivalent identity, TLS, config, paths, and lifecycle behavior.
- **Inspect:** `dev/validate-all.sh`, example scripts, CLI bundle/deploy generation, deployment templates.
- **Prove:** `just validate-ecommerce`; deployment tests and repeated-output diff.

## Tests and examples

- **Preserve:** shared helpers live under `wr-tests/tests/helpers/`; WASM guests are protocol/negative-test fixtures, not production scaffolds; prerequisite-based skipping remains explicit and consistent.
- **Inspect:** helper modules, split WASM tests, Just recipes, and test fixture manifests.
- **Prove:** affected focused tests through the same recipe users run.

- **Preserve:** ecommerce, stockmarket, and codegen examples are executable specifications. Advertised configurations, APIs, schemas, migrations, and run scripts must agree.
- **Inspect:** all files in the affected example and linked guest documentation.
- **Prove:** build and inline recipe for that example; use `just validate-ecommerce` for ecommerce.

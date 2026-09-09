# Runtime Invariants

For each change, **preserve** the contract, **inspect** the listed implementation boundary, and **prove** it with focused tests.

## Lifecycle and readiness

- **Preserve:** manager, proxy, and engine startup/shutdown ordering; registrations begin with unhealthy routes; semantic readiness opens admission only after each service's documented barriers; engine provisioning, migrations, pools, component loading, module health checks, atomic manager publication, and local proxy routing-version convergence finish before `READY`; periodic engine/module heartbeats continue afterward.
- **Preserve:** lifecycle state remains distinct from cluster health severity. Every long-lived, listener, connection, module-request, worker, LISTEN, recovery, scheduler, discovery, and heartbeat task is owned and joined. Drain closes the relevant admission before teardown, worker claims stop before waiting, route withdrawal converges before engine HTTP admission closes, heartbeat publication is fenced before drain/deregister, and final deregistration cannot be undone by stale proxy state.
- **Preserve:** each signal-driven stop operation uses one absolute 30-second deadline; nested convergence, admission, deregistration, and join waits never reset it. Generated systemd and Compose supervisors provide 45 seconds for signal-driven full shutdown, use semantic readiness notification/probes and SIGTERM, and stop engines before their proxy. Deadline escalation aborts and joins named leftovers and exits non-zero. Lifecycle RPC is read-only status; service-specific drain phases remain internal.
- **Preserve runner ownership:** one scoped foreground `dev run` owner retains every local service `Child`, detects unexpected exit, owns the optional scenario process group, and reaps concurrent engine, proxy, then manager waves. The 45-second per-process boundary latches terminal failure evidence; it does not permit dropping the sole handle, so the owner remains in reap-only mode until exit is proven. Startup is manager, proxies, engines; after engine READY the runner captures one manager routing version and requires every exact READY proxy activation to report at least that installed version. Runners use typed lifecycle/routing/deployment/health expectations rather than PID, TCP, logs, fixed sleeps, or expected-nonzero gates. A primary failure remains primary, while cleanup failure is separately reported and fails a clean run; no socket, lock, or persistent process state belongs to foreground orchestration. Local exit proof belongs to the retained `Child`; deployed exit proof belongs to the continuously fenced node agent's typed systemd/Docker backend adapter. The agent sends SIGTERM through the backend owner and inspects the exact backend instance; endpoint absence is not exit proof and inspection failure is unknown.
- **Inspect:** manager service/state/database and orchestration; proxy NodeService/routing/admission/listeners; engine `main.rs`/`server.rs`/`engine.rs`/`worker.rs`/`registry.rs`; lifecycle task ownership; `wr-cli/src/cmd/{dev,foreground_runner,lifecycle,helpers,node}.rs`; example/deployment runners; and deployment generation.
- **Prove:** lifecycle, manager, health, proxy, version, worker, migration, multi-manager, CLI foreground-runner/expectation/node-stop, and deployment-template tests; `just test-wasm`; `just deployment-e2e-python-test`; warning-free local examples; and protected systemd/Docker lifecycle qualification when generation or remote lifecycle behavior changes.

## Durable operator lifecycle

- **Preserve:** manager-owned intent is immutable and idempotent by `(actor, request_token)`; one forward/restoration operation runs per node, with an absolute forward deadline, explicit phase, append-only events, and per-slot staged/committed authority. Agent claims, renewals, observations, and results are fenced by node, authenticated principal, activation ID, and lease epoch. A mutation is returned only after its manager delivery/ambiguity record commits. The live activation retries one exact tagged result from memory before any further claim; advancement and its indefinitely retained exact receipt are atomic. No agent recovery journal exists: a replacement activation never replays old work and must use manager-directed typed backend inspection. Inconclusive/query-error evidence pauses and cannot authorize another protected mutation. Reported result fields are evidence only; the manager derives advancement from one coherent snapshot.
- **Preserve:** destructive authorization comes only from manager mTLS leaf fingerprints mapped to operators or node-bound agents. Source/proxy headers are never authorization. The agent advertises only node/activation identity, its process-computed binary digest, exact protocol, backend kind, and normalized capabilities; required capabilities use subset matching. Manager receipt time, fresh activation, and lease epoch remain independent fences. Local config/paths/credentials and manager-owned cleanup retention never authorize claims; protocol mismatch requires binary-update remediation rather than compatibility behavior.
- **Preserve:** a finalized deployment is the complete desired inventory; the manager derives deterministic sequential additions, retained replacements, and removals without caller-selected rollout mechanics. Exact unchanged identities cause no host effect, and a new actor/token submission of the exact committed revision succeeds immediately without deployment or authority mutation. `max_unavailable` uses coherent serving progress before every destructive stop; final-slot and empty-inventory transitions require explicit downtime acknowledgement. Restart is single-slot maintenance that preserves the committed revision/inventory. Pre-commit cancel/failure/deadline enters deadline-free restoration of the complete source inventory and authority before terminal state; rollback is separately authorized and never automatic. Commit occurs only after exact backend/lifecycle/registration/routing evidence and per-slot authority convergence and is terminally successful. Release cleanup is independent per-node maintenance: periodic reconciliation alone discovers/materializes exact manager-derived revision/digest authority, every protection-changing transaction immediately fences existing authority, renewable generation leases cancel stale effects, and cleanup degradation never poisons serving health.
- **Inspect:** root protobuf, manager auth/service/operations/status/database and migrations, CLI operation/deploy/agent/backend code, deployment fixtures, and shared integration helpers.
- **Prove:** migration; the zero-to-N, no-effect, replacement, mixed add/retained/remove, N-to-zero, rollback, restart, availability, restoration, ambiguity, and lease operation matrix; node-agent-operation; manager mTLS; multi-manager takeover; lifecycle/proxy/version integration; deterministic bundle/config/unit tests; pure deployment assertions; and protected systemd/Docker qualification.

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

- **Preserve:** loopback engine/proxy listeners may use plain HTTP only on their documented boundary; manager gRPC and peer-proxy traffic use mTLS with identity validation. Manager liveness is derived only from PostgreSQL leases using server-side time; manager and proxy discovery thresholds are one cluster-wide contract, stale-row retention remains substantially longer than live membership, and cleanup cannot suspend self-renewal.
- **Inspect:** manager/proxy/engine listener setup, TLS helpers/config, manager lease registration/heartbeat/reaping, and manager discovery fallback.
- **Prove:** config, proxy discovery, cross-node, multi-manager, migration, and certificate/identity tests.

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

- **Preserve job administration:** every operation has explicit queue scope. Engines sharing one physical `wr__jobs` database use one stable queue ID; different databases use different IDs. `JobService` shares the manager's unified mTLS listener and is default-deny under URI-principal policy. Manager-to-engine calls use the manager's distinct clientAuth workload leaf; engines require an enrolled, non-revoked same-cluster manager principal from a fresh snapshot. Routing/source headers never authorize either hop. The manager stores only delegate metadata and never opens the queue database. Reads qualify delegates before dispatch; mutations qualify one deterministic delegate and never replay. List is metadata-only and bounded; inspect is the only byte-disclosing operation. Compatible size ceilings apply across persistence and transport. Persisted lifecycle corruption fails closed. Retry locks one dead row, resets attempt/terminal/claim state, preserves last failure, notifies before commit, and is never replayed after uncertainty. Engine shutdown closes and drains admitted admin RPCs before route withdrawal.
- **Inspect:** control-plane proto, manager auth/delegate selection, engine job-admin listener/queue SQL, V3 inventory indexes, CLI redaction/export, and deployment certificate/address generation.
- **Prove:** job migration, worker query/summary/retry-race, manager registration/delegate, job-admin mTLS/forwarding, CLI, and protected deployment tests.

## Migrations and generated contracts

- **Preserve:** manager migrations are embedded control-plane migrations under their advisory-lock policy. Engine job-queue migrations are embedded, use a distinct detached-session lock/history in `wr__jobs`, and complete before module migrations, workers, recovery, or readiness. Module migrations are trusted guest-owned SQL run with engine admin credentials, use the module schema as their default `search_path`, hold cancellation-safe per-schema serialization locks, and complete before readiness. Duplicate configured instances sharing `(namespace, module)` migrate once and must agree on one canonical migration source.
- **Inspect:** `wr-manager/src/migrate.rs`, manager migrations, `wr-engine/src/{startup_db,job_migration,migration}.rs`, engine job migrations, and guest configs/migrations.
- **Prove:** migration and startup/health tests plus affected example.

- **Preserve:** canonical protobuf/WIT sources fan out consistently; generated `OUT_DIR` Rust is never edited; WIT mirrors and checked-in descriptors stay synchronized.
- **Inspect:** [generated contracts](generated_contracts.md).
- **Prove:** compile checks, generator unit tests, `just test-wasm`, and affected example builds.

## Manager fleet rollout

- **Preserve:** one replay-stable manifest and shared policy validator bind the caller operation/executor identity, complete source/target sets, target generation/digest, deployment leaf, and every executable/image, backend-spec, config, credential, and selector digest. `STAGING` and `FAILED_PRE_CLOSE` change no active selector. Only after `OLD_CLOSED` may a fenced stopped/masked host action atomically select `current-activation.json`; the stable launcher verifies the complete selected set. Multi-manager rollout retains a closed control endpoint; sole-manager continuation is fsynced and bounded to 120 seconds. Stale/conflicting host evidence, digest mismatch, and ambiguous takeover fail closed. No automatic rollback or generic cleanup is permitted.
- **Preserve protected state:** systemd binaries and OCI images/specs are digest-qualified, roots are fixed under `/etc/wruntime/pki/roots`, immutable credential sets stay outside releases/images, and manager/node-agent config retains only `current.toml` and `previous.toml`. Sensitive transfer uses unpredictable owner-only staging, verified mode/digest, atomic install, parent fsync, and invocation-owned cleanup.
- **Inspect:** `wr-cli/src/cmd/{manager_deploy_set,managers,helpers,service_gen}.rs`, manager rollout services/database, deployment fixtures, and protected systemd/Compose assertions.
- **Prove:** focused CLI/manager tests, deployment Python tests, selector/digest failure fixtures, and `just validate-all --deployment-e2e` on the protected runner.

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

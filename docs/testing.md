# Testing

Maintainers should select checks by change class in the [validation matrix](agents/wruntime-maintainer/validation.md). This page documents command behavior and prerequisites.

Common recipes:

```bash
just dev-up                  # start Postgres, Grafana/LGTM, and RustFS S3
just multi-node              # run the local two-node topology until Ctrl-C
just multi-node-inline       # start, verify, and stop the local topology
just test                    # build test guests, then run all tests
just test-integration        # build test guests, then run wr-tests
just test-one <test_name>    # build test guests, then run one named test
just build-wasm-guests       # build every WASM guest sequentially
just test-wasm               # build WASM guests, then run host binding tests
just clean-wasm-cache        # remove only the shared Cargo guest cache
just validate-ecommerce      # ecommerce inline run, failing on WARN/WARNING output
just bench-proxy-routing     # warmed HTTP/2 proxy routing/forwarding benchmark
just validate-all --no-deployment-e2e # full local suite with an explicit live-stage skip
just validate-all --no-deployment-e2e --skip-dev-up --no-codegen-e2e # Pi sandbox
just validate-all --deployment-e2e    # trusted runner: require both live deployment backends
just deployment-e2e-python-test       # locked provider/assertion unit tests
just test-local-e2e-lock              # same-host fixed-port exclusion fixture
just test-lifecycle-runners            # foreground runner/barrier/reaping fixtures
just deployment-e2e-preflight         # non-mutating Proxmox target verification
just dev-down                # stop dev infrastructure
```

`just test`, `just test-integration`, `just test-one`, and `just test-wasm`
first rebuild the test WASM guest artifacts incrementally, then set the
`WRT_TEST_DB_URL` and `WRT_TEST_S3_*` variables expected by integration tests.
This prevents changed guest sources or schemas from running against stale staged
components. Run `just dev-up` first when using those full recipes.

`just bench-proxy-routing [iterations] [warmup] [concurrency]` runs only the proxy-to-stub benchmark. It creates one HTTP/2 client, warms its connection before measurement, and reuses it for sequential and concurrent requests. The default dimensions are `500 10 20`; explicit positional values are preserved. `just bench [iterations] [warmup] [concurrency]` applies the same dimension contract to the full benchmark test target. `cargo test -p wr-proxy --features count-allocations direct_selection_core_is_allocation_free_for_eight_candidates` runs the vetted `allocation-counter` gate after a warm routing-core call; selector parsing is checked separately because `semver::VersionReq` parsing is not part of that zero-allocation selection boundary.

Direct `cargo test -p wr-tests` runs are allowed for quick local checks.
DB-backed tests use `WRT_TEST_DB_URL` and skip through the shared helper policy
when it is absent. S3-backed tests use `WRT_TEST_S3_ENDPOINT`,
`WRT_TEST_S3_ACCESS_KEY`, and `WRT_TEST_S3_SECRET_KEY`; direct S3-backed cargo
tests require those variables because the current blobstore helper expects
them. Required WASM artifacts must be built before direct WASM host binding
test runs. The LLM guest protocol uses protobuf enums for stop reasons, stream
events, and error kinds, while the DB guest protocol uses `oneof` parameter and
column values rather than JSON strings. Positive-path tests can use `RpcPath`
and `GuestHarness::dispatch_typed`; raw request helpers remain available for
malformed-input coverage.

Rust guest builds launched by `wr-cli` use the mandatory shared Cargo target
at `target/wasm-guests`. Each configured guest-local `wasm_path` is a staged,
stripped runtime artifact; Cargo's unmodified build output remains in the
shared target. `just build-wasm-guests` resolves every repository guest and
builds them sequentially in one CLI invocation.

`just clean-wasm-cache` removes only `target/wasm-guests`, not staged guest
artifacts. Root `cargo clean` removes the whole root `target/` tree, including
the shared cache. Running `cargo clean` inside an individual guest does not
clear the shared cache unless Cargo is explicitly given that target directory.
Deleting a staged guest artifact does not require a cache reset: the next CLI
guest build recreates it from the shared artifact. Existing guest-local target
directories containing old Cargo intermediates may be deleted once; subsequent
CLI builds recreate only the configured staged output directories there.

WASM host binding tests require:

- `rustup target add wasm32-wasip2`
- `protoc`
- `wasm-tools`
- Postgres and RustFS from `just dev-up`

Example inline scripts require the built workspace binaries, the same dev
infrastructure, and Python 3 for small JSON/config rendering and assertions.
The multi-node smoke test requires Postgres but not RustFS. `just test-lifecycle-runners` uses focused OS-process and local gRPC fixtures for `dev run` argument validation, fixed startup order and within-wave concurrency, READY activation and exit-before-ready tails, the shared routing deadline and scenario gate, zero-descendant one-shot success, unexpected-service scenario-tree cleanup, primary/cleanup/both-failure reporting, recorded-signal spawn guards, subprocess parent-death protection, and a controlled post-boundary exit that proves reap-before-return while retaining terminal evidence. Real SIGINT fixtures run serially and cover interruption during readiness, routing convergence, no-scenario monitoring, plus second-signal scenario escalation. The runner retains every service `Child` and the scenario process group until reaping proves exit; the fixtures also assert that no owned descendant, state socket, lock, or persistent process state remains. Repository example Just recipes build artifacts first, then each run script makes one foreground `dev run` call with its scenario. The codegen example uses `wr-cli invoke --json` and Python stdlib JSON parsing; no `jq` dependency is required.

`just validate-all` is a thin alias for `dev/validate-all.sh`. The script
orchestrates existing Just recipes for formatting, compile checks, lints, WASM
guest builds, Rust tests, and semantic-lifecycle fixed-port E2E examples. All guests are built
through one sequential `just build-wasm-guests` invocation so independent
Cargo processes never contend on the shared target during that stage. E2E
examples run sequentially because they share ports and example resources. The
runner holds a non-blocking, per-user host lock before resetting those resources;
individual example scripts acquire the same lock, so conflicting runs from
another worktree fail before starting services or mutating the example database.
The lock defaults to `${XDG_RUNTIME_DIR:-/tmp}/wruntime-local-e2e-<uid>.lock` and
can be overridden with `WRT_LOCAL_E2E_LOCK_FILE`. Kernel `flock` ownership makes
stale lock files harmless after a runner exits. Logs and `summary.txt` are
written under `target/validate-all/<timestamp>-<pid>/`; terminal
failure output is capped for agent-friendly context use. Codegen E2E runs only
when `ANTHROPIC_API_KEY` is set by default; use `--codegen-e2e` to require it
or `--no-codegen-e2e` to always skip it.

The deployment lifecycle stage always requires an explicit choice. Trusted
runners use `just validate-all --deployment-e2e`, which runs the systemd and
Docker passes serially before fixed-port local examples. Local development uses
`just validate-all --no-deployment-e2e`; the summary records separate `SKIPPED`
rows for both backends. In the Pi sandbox (`DOTGEN_PI_SANDBOX=1`), use
`just validate-all --no-deployment-e2e --skip-dev-up --no-codegen-e2e`: Docker
cannot run there, but the existing development services are exposed. Only
`ANTHROPIC_API_KEY` is unavailable for the local examples, so codegen is skipped
while multi-node, ecommerce, and stockmarket still run. Do not pass `--no-e2e`
in Pi. Outside Pi, `--no-e2e` affects only fixed-port local examples, so it may
be combined with `--deployment-e2e`. `--e2e-only` still requires an explicit
deployment choice and runs the enabled E2E stages.

Fixed-port local E2Es require `flock`. Live deployment additionally requires
`uv`, `cargo-zigbuild`, SSH, and `psql`; its protected-target lock remains
separate from the local example lock.
Python dependencies and the Python 3.12 toolchain request are owned by the
nested `dev/deployment-e2e` project through `pyproject.toml`, `.python-version`,
and the checked-in `uv.lock`. Recipes and the lifecycle harness use
`uv run --project dev/deployment-e2e --locked`, so no manually activated virtual
environment or system `pip` installation is required. Protected runner inputs
are `PVE_HOST`, `PVE_USER`,
`PVE_TOKEN_NAME`, `PVE_TOKEN_VALUE`, `WRT_DEPLOY_E2E_SSH_KEY`,
`WRT_DEPLOY_E2E_DB_URL`, and `WRT_SECRET_ENCRYPTION_KEY`. The dedicated SSH
known-hosts file defaults to `~/.ssh/wruntime-e2e-known_hosts` and can be
overridden with `WRT_DEPLOY_E2E_KNOWN_HOSTS`. Never pass these values in a
checked-in config or transcript.

The Proxmox HTTPS client uses the Debian/Ubuntu OS CA bundle at
`/etc/ssl/certs/ca-certificates.crt` instead of Requests' bundled `certifi`
roots. Install the private Proxmox CA under `/usr/local/share/ca-certificates/`
and run `sudo update-ca-certificates` before preflight. Set `PVE_CA_BUNDLE` to
an alternate bundle path on other operating systems or runner layouts; TLS
verification is never disabled.

Each backend starts and ends with snapshot rollback and normally takes several
minutes plus cross-compilation time. Manager deployment restarts/recreates the
service and waits on the exact launcher-issued activation identity and manager service kind; node
deployment and rollback wait on exact `VerifyDeployment` identity and conditions. Post-ready guest invocations run
once. Expected unhealthy evidence uses `cluster wait` and therefore succeeds
with a matching JSON snapshot rather than an expected non-zero display gate.
Engine stop uses one `wr-cli node stop --json` call. The command maps the engine slot to the generated systemd unit or Docker Compose service and owns graceful action, bounded escalation, and final-exit inspection under one absolute 45-second deadline. The harness validates and retains its structured component/backend/target/action/final-state record, keeps the proxy available while cluster status becomes unhealthy, and performs no lifecycle tunnel or separate backend poll.
Per-task output, lifecycle state JSON, remote diagnostics, bundle inspections,
and the final reset result are retained under `WR_VALIDATE_LOG_DIR` (or
`target/validate-all/<timestamp>/`). Diagnostic collection failures are listed
without replacing a primary failure; cleanup/reset failure makes a nominally
successful run fail. Live runs must not be started unless all protected inputs
are present. The provider never creates or deletes snapshots or VMs, and reset
failures are fatal.

Focused commands are `just deployment-e2e-python-test`, `just
deployment-e2e-preflight`, `just deployment-e2e-systemd`, `just
deployment-e2e-docker`, and `just deployment-e2e`. The locked Python test recipe
runs the provider and JSON assertion `unittest` targets without Proxmox access.
After intentionally changing Python dependencies, refresh the nested lock with
`uv lock --project dev/deployment-e2e` and commit `pyproject.toml` and `uv.lock`
together.

The remaining polling in runner code is deliberately outside wruntime service
readiness: codegen polls application task status, deployment waits for the SSH
forward and PostgreSQL infrastructure under bounded deadlines, and the Proxmox
provider waits for asynchronous platform tasks. These paths preserve typed last
error/status evidence and must not be copied into service lifecycle gates.

## Dev infrastructure

Docker Compose provides Postgres, Grafana/LGTM, and RustFS S3:

```bash
just dev-up                  # start all dev services
just dev-down                # stop all dev services
just dev-logs                # tail logs from all services
just dev-logs postgres       # tail logs from a single service
just dev-ps                  # show running container status
just dev-reset-db            # drop module schemas, manager tables, migrations
```

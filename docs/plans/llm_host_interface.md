# LLM Host Interface (Claude API)

Allow WASM modules to call the Claude API (and potentially other LLM providers)
through a host binding, following the same pattern as `db.wit`, `blobstore.wit`,
and `tracing.wit`.

---

## Overview

Modules that need LLM inference currently have no way to call the Claude API
from within the WASM sandbox. The proxy's egress layer could forward raw HTTP to
`api.anthropic.com`, but that leaks API keys into guest code and forces every
module to reimplement the Claude protocol.

This plan adds a `wruntime:llm/inference` WIT interface that the engine
implements as a host binding. The engine holds the API key and HTTP client on
the host side — guests never see credentials.

---

## Steps

### 1. Add `wit/llm.wit`

Create `/wit/llm.wit`:

```wit
package wruntime:llm@0.1.0;

interface inference {
    /// A single message in a conversation.
    record message {
        role: message-role,
        content: string,
    }

    enum message-role {
        user,
        assistant,
    }

    /// Tool definition for function calling.
    record tool-def {
        name: string,
        description: string,
        /// JSON Schema for the tool's input parameters.
        input-schema: string,
    }

    /// A tool use request returned by the model.
    record tool-use {
        id: string,
        name: string,
        /// JSON-encoded arguments.
        input: string,
    }

    /// What the model produced.
    variant completion {
        /// Plain text response.
        text(string),
        /// The model wants to call one or more tools.
        tool-calls(list<tool-use>),
    }

    record completion-request {
        model: string,
        messages: list<message>,
        /// Optional system prompt.
        system: option<string>,
        /// Max tokens to generate.
        max-tokens: u32,
        /// Temperature (0.0–1.0).
        temperature: option<f32>,
        /// Tool definitions (empty = no tools).
        tools: list<tool-def>,
    }

    record token-usage {
        input-tokens: u32,
        output-tokens: u32,
    }

    record completion-response {
        completion: completion,
        usage: token-usage,
        /// "end_turn" | "tool_use" | "max_tokens"
        stop-reason: string,
    }

    variant llm-error {
        /// Bad request (invalid params, context too long).
        invalid-request(string),
        /// Auth failure (missing or bad API key on the host).
        auth(string),
        /// Rate limited — includes retry-after seconds if available.
        rate-limited(option<u32>),
        /// Upstream API error.
        api(string),
    }

    /// Single-shot completion (request → response).
    complete: func(req: completion-request) -> result<completion-response, llm-error>;

    /// Streaming completion — returns a cursor that yields text deltas.
    resource completion-stream {
        /// Pull the next chunk. Returns `none` when the stream is finished.
        next: func() -> result<option<string>, llm-error>;

        /// Final usage stats (available after stream exhausted).
        usage: func() -> option<token-usage>;
    }

    complete-stream: func(req: completion-request) -> result<completion-stream, llm-error>;
}

world llm-access {
    import inference;
}
```

### 2. Add engine config section

Add `LlmConfig` to `wr-engine/src/config.rs`:

```rust
#[derive(Deserialize, Clone)]
pub struct LlmConfig {
    /// LLM provider. Currently only "anthropic" is supported.
    pub provider: String,
    /// Environment variable name that holds the API key.
    /// Resolved at engine startup, never passed to guests.
    pub api_key_env: String,
    /// Base URL for the API. Defaults to "https://api.anthropic.com".
    #[serde(default = "default_llm_base_url")]
    pub base_url: String,
    /// Host-enforced ceiling on max_tokens per request.
    #[serde(default = "default_max_tokens_limit")]
    pub max_tokens_limit: u32,
}
```

Add to `EngineConfig`:

```rust
pub struct EngineConfig {
    // ... existing fields ...
    /// Optional LLM provider for inference-enabled modules.
    pub llm: Option<LlmConfig>,
}
```

Add `llm: bool` to `ModuleConfig` (same pattern as `database` and `blobstore`),
with a validation rule:

```rust
anyhow::ensure!(
    !module.llm || self.llm.is_some(),
    "module '{}' has llm = true but no [llm] section is configured",
    module.name,
);
```

Example TOML:

```toml
[llm]
provider = "anthropic"
api_key_env = "ANTHROPIC_API_KEY"
max_tokens_limit = 8192

[[module]]
name = "my-agent"
namespace = "example"
version = "0.1.0"
wasm_path = "target/my_agent.wasm"
schema_path = "proto/my_agent.binpb"
llm = true
```

### 3. Add `LlmRuntime` client (`wr-engine/src/llm.rs`)

Create `wr-engine/src/llm.rs`. This is the host-side HTTP client that talks to
the Claude API. Mirrors the structure of `blobstore.rs`:

```rust
use reqwest::Client;
use serde::{Deserialize, Serialize};
use crate::config::LlmConfig;

pub struct LlmRuntime {
    client: Client,
    api_key: String,
    base_url: String,
    max_tokens_limit: u32,
}

impl LlmRuntime {
    pub fn new(config: &LlmConfig) -> anyhow::Result<Self> {
        let api_key = std::env::var(&config.api_key_env)
            .with_context(|| format!("missing env var: {}", config.api_key_env))?;
        Ok(Self {
            client: Client::new(),
            api_key,
            base_url: config.base_url.clone(),
            max_tokens_limit: config.max_tokens_limit,
        })
    }

    /// Non-streaming Messages API call.
    pub async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        // Enforce host-side max_tokens ceiling
        let max_tokens = req.max_tokens.min(self.max_tokens_limit);
        // POST to {base_url}/v1/messages
        // Headers: x-api-key, anthropic-version: 2023-06-01, content-type: application/json
        // Map response JSON to CompletionResponse
        todo!()
    }

    /// Streaming Messages API call. Returns a tokio channel receiver
    /// that yields server-sent event deltas.
    pub async fn complete_stream(
        &self,
        req: CompletionRequest,
    ) -> Result<tokio::sync::mpsc::Receiver<StreamEvent>, LlmError> {
        // Same as complete but with "stream": true
        // Parse SSE events, send text deltas through channel
        todo!()
    }
}
```

**Key implementation details:**

- Use `reqwest` with `stream` feature for SSE parsing
- The `complete` path deserializes the full JSON response and maps `content`
  blocks to the `Completion` variant (text blocks → `Text`, tool_use blocks →
  `ToolCalls`)
- The `complete_stream` path opens an SSE connection, parses
  `content_block_delta` events, and sends text deltas through an mpsc channel
- Map HTTP status codes to `LlmError` variants: 400 → `InvalidRequest`,
  401/403 → `Auth`, 429 → `RateLimited` (parse `retry-after` header),
  5xx → `Api`

### 4. Add WIT host binding implementation

Add the WIT binding implementation alongside `LlmRuntime` in
`wr-engine/src/llm.rs` (or a separate `wr-engine/src/llm_host.rs`):

```rust
wasmtime::component::bindgen!({
    path:  "../wit/llm.wit",
    world: "llm-access",
    imports: { default: async },
    with: {
        "wruntime:llm/inference/completion-stream": CompletionStreamState,
    },
});

/// Resource state for a streaming completion.
pub struct CompletionStreamState {
    rx: tokio::sync::mpsc::Receiver<StreamEvent>,
    usage: Option<TokenUsage>,
}

impl wruntime::llm::inference::Host for ModuleState {
    async fn complete(
        &mut self,
        req: CompletionRequest,
    ) -> wasmtime::Result<Result<CompletionResponse, LlmError>> {
        let runtime = self.llm.as_ref()
            .ok_or_else(|| anyhow::anyhow!("LLM not configured for this module"))?;
        Ok(runtime.complete(req.into()).await)
    }

    async fn complete_stream(
        &mut self,
        req: CompletionRequest,
    ) -> wasmtime::Result<Result<Resource<CompletionStreamState>, LlmError>> {
        let runtime = self.llm.as_ref()
            .ok_or_else(|| anyhow::anyhow!("LLM not configured for this module"))?;
        match runtime.complete_stream(req.into()).await {
            Ok(rx) => {
                let handle = self.table.push(CompletionStreamState { rx, usage: None })?;
                Ok(Ok(handle))
            }
            Err(e) => Ok(Err(e)),
        }
    }
}

impl wruntime::llm::inference::HostCompletionStream for ModuleState {
    async fn next(
        &mut self,
        self_: Resource<CompletionStreamState>,
    ) -> wasmtime::Result<Result<Option<String>, LlmError>> {
        let state = self.table.get_mut(&self_)?;
        match state.rx.recv().await {
            Some(StreamEvent::Delta(text)) => Ok(Ok(Some(text))),
            Some(StreamEvent::Usage(u)) => {
                state.usage = Some(u);
                Ok(Ok(None))
            }
            None => Ok(Ok(None)),
        }
    }

    async fn usage(
        &mut self,
        self_: Resource<CompletionStreamState>,
    ) -> wasmtime::Result<Option<TokenUsage>> {
        let state = self.table.get(&self_)?;
        Ok(state.usage.clone())
    }

    fn drop(
        &mut self,
        self_: Resource<CompletionStreamState>,
    ) -> wasmtime::Result<()> {
        self.table.delete(self_)?;
        Ok(())
    }
}

pub fn add_to_linker<T, U>(
    linker: &mut wasmtime::component::Linker<T>,
    get: impl Fn(&mut T) -> &mut U + Send + Sync + Copy + 'static,
) -> wasmtime::Result<()>
where
    T: Send,
    U: wruntime::llm::inference::Host + wruntime::llm::inference::HostCompletionStream,
{
    wruntime::llm::inference::add_to_linker(linker, get)
}
```

### 5. Wire into `ModuleState`

In `wr-engine/src/state.rs`, add:

```rust
pub struct ModuleServices {
    // ... existing fields ...
    pub llm: Option<Arc<LlmRuntime>>,
}
```

### 6. Wire into the linker and module spawn

In `wr-engine/src/engine.rs`, add the linker call alongside db/tracing/blobstore:

```rust
wr_engine::llm::add_to_linker::<ModuleState, wasmtime::component::HasSelf<ModuleState>>(
    &mut linker,
    |s| s,
)?;
```

In `EngineRunner::new()`, construct the shared `LlmRuntime`:

```rust
let llm_client = config
    .llm
    .as_ref()
    .map(LlmRuntime::new)
    .transpose()?
    .map(Arc::new);
```

In `spawn_module()`, pass it to the module context:

```rust
let llm = if module_config.llm {
    self.llm_client.clone()
} else {
    None
};
```

### 7. Update SDK world

In `wr-sdk/wit/world.wit`, add the import:

```wit
import wruntime:llm/inference@0.1.0;
```

Vendor `llm.wit` into `wr-sdk/wit/deps/wruntime/llm.wit`.

### 8. Add SDK ergonomic layer (`wr-sdk/src/llm.rs`)

```rust
use crate::bindings::wruntime::llm::inference::*;

pub struct CompletionBuilder {
    model: String,
    messages: Vec<Message>,
    system: Option<String>,
    max_tokens: u32,
    temperature: Option<f32>,
    tools: Vec<ToolDef>,
}

impl CompletionBuilder {
    pub fn new(model: &str) -> Self {
        Self {
            model: model.into(),
            messages: vec![],
            system: None,
            max_tokens: 4096,
            temperature: None,
            tools: vec![],
        }
    }

    pub fn sonnet() -> Self { Self::new("claude-sonnet-4-6") }
    pub fn haiku() -> Self { Self::new("claude-haiku-4-5-20251001") }

    pub fn system(mut self, s: impl Into<String>) -> Self {
        self.system = Some(s.into()); self
    }
    pub fn user(mut self, content: impl Into<String>) -> Self {
        self.messages.push(Message { role: MessageRole::User, content: content.into() }); self
    }
    pub fn assistant(mut self, content: impl Into<String>) -> Self {
        self.messages.push(Message { role: MessageRole::Assistant, content: content.into() }); self
    }
    pub fn max_tokens(mut self, n: u32) -> Self { self.max_tokens = n; self }
    pub fn temperature(mut self, t: f32) -> Self { self.temperature = Some(t); self }
    pub fn tool(mut self, name: &str, description: &str, schema: &str) -> Self {
        self.tools.push(ToolDef {
            name: name.into(),
            description: description.into(),
            input_schema: schema.into(),
        });
        self
    }

    pub fn complete(self) -> Result<CompletionResponse, LlmError> {
        complete(&self.into_request())
    }

    pub fn stream(self) -> Result<CompletionStream, LlmError> {
        complete_stream(&self.into_request())
    }

    /// Send, expect text, return just the string.
    pub fn complete_text(self) -> Result<String, LlmError> {
        let resp = self.complete()?;
        match resp.completion {
            Completion::Text(s) => Ok(s),
            Completion::ToolCalls(_) => Err(LlmError::InvalidRequest(
                "expected text but got tool_use".into(),
            )),
        }
    }

    fn into_request(self) -> CompletionRequest {
        CompletionRequest {
            model: self.model,
            messages: self.messages,
            system: self.system,
            max_tokens: self.max_tokens,
            temperature: self.temperature,
            tools: self.tools,
        }
    }
}

/// Collect a stream into a single string.
pub fn collect_stream(stream: CompletionStream) -> Result<String, LlmError> {
    let mut buf = String::new();
    loop {
        match stream.next()? {
            Some(chunk) => buf.push_str(&chunk),
            None => return Ok(buf),
        }
    }
}
```

Add `pub mod llm;` to `wr-sdk/src/lib.rs`.

### 9. Add `reqwest` dependency

In `wr-engine/Cargo.toml`, add:

```toml
reqwest = { version = "0.12", features = ["json", "stream"] }
```

Note: if the engine already uses `hyper` directly for outbound HTTP, consider
using `hyper` + `hyper-util` instead of adding `reqwest`. The choice depends on
whether we want connection pooling and cookie handling (reqwest) or minimal
footprint (raw hyper). `reqwest` is simpler for JSON API calls with SSE.

### 10. Tests

Add test cases to `wr-tests/tests/integration_test.rs`:

- **Config validation**: module with `llm = true` but no `[llm]` section fails
- **Host binding smoke test**: construct a `ModuleState` with an `LlmRuntime`
  pointing at a mock HTTP server (use `wiremock` or `mockito`), call `complete`,
  verify the response mapping
- **Streaming test**: mock SSE endpoint, call `complete_stream`, drain the
  cursor, verify text and usage
- **Error mapping**: mock 429/401/500 responses, verify correct `LlmError`
  variants

### 11. Update documentation

Update the following files:

- `CLAUDE.md` — add LLM to the Architecture section, mention `llm.wit`
- `docs/configuration.md` — document `[llm]` section and `llm = true` module flag
- `docs/agents/api_reference.md` — add `wruntime:llm/inference` function signatures
- `docs/architecture.md` — add LLM host binding to the engine description

---

## File Changelist

| File | Change |
|---|---|
| `wit/llm.wit` | New — WIT interface definition |
| `wr-engine/src/config.rs` | Add `LlmConfig`, `llm` field on `EngineConfig` and `ModuleConfig`, validation |
| `wr-engine/src/llm.rs` | New — `LlmRuntime` HTTP client + WIT host binding impl |
| `wr-engine/src/lib.rs` | Add `pub mod llm` |
| `wr-engine/src/state.rs` | Add `llm: Option<Arc<LlmRuntime>>` to `ModuleServices` |
| `wr-engine/src/engine.rs` | Construct `LlmRuntime`, add linker call, pass to module context |
| `wr-engine/Cargo.toml` | Add `reqwest` dependency |
| `wr-sdk/wit/world.wit` | Add `import wruntime:llm/inference@0.1.0` |
| `wr-sdk/wit/deps/wruntime/llm.wit` | New — vendored copy for SDK |
| `wr-sdk/src/llm.rs` | New — `CompletionBuilder` ergonomic layer |
| `wr-sdk/src/lib.rs` | Add `pub mod llm` |
| `examples/config/engine.toml` | Add example `[llm]` section (commented out) |
| `wr-tests/tests/integration_test.rs` | Config validation + mock-based host binding tests |
| `CLAUDE.md` | Mention LLM host binding in Architecture |
| `docs/configuration.md` | Document `[llm]` config |
| `docs/agents/api_reference.md` | Add LLM function signatures |

---

## Design Notes

**Why a host binding instead of egress?**
Egress would work — modules could POST to `api.anthropic.com` through the proxy.
But that requires every module to carry the API key, implement the Messages API
protocol, handle SSE parsing, and manage retries. A host binding centralizes
all of this: one HTTP client, one API key, one place to enforce rate limits and
token ceilings.

**Why `reqwest` over raw `hyper`?**
The engine already uses `hyper` for inbound request handling, but `reqwest` is
significantly simpler for outbound JSON+SSE calls. The SSE stream can be read
via `reqwest::Response::bytes_stream()`. If the dependency is undesirable, the
alternative is `hyper` + manual JSON serialization + SSE line parsing.

**API key management**: The key is read from an environment variable at engine
startup (same pattern as database URLs). It never enters the WASM sandbox.
Future work could add per-module API key overrides or a key vault integration.

**Host-enforced `max_tokens_limit`**: The engine caps `max_tokens` before
forwarding to the API. This prevents a buggy or malicious module from requesting
unbounded generation.

**Streaming resource lifetime**: `CompletionStreamState` holds an mpsc receiver.
When the guest drops the resource handle, the receiver drops, which causes the
host-side sender task to terminate. No leaked connections.

**Provider abstraction**: The WIT interface is provider-agnostic (no
Anthropic-specific types). `LlmRuntime` maps to the Claude Messages API, but
the same WIT interface could back OpenAI, Bedrock, or a local model. The
`provider` config field exists to support this future extension without WIT
changes.

**What's NOT in v0.1**: Images/vision (would need `list<content-block>` instead
of `string` content), caching (`anthropic-beta` headers), batching, embeddings.
These can be added as the interface evolves.

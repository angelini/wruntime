# WASM Module Tracing Interface

Allow WASM modules to emit trace spans that appear as children of the host's
`engine.dispatch` span in the existing OpenTelemetry pipeline.

---

## Overview

The host already creates an `engine.dispatch` span per request (server.rs) and
exports traces via OTLP. Modules currently have no way to contribute spans of
their own. This plan adds a `wruntime:tracing/span` WIT interface that modules
import — following the exact same pattern as `wruntime:db/database`.

---

## Steps

### 1. Add `wit/tracing.wit`

Create `/wit/tracing.wit`:

```wit
package wruntime:tracing;

interface span {
    /// An active span. Ends when the resource is dropped.
    resource active-span {
        /// Record a key/value attribute on the span.
        set-attribute: func(key: string, value: string);

        /// Record an event (point-in-time annotation).
        record-event: func(name: string, attrs: list<tuple<string, string>>);

        /// Mark the span as failed with a message.
        set-error: func(message: string);
    }

    /// Start a new child span under the current request span.
    start: func(name: string) -> active-span;
}

world tracing-access {
    import span;
}
```

### 2. Update `wr-sdk/wit/world.wit`

Add the tracing import alongside the existing db import:

```wit
import wruntime:tracing/span;
```

Also copy `tracing.wit` into `wr-sdk/wit/deps/wruntime/tracing.wit` (mirroring
how `db.wit` is vendored under `deps/`).

### 3. Add `ModuleState::active_span` field

In `wr-engine/src/state.rs`, add a field to carry the request-level span into
host function calls:

```rust
pub struct ModuleState {
    // ... existing fields ...
    pub active_span: tracing::Span,
}
```

Populate it in `dispatch_request()` (engine.rs) when constructing `ModuleState`:

```rust
let state = ModuleState {
    // ... existing fields ...
    active_span: tracing::Span::current(),
};
```

`tracing::Span::current()` returns the `engine.dispatch` span because
`dispatch_request` is called from within the `.instrument(span)` future in
server.rs.

### 4. Add `SpanState` resource type

Create `wr-engine/src/tracing.rs`. This mirrors the structure of `db.rs`:

```rust
use wasmtime::component::Resource;
use crate::state::ModuleState;

pub struct SpanState {
    span: tracing::Span,
}

wasmtime::component::bindgen!({
    path: "../wit",
    world: "tracing-access",
    with: {
        "wruntime:tracing/span.active-span": SpanState,
    },
});

impl wruntime::tracing::span::Host for ModuleState {
    fn start(&mut self, name: String) -> wasmtime::Result<Resource<SpanState>> {
        let child = self.active_span.in_scope(|| {
            tracing::info_span!("module", otel.name = name.clone(), "wasm.span.name" = name)
        });
        child.follows_from(self.active_span.id());
        let handle = self.table.push(SpanState { span: child })?;
        Ok(handle)
    }
}

impl wruntime::tracing::span::HostActiveSpan for ModuleState {
    fn set_attribute(
        &mut self,
        self_: Resource<SpanState>,
        key: String,
        value: String,
    ) -> wasmtime::Result<()> {
        // tracing doesn't support dynamic field names post-creation;
        // emit a child event instead, which OTLP exporters record as a span event.
        let state = self.table.get(&self_)?;
        state.span.in_scope(|| {
            tracing::info!(key = key.as_str(), value = value.as_str(), "attribute");
        });
        Ok(())
    }

    fn record_event(
        &mut self,
        self_: Resource<SpanState>,
        name: String,
        attrs: Vec<(String, String)>,
    ) -> wasmtime::Result<()> {
        let state = self.table.get(&self_)?;
        state.span.in_scope(|| {
            tracing::info!(
                event = name.as_str(),
                attrs = ?attrs,
            );
        });
        Ok(())
    }

    fn set_error(&mut self, self_: Resource<SpanState>, message: String) -> wasmtime::Result<()> {
        let state = self.table.get(&self_)?;
        state.span.in_scope(|| {
            tracing::error!(
                "otel.status_code" = "ERROR",
                "exception.message" = message.as_str(),
            );
        });
        Ok(())
    }

    fn drop(&mut self, self_: Resource<SpanState>) -> wasmtime::Result<()> {
        self.table.delete(self_)?;
        // SpanState drops here → tracing::Span drops → span ends in OTLP
        Ok(())
    }
}

pub fn add_to_linker<T, U>(
    linker: &mut wasmtime::component::Linker<T>,
    get: impl Fn(&mut T) -> &mut U + Send + Sync + Copy + 'static,
) -> wasmtime::Result<()>
where
    T: Send,
    U: wruntime::tracing::span::Host + wruntime::tracing::span::HostActiveSpan,
{
    wruntime::tracing::span::add_to_linker(linker, get)
}
```

**Note on dynamic field names**: `tracing::Span::record()` only works for fields
declared at span creation time. The workaround above emits events into the span
scope; OTLP collectors attach these as span events, which is idiomatic OTel. If
named attributes are required on the span itself, an alternative is to use the
`opentelemetry` crate's API directly (bypassing the tracing bridge) and store an
`opentelemetry::trace::Span` in `SpanState`.

### 5. Wire into the linker (engine.rs)

Add to the linker setup block alongside the existing db call:

```rust
wr_engine::tracing::add_to_linker::<
    ModuleState,
    wasmtime::component::HasSelf<ModuleState>,
>(&mut linker, |s| s)?;
```

Add `mod tracing;` in `wr-engine/src/lib.rs` or `main.rs`.

### 6. Update SDK world (`wr-sdk/src/lib.rs`)

The `wit_bindgen::generate!` macro in wr-sdk picks up all imports declared in
`wr-sdk/wit/world.wit`. Adding the tracing import there is sufficient — the
generated `wr_sdk::bindings::wruntime::tracing::span` module appears
automatically. No Rust changes needed in the SDK crate itself.

### 7. Update module WIT worlds

Each module that wants tracing adds one line to its `wit/world.wit`:

```wit
import wruntime:tracing/span;
```

This is optional — modules that don't import it are unaffected.

### 8. Tests

Add cases to `wr-tests/tests/integration_test.rs`:

- A stub WASM module (or a test using the SDK directly via the host linker) that
  calls `span::start`, `set_attribute`, and `set_error`.
- Assert that no error is returned and the span handle is valid.
- If the test environment has an OTLP collector available, assert the span
  appears as a child of `engine.dispatch` in the trace output.

---

## File Changelist

| File | Change |
|---|---|
| `wit/tracing.wit` | New — WIT interface definition |
| `wr-sdk/wit/world.wit` | Add `import wruntime:tracing/span` |
| `wr-sdk/wit/deps/wruntime/tracing.wit` | New — vendored copy for SDK |
| `wr-engine/src/state.rs` | Add `active_span: tracing::Span` field to `ModuleState` |
| `wr-engine/src/engine.rs` | Populate `active_span` in `dispatch_request()`; add `add_to_linker` call |
| `wr-engine/src/tracing.rs` | New — `SpanState`, `bindgen!`, host trait impls |
| `wr-engine/src/lib.rs` | Add `pub mod tracing` |
| `examples/ecommerce/inventory/wit/world.wit` | Add tracing import (optional, for demo) |
| `examples/ecommerce/inventory/src/lib.rs` | Add spans to handlers (optional, for demo) |
| `wr-tests/tests/integration_test.rs` | New tracing test cases |

---

## Design Notes

**Why `tracing::Span` in `ModuleState` and not `Span::current()` directly in
host impls?**
The host functions run synchronously from wasmtime's call stack but the
`engine.dispatch` span is entered on the async task driving the request. Storing
the span in `ModuleState` at construction time (when we're definitely inside the
instrumented context) avoids any ambient-context race or missing-span surprises.

**Span lifetime**: `SpanState` holds a `tracing::Span`. When the module drops the
`active-span` resource (end of scope or explicit drop), wasmtime calls
`HostActiveSpan::drop`, which deletes it from the `ResourceTable`. The
`tracing::Span` is then dropped, which closes the span in the OTel pipeline.

**Module isolation**: Each `active-span` handle is scoped to one request's
`ResourceTable`. A module cannot access spans from other requests or other
modules.

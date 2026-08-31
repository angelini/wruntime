# Generated Contracts

Treat generators and generated artifacts as a fanout, not as isolated files.

```mermaid
flowchart TB
    subgraph control_proto["Control-plane protobuf fanout"]
        direction LR
        runtime_proto["Canonical source<br/>proto/wruntime.proto"] --> common_build["wr-common/build.rs"]
        common_build --> runtime_types["tonic/prost types<br/>generated in OUT_DIR"]
        runtime_types --> manager["Manager"]
        runtime_types --> proxy["Proxy"]
        runtime_types --> engine["Engine"]
        runtime_types --> cli["CLI"]
        runtime_types --> tests["Tests"]
    end

    subgraph host_abi["Host ABI fanout"]
        direction LR
        root_wit["Canonical source<br/>wit/*.wit"] --> host_bindings["Engine async<br/>host bindings"]
        root_wit --> sdk_bindings["wr-sdk guest<br/>bindings"]
        root_wit --> wit_mirrors["wr-sdk/wit/deps<br/>mirrors"]
        wit_mirrors --> guest_worlds["Guest component<br/>worlds"]
        guest_worlds --> wasm_tests["Split WASM<br/>host tests"]
        root_wit -.->|"usage or semantics change"| api_guide["Guest API guide"]
    end

    subgraph guest_schema["Guest schema fanout"]
        direction LR
        guest_proto["Canonical source<br/>guest, example, or test .proto"] --> generators["prost-build<br/>and wr-build"]
        generators --> generated_rust["Rust generated<br/>in OUT_DIR"]
        guest_proto --> protoc["protoc with imports"]
        protoc --> descriptors["Checked-in .binpb<br/>descriptor"]
        descriptors --> schema_registration["Engine schema<br/>registration"]
    end
```

## Rules

- Never edit generated Rust under Cargo `OUT_DIR`; change the proto, WIT, or generator.
- Root [`wit/`](../../../wit/) is canonical. Synchronize matching files under [`wr-sdk/wit/deps/`](../../../wr-sdk/wit/deps/) in the same change.
- Regenerate every affected checked-in `.binpb` descriptor with imports included.
- Keep guest `world.wit` imports aligned with enabled module capabilities.
- `wr-build` emits service `_router` and `_handle` helpers; worker clients are generated only for services whose names end in `WorkerService`.
- SDK, WIT, build-generator, or host-binding changes require focused `just test-wasm-one <target>` where possible and full `just test-wasm` before completion.
- Update the guest API guide when preferred usage or guest-visible semantics change. Exact signatures stay in Rust/WIT source.
- Manager migrations under `wr-manager/migrations/` modify control-plane state. Module migrations are trusted guest-owned SQL, run at engine startup with target-database admin credentials and the module schema as the default `search_path`, and use a separate history/cancellation-safe locking policy.

## Review checklist

1. Identify the canonical source.
2. Find every generated or mirrored consumer.
3. Regenerate rather than hand-edit outputs.
4. Update source-owned tests and downstream fixtures.
5. Run the focused validation in [validation.md](validation.md).
6. Update the documentation owner in [documentation_ownership.md](documentation_ownership.md).

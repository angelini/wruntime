# Local multi-node topology

This example runs two proxy nodes and three engines on one machine. A minimal Echo service runs only on Node B, so invoking it through Node A proves the request traverses the mTLS peer-proxy connection before reaching the guest.

## Run

Start the shared Postgres development infrastructure once:

```bash
just dev-up
```

Then start the complete topology:

```bash
just multi-node
```

Press `Ctrl-C` to stop the manager, both proxies, and all three engines. The shared Docker development infrastructure remains running; stop it separately with `just dev-down`.

For a non-interactive check that starts the topology, verifies a cross-node Echo request, and exits:

```bash
just multi-node-inline
```

## Ports

| Process | Local HTTP/control | Peer mTLS |
|---|---|---|
| Manager | `9000` (mTLS gRPC) | — |
| Node A proxy | `9001` / `9002` | `9443` |
| Node A engines | `9100`, `9101` | — |
| Node B proxy | `9003` / `9004` | `9444` |
| Node B engine | `9200` | — |

A successful startup reports three healthy engines and `multinode.echo` on Node B, then prints `echo response: hello across nodes`. The same peer-routing behavior is also covered in-process by `wr-tests/tests/cross_node_test.rs`.

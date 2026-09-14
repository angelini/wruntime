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

The checked-in configs use the conventional 9000/9100/9200/9443 ranges as
templates. The runner renders them into the current worktree's persistent port
block before startup. `just dev-up` prints the stack endpoints; the runner's
startup summary prints the resolved manager, proxy, peer, and engine ports.

A successful startup reports three healthy engines and `multinode.echo` on Node B, then prints `echo response: hello across nodes`. The same peer-routing behavior is also covered in-process by `wr-tests/tests/cross_node_test.rs`.

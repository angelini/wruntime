# Testing

```bash
just test                    # all tests
just test-integration        # wr-tests crate only
just test-one <test_name>    # single test by name
just test-db                 # integration tests with a local Postgres instance
```

The `wr-tests` crate contains integration tests that spin up in-process instances of all three services on random ports — no external processes or files required:

- Manager RPC coverage (register, deregister, heartbeat, routing rules, metrics)
- Proxy routing end-to-end with a stub engine
- Schema validation: invalid protobuf bodies rejected with `400`; unknown method paths rejected with `404`; missing schema returns `503`
- All three example TOML files parse without error
- Version routing: `x-wr-version` header routes to the correct instance; no header routes to the highest semver
- Returns 503 when the requested version has no healthy instance
- Load balancing: requests distributed across multiple instances of the same `(module, version)`
- Failover: deregistering an instance immediately redirects traffic to remaining healthy instances
- Full failure: 503 when all instances are unhealthy
- Cross-node routing: request originating on Node A is relayed to Node B's proxy when the destination engine lives on Node B; schema validation is skipped on the second hop (`x-wr-via-proxy`)

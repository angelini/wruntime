# Investigation: Gossip Protocol Scaling — Chitchat Between Managers

Engines heartbeat through proxies to managers. Managers share heartbeat state via chitchat (UDP gossip). This investigation traces how much data flows through gossip, identifies the scaling ceiling, and documents the changes made to improve it.

## Gossip Data Model

Each manager sets key-value pairs on its own chitchat node state. After removing module-level health keys (`mh/`) and keeping only engine heartbeats:

```
hb/{engine_id} = unix_timestamp_millis
```

One key per engine, per manager that receives that engine's heartbeat. Values are 13-byte timestamp strings. The merged view across all peers uses max-timestamp-wins semantics.

## Heartbeat Flow

```
Engine (3s) → Proxy NodeAgent (3s flush) → sticky Manager (120s affinity) → chitchat gossip
                                                 │
                                         set hb/{engine_id}
                                         on self_node_state
                                                 │
                                         gossip to K-1 peers
                                         (500ms interval, UDP)
```

### Sticky Affinity

`ManagerDiscovery` caches a single manager connection for 120 seconds (`wr-common/src/discovery.rs`). All engines on a proxy heartbeat to the same manager within that window. On connection failure, affinity clears and the next call picks a new random manager.

This keeps engine-to-manager mapping stable. Without it, keys scatter across all managers and accumulate (E×K total keys instead of ~E).

### Stale Key Cleanup

The monitor loop (`wr-manager/src/state.rs`) calls `cleanup_stale_heartbeats()` (`wr-manager/src/cluster.rs`) every 5 seconds. Any `hb/` key on self_node_state whose timestamp exceeds the 10-second timeout is deleted. This catches keys left behind when engines migrate to a different manager after affinity rotation.

## Timing Parameters

```
Engine heartbeat interval     3s
Proxy heartbeat flush         3s
Manager monitor interval      5s
Manager heartbeat timeout     10s
Gossip interval               500ms
Proxy routing table TTL       2s
Sticky affinity duration      120s
Stale key cleanup threshold   10s (= heartbeat timeout)
```

Timeout-to-cycle ratio: 10s ÷ 3s ≈ 3.3 missed cycles before an engine is marked unhealthy.

## Gossip Protocol Mechanics

Chitchat operates over UDP (~1400 bytes usable payload per datagram):

1. Each gossip round (every 500ms), a node picks one random peer
2. Sends a digest: `(node_id, generation_id, max_version)` per known node (~70 bytes each)
3. Peer responds with key-values newer than the digest indicated
4. Each delta entry is ~50 bytes (key + value + version + overhead)
5. One packet carries ~28 delta entries

Outbound throughput per manager: **~56 deltas/second** (28 deltas × 2 rounds/s).

## Key Distribution (Steady State)

With sticky affinity, engines distribute roughly evenly across K managers. Each manager holds ~E/K keys plus a small transient tail from affinity rotation (cleaned within 10s).

| Engines | Managers | Keys/manager | Total gossip keys |
|---------|----------|-------------|-------------------|
| 50      | 3        | ~17         | ~50               |
| 100     | 3        | ~33         | ~100              |
| 200     | 3        | ~67         | ~200              |
| 500     | 3        | ~167        | ~500              |
| 100     | 5        | ~20         | ~100              |
| 200     | 5        | ~40         | ~200              |
| 100     | 10       | ~10         | ~100              |

## Convergence Analysis

Every 3-second heartbeat cycle, E deltas are produced across K managers. Each manager must propagate its E/K deltas to K-1 peers.

Convergence time ≈ `(E/K / 28) × (K-1) × 500ms`

| Engines | Managers | Convergence | vs Timeout (10s) | vs Monitor (5s) |
|---------|----------|-------------|-------------------|-----------------|
| 50      | 3        | ~1.5s       | 8.5s margin       | 3.5s margin     |
| 100     | 3        | ~2.5s       | 7.5s margin       | 2.5s margin     |
| 200     | 3        | ~5s         | 5s margin         | at limit        |
| 300     | 3        | ~7s         | 3s margin         | **exceeds**     |
| 500     | 3        | ~12s        | **exceeds**       | **exceeds**     |
| 100     | 5        | ~3s         | 7s margin         | 2s margin       |
| 200     | 5        | ~6s         | 4s margin         | **exceeds**     |
| 300     | 5        | ~9s         | 1s margin         | **exceeds**     |
| 100     | 10       | ~5s         | 5s margin         | at limit        |
| 200     | 10       | ~9s         | 1s margin         | **exceeds**     |

### Two Thresholds

**Monitor flapping** (convergence > 5s): A manager hasn't received gossiped heartbeats by the time the monitor runs. Health status oscillates between healthy/unhealthy across monitor cycles. This is the **practical ceiling**.

**False unhealthy marks** (convergence > 10s): Gossip can't propagate a heartbeat before the timeout expires. Managers that didn't receive the heartbeat directly will mark the engine unhealthy even though it's alive. This is the **hard ceiling**.

## Scale Ceilings

| | 3 managers | 5 managers | 10 managers |
|---|---|---|---|
| No flapping (convergence < 5s) | ~140 engines | ~80 | ~35 |
| Reliable (convergence < 10s) | ~300 engines | ~150 | ~75 |
| Broken (false unhealthy) | >330 engines | >170 | >85 |

Managers are the multiplier — each additional manager adds another peer that needs every delta. Doubling managers roughly halves the engine ceiling.

## Digest Overhead

The gossip digest scales with manager count, not engine count:

| Managers | Digest size |
|----------|-------------|
| 3        | ~240 bytes  |
| 10       | ~800 bytes  |
| 50       | ~4KB (exceeds UDP MTU) |

Not a concern for realistic manager counts.

## Full-State Sync on Manager Restart

When a manager restarts, it must catch up from peers. During this window it has no heartbeat data and will mark engines unhealthy (`unwrap_or(false)`). Other managers maintain correct state during this period.

| Engines | Full state size | Packets | Sync time |
|---------|----------------|---------|-----------|
| 100     | ~5KB           | 4       | ~2s       |
| 200     | ~10KB          | 7       | ~3.5s     |
| 500     | ~25KB          | 18      | ~9s       |

## Routing Table Poll Load

Proxies poll a manager for the routing table every 2 seconds. With sticky affinity this load concentrates on fewer managers:

| Proxies | Requests/second (total) |
|---------|------------------------|
| 10      | 5/s                    |
| 50      | 25/s                   |
| 100     | 50/s                   |

Each request is a single Postgres read (`get_routing_table`). Lightweight until hundreds of proxies.

## What Was Changed

| Change | File | Effect |
|--------|------|--------|
| Removed `mh/` keys from gossip | `wr-manager/src/cluster.rs` | Keys drop from E×(1+M) to E |
| Removed `set_module_health` calls | `wr-manager/src/service.rs` | No per-module gossip writes on heartbeat |
| Derive module health from engine liveness | `wr-manager/src/state.rs` | Module health = engine heartbeat freshness |
| Sticky manager affinity (120s) | `wr-common/src/discovery.rs` | Keys stay on one manager, not scattered |
| Clear affinity on failure | `wr-proxy/src/node_service.rs` | Fast failover to new manager |
| Stale key cleanup | `wr-manager/src/cluster.rs` | Keys cleaned when timestamp > timeout |
| Missing heartbeat = unhealthy | `wr-manager/src/state.rs` | `unwrap_or(false)` instead of startup grace |
| Reduced deletion grace period | `wr-manager/src/cluster.rs` | 3600s → 120s for deleted key propagation |

## Conclusion

The gossip protocol is appropriate for clusters up to **~100-200 engines with 3 managers**. Beyond that, the 10-second timeout leaves insufficient convergence budget for UDP gossip at 500ms intervals. Scaling past this ceiling would require either relaxing the timeout, increasing gossip frequency, or replacing gossip with a direct-push or shared-storage heartbeat mechanism.

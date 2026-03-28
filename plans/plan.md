# Wruntime

This is a design document explaining the Wruntime application and how it should be built.

The goal of `wruntime` is to network together multiple WASM modules. It is a runtime that will
take WASM modules and connect them for internal HTTP traffic.

There are three components to `wruntime`:

- A management service called `wr-manager`
- An engine that runs multiple WASM modules in one process called `wr-engine`
- A proxy which receives requests from `wr-engine`s and forwards them to other engines called `wr-proxy`

## Build instructions

This app should be built in Rust using `wasmtime` to run and manage WASM modules and `tower` to
connect the services.

## Components

### `wr-manager`

In a typical deployment there will only be one instance of the manager running. It has an API that allows
adding new engines to the network or removing existing ones. It can list and report the status of engines
as well as metrics about their network traffic, response times and errors.

### `wr-proxy`

There can be multiple proxies which manage a local routing table. They run a service which receives traffic
from engines and routes it to the appropriate destination engine. Routing rules are managed by `wr-manager`
but are cached locally on the proxy.

`wr-proxy` should intercept all HTTP traffic ingressing or egressing from a WASM module running in an engine.

It should keep track of request metrics, performance and failures.

### `wr-engine`

This is a process that will run and manage multiple WASM modules. These modules will be configured with
specific networking egress and ingress rules which will be enforced by the `wr-proxy`.

An engine runs specific versions of modules and will register itself with a `wr-manager` when added to
the network.

Every module will be configured with a strict HTTP schema and the proxy and manager will ensure
that other WASM modules only send appropriate requests to these modules.

## Configuration

Each engine is configured using a TOML configuration file. This configuration file will include all
modules running in the engine, a reference to their HTTP schema, and the specific version of the module.

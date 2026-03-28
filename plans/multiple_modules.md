# Multiple Modules

This project currently supports multiple WASM modules running in parallel across various engines.

However, it only supports 1 copy of a named module. At this point we can only have 1 `InventoryService`.

In order to allow this tool to scale up we want to support multiple instances of the same module as well
as multiple versions of one module.

## Requirements

- All module versions need to include a version number
- `wr-proxy` routing needs to account for versions:
  - if no version is provided in a request, via a header, it should default to routing to the latest version
  - if a version is provided it should ensure the request gets routed to a service running that explicit version
  - if there are no running Services with that version an HTTP error should be returned
- If two instances of the same module and same version are running across 1 or more engines:
  - network traffic should be load balanced across those services
  - if one service becomes unhealthy all traffic should be routed to the remaining healthy services

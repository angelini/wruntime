/// A `prost_build::ServiceGenerator` that emits a typed gRPC client struct for
/// each service.  Add it to your `build.rs` to get zero-boilerplate RPC calls
/// via `wr_sdk::http::http_rpc`.
///
/// # Example `build.rs`
///
/// ```rust,no_run
/// prost_build::Config::new()
///     .service_generator(Box::new(wr_build::WrClientGenerator))
///     .compile_protos(&["../schemas/inventory.proto"], &["../schemas"])
///     .unwrap();
/// ```
///
/// For a service `InventoryService` in package `ecommerce` with a `Seed` RPC,
/// the generator emits:
///
/// ```rust,ignore
/// pub struct InventoryServiceClient { authority: String }
///
/// impl InventoryServiceClient {
///     pub fn new(authority: impl Into<String>) -> Self { ... }
///     pub fn seed(&self, req: SeedRequest) -> Result<SeedResponse, String> { ... }
/// }
/// ```
///
/// The RPC path is derived at runtime from the authority (e.g. `ecommerce.inventory`),
/// producing paths like `/ecommerce.inventory/Seed`.  This keeps the path prefix
/// consistent with the HTTP hostname used for inter-module addressing.
pub struct WrClientGenerator;

impl prost_build::ServiceGenerator for WrClientGenerator {
    fn generate(&mut self, service: prost_build::Service, buf: &mut String) {
        let struct_name = format!("{}Client", service.name);

        // struct definition
        buf.push_str(&format!("pub struct {struct_name} {{\n"));
        buf.push_str("    authority: String,\n");
        buf.push_str("}\n\n");

        // impl block
        buf.push_str(&format!("impl {struct_name} {{\n"));
        buf.push_str("    pub fn new(authority: impl Into<String>) -> Self {\n");
        buf.push_str("        Self { authority: authority.into() }\n");
        buf.push_str("    }\n");

        for method in &service.methods {
            let proto_name = &method.proto_name;
            let method_name = &method.name;
            let input = &method.input_type;
            let output = &method.output_type;

            buf.push_str(&format!(
                "\n    pub fn {method_name}(&self, req: {input}) -> Result<{output}, String> {{\n"
            ));
            buf.push_str("        let body = prost::Message::encode_to_vec(&req);\n");
            // Path is /{authority}/{MethodName} — e.g. /ecommerce.inventory/Seed.
            // This mirrors the HTTP hostname format so both use the same namespace.module identifier.
            buf.push_str(&format!(
                "        let path = format!(\"/{{}}/{{}}\", self.authority, \"{proto_name}\");\n"
            ));
            buf.push_str(
                "        let (status, resp_bytes) = wr_sdk::http::http_rpc(&self.authority, &path, &body)?;\n",
            );
            buf.push_str("        if status != 200 {\n");
            buf.push_str("            return Err(format!(\"rpc error: HTTP {status}\"));\n");
            buf.push_str("        }\n");
            buf.push_str(
                "        prost::Message::decode(resp_bytes.as_slice()).map_err(|e| e.to_string())\n",
            );
            buf.push_str("    }\n");
        }

        buf.push_str("}\n");
    }
}

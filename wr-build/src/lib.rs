use proc_macro2::{Ident, Span, TokenStream};
use quote::{format_ident, quote};

fn to_snake(s: &str) -> String {
    let mut out = String::new();
    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

/// Create an identifier, using raw syntax (`r#name`) for Rust keywords.
fn method_ident(name: &str) -> Ident {
    syn::parse_str::<Ident>(name).unwrap_or_else(|_| Ident::new_raw(name, Span::call_site()))
}

/// Format a token stream into clean Rust source via `prettyplease`.
fn pretty(tokens: TokenStream) -> String {
    let file = syn::parse2(tokens).expect("generated tokens must be valid syntax");
    prettyplease::unparse(&file)
}

/// Parse a prost type string (e.g. `"SeedRequest"`) into a `syn::Type`.
fn parse_type(s: &str) -> syn::Type {
    syn::parse_str(s).unwrap_or_else(|_| panic!("invalid type: {s}"))
}

// ── WrCombinedGenerator ──────────────────────────────────────────────────────

/// Wraps two `ServiceGenerator`s so both run on every service definition.
pub struct WrCombinedGenerator<A, B> {
    a: A,
    b: B,
}

impl<A, B> WrCombinedGenerator<A, B> {
    pub fn new(a: A, b: B) -> Self {
        Self { a, b }
    }
}

impl<A: prost_build::ServiceGenerator, B: prost_build::ServiceGenerator>
    prost_build::ServiceGenerator for WrCombinedGenerator<A, B>
{
    fn generate(&mut self, service: prost_build::Service, buf: &mut String) {
        self.a.generate(service.clone(), buf);
        self.b.generate(service, buf);
    }
}

// ── WrServiceGenerator ──────────────────────────────────────────────────────

/// A `prost_build::ServiceGenerator` that emits a trait, router function, and
/// default `ServiceGuest` handler for each service.
///
/// For a service `InventoryService` in package `ecommerce` with a `Seed` RPC,
/// the generator emits:
///
/// ```rust,ignore
/// pub trait InventoryService {
///     fn seed(&self, req: SeedRequest) -> Result<SeedResponse, wr_sdk::ServiceError>;
/// }
///
/// pub fn inventory_service_router<T: InventoryService>(
///     svc: &T, path: &str, body: &[u8],
/// ) -> (u16, Vec<u8>) { ... }
///
/// /// Default `ServiceGuest` handler that routes to the service trait impl.
/// pub fn inventory_service_handle<T: InventoryService>(
///     svc: &T,
///     request: wr_sdk::bindings::wasi::http::types::IncomingRequest,
///     response_out: wr_sdk::bindings::wasi::http::types::ResponseOutparam,
/// ) { ... }
/// ```
pub struct WrServiceGenerator;

impl prost_build::ServiceGenerator for WrServiceGenerator {
    fn generate(&mut self, service: prost_build::Service, buf: &mut String) {
        let trait_ident = format_ident!("{}", service.name);
        let router_ident = format_ident!("{}_router", to_snake(&service.name));
        let handle_ident = format_ident!("{}_handle", to_snake(&service.name));
        let service_snake = to_snake(&service.name)
            .trim_end_matches("_service")
            .to_string();

        // ── trait methods ──
        let trait_methods: Vec<_> = service
            .methods
            .iter()
            .map(|m| {
                let name = method_ident(&m.name);
                let input = parse_type(&m.input_type);
                let output = parse_type(&m.output_type);
                quote! {
                    fn #name(&self, req: #input) -> Result<#output, wr_sdk::ServiceError>;
                }
            })
            .collect();

        // ── match arms ──
        let match_arms: Vec<_> = service
            .methods
            .iter()
            .map(|m| {
                let route = format!("/{}.{}/{}", service.package, service_snake, m.proto_name);
                let name = method_ident(&m.name);
                let input = parse_type(&m.input_type);
                quote! {
                    #route => {
                        let req = match <#input as prost::Message>::decode(body) {
                            Ok(r) => r,
                            Err(e) => {
                                return (
                                    400,
                                    format!("{{\"error\":\"decode: {e}\"}}").into_bytes(),
                                )
                            }
                        };
                        match svc.#name(req) {
                            Ok(resp) => (200, prost::Message::encode_to_vec(&resp)),
                            Err(e) => (
                                e.status,
                                format!("{{\"error\":\"{}\"}}", e.message).into_bytes(),
                            ),
                        }
                    }
                }
            })
            .collect();

        let tokens = quote! {
            pub trait #trait_ident {
                #(#trait_methods)*
            }

            pub fn #router_ident<T: #trait_ident>(
                svc: &T,
                path: &str,
                body: &[u8],
            ) -> (u16, Vec<u8>) {
                match path {
                    #(#match_arms)*
                    _ => (
                        404,
                        format!("{{\"error\":\"no handler for {}\"}}", path).into_bytes(),
                    ),
                }
            }

            /// Default `ServiceGuest` handler that reads the request body,
            /// routes to the service trait impl, and sends the response.
            pub fn #handle_ident<T: #trait_ident>(
                svc: &T,
                request: wr_sdk::bindings::wasi::http::types::IncomingRequest,
                response_out: wr_sdk::bindings::wasi::http::types::ResponseOutparam,
            ) {
                let path = request.path_with_query().unwrap_or_default();
                let body = wr_sdk::io::read_body(request.consume().unwrap());
                let (status, resp) = #router_ident(svc, &path, &body);
                wr_sdk::io::send_response(response_out, status, resp);
            }
        };

        buf.push_str(&pretty(tokens));
    }
}

// ── WrClientGenerator ───────────────────────────────────────────────────────

/// A `prost_build::ServiceGenerator` that emits a typed gRPC client struct for
/// each service.  Add it to your `build.rs` to get zero-boilerplate RPC calls
/// via `wr_sdk::http::http_request`.
///
/// For a service `InventoryService` in package `ecommerce` with a `Seed` RPC,
/// the generator emits:
///
/// ```rust,ignore
/// pub struct InventoryServiceClient { authority: String }
///
/// impl InventoryServiceClient {
///     pub fn new(authority: impl Into<String>) -> Self { ... }
///     pub fn seed(&self, req: SeedRequest) -> Result<SeedResponse, wr_sdk::http::HttpError> { ... }
/// }
/// ```
///
/// The RPC path is derived from the proto method name, producing paths like `/Seed`.
/// The authority (e.g. `ecommerce.inventory`) is used only as the HTTP host.
pub struct WrClientGenerator;

impl prost_build::ServiceGenerator for WrClientGenerator {
    fn generate(&mut self, service: prost_build::Service, buf: &mut String) {
        // Skip worker services — those are handled by WrWorkerClientGenerator.
        if service.name.ends_with("WorkerService") {
            return;
        }
        let struct_ident = format_ident!("{}Client", service.name);

        let methods: Vec<_> = service
            .methods
            .iter()
            .map(|m| {
                let method_ident = method_ident(&m.name);
                let input = parse_type(&m.input_type);
                let output = parse_type(&m.output_type);
                let proto_name = &m.proto_name;
                quote! {
                    pub fn #method_ident(&self, req: #input) -> Result<#output, wr_sdk::http::HttpError> {
                        let body = prost::Message::encode_to_vec(&req);
                        let path = format!("/{}/{}", self.authority, #proto_name);
                        wr_sdk::http::http_request(&wr_sdk::http::HttpRequest {
                            authority: &self.authority,
                            path: &path,
                            method: wr_sdk::http::Method::Post,
                            headers: &[("content-type", b"application/x-protobuf" as &[u8])],
                            body: &body,
                        })?
                        .error_for_status()?
                        .decode()
                    }
                }
            })
            .collect();

        let tokens = quote! {
            pub struct #struct_ident {
                authority: String,
            }

            impl #struct_ident {
                pub fn new(authority: impl Into<String>) -> Self {
                    Self {
                        authority: authority.into(),
                    }
                }

                #(#methods)*
            }
        };

        buf.push_str(&pretty(tokens));
    }
}

// ── WrWorkerClientGenerator ────────────────────────────────────────────────

/// A `prost_build::ServiceGenerator` that emits typed job submission clients
/// for worker modules.  Each RPC method becomes a function that serializes the
/// request, submits a job via `wr_sdk::jobs::submit_job`, and returns the job_id.
///
/// For a service `TaskWorker` with RPC `ProcessTask`:
///
/// ```rust,ignore
/// pub struct TaskWorkerClient { authority: String }
///
/// impl TaskWorkerClient {
///     pub fn new(authority: impl Into<String>) -> Self { ... }
///     pub fn process_task(&self, req: ProcessTaskRequest) -> Result<String, wr_sdk::http::HttpError> { ... }
/// }
/// ```
pub struct WrWorkerClientGenerator;

impl prost_build::ServiceGenerator for WrWorkerClientGenerator {
    fn generate(&mut self, service: prost_build::Service, buf: &mut String) {
        // Only generate for worker services.
        if !service.name.ends_with("WorkerService") {
            return;
        }
        let struct_ident = format_ident!("{}Client", service.name);
        let service_snake = to_snake(&service.name)
            .trim_end_matches("_service")
            .to_string();

        let methods: Vec<_> = service
            .methods
            .iter()
            .map(|m| {
                let method_name = method_ident(&m.name);
                let input = parse_type(&m.input_type);
                let job_type = format!("/{}.{}/{}", service.package, service_snake, m.proto_name);
                quote! {
                    pub fn #method_name(&self, req: #input) -> Result<String, wr_sdk::http::HttpError> {
                        let payload = prost::Message::encode_to_vec(&req);
                        wr_sdk::jobs::submit_job(&self.authority, #job_type, &payload)
                    }
                }
            })
            .collect();

        let tokens = quote! {
            pub struct #struct_ident {
                authority: String,
            }

            impl #struct_ident {
                pub fn new(authority: impl Into<String>) -> Self {
                    Self {
                        authority: authority.into(),
                    }
                }

                #(#methods)*
            }
        };

        buf.push_str(&pretty(tokens));
    }
}

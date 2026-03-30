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

/// A `prost_build::ServiceGenerator` that emits a trait and router function for
/// each service, enabling typed server-side handlers.
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
/// ```
pub struct WrServiceGenerator;

impl prost_build::ServiceGenerator for WrServiceGenerator {
    fn generate(&mut self, service: prost_build::Service, buf: &mut String) {
        let trait_ident = format_ident!("{}", service.name);
        let router_ident = format_ident!("{}_router", to_snake(&service.name));
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
        };

        buf.push_str(&pretty(tokens));
    }
}

// ── WrClientGenerator ───────────────────────────────────────────────────────

/// A `prost_build::ServiceGenerator` that emits a typed gRPC client struct for
/// each service.  Add it to your `build.rs` to get zero-boilerplate RPC calls
/// via `wr_sdk::http::http_rpc`.
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
/// The RPC path is derived from the proto method name, producing paths like `/Seed`.
/// The authority (e.g. `ecommerce.inventory`) is used only as the HTTP host.
pub struct WrClientGenerator;

impl prost_build::ServiceGenerator for WrClientGenerator {
    fn generate(&mut self, service: prost_build::Service, buf: &mut String) {
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
                    pub fn #method_ident(&self, req: #input) -> Result<#output, String> {
                        let body = prost::Message::encode_to_vec(&req);
                        let path = format!("{}/{}", self.authority, #proto_name);
                        let (status, resp_bytes) =
                            wr_sdk::http::http_rpc(&self.authority, &path, &body)?;
                        if status != 200 {
                            return Err(format!("rpc error: HTTP {status}"));
                        }
                        prost::Message::decode(resp_bytes.as_slice())
                            .map_err(|e| e.to_string())
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

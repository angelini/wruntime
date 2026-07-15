use std::convert::Infallible;
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Result};
use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::server::conn::http2;
use hyper_util::rt::{TokioExecutor, TokioIo};
use prost::Message as _;
use prost_types::{
    field_descriptor_proto::{Label, Type},
    DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
    MethodDescriptorProto, ServiceDescriptorProto,
};
use tokio::net::TcpListener;
use tokio::sync::{oneshot, OnceCell};
use wasmtime::component::Component;
use wasmtime::Engine;
use wasmtime_wasi_http::p2::bindings::ProxyPre;

use super::db::{ModuleServices, ModuleState};
use super::proxy::http_pool;

pub fn minimal_file_descriptor_set() -> Vec<u8> {
    let req_msg = DescriptorProto {
        name: Some("PingRequest".into()),
        field: vec![FieldDescriptorProto {
            name: Some("message".into()),
            number: Some(1),
            label: Some(Label::Optional as i32),
            r#type: Some(Type::String as i32),
            json_name: Some("message".into()),
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp_msg = DescriptorProto {
        name: Some("PingResponse".into()),
        ..Default::default()
    };
    let service = ServiceDescriptorProto {
        name: Some("PingService".into()),
        method: vec![MethodDescriptorProto {
            name: Some("Ping".into()),
            input_type: Some(".test.PingRequest".into()),
            output_type: Some(".test.PingResponse".into()),
            ..Default::default()
        }],
        ..Default::default()
    };
    let file = FileDescriptorProto {
        name: Some("test.proto".into()),
        package: Some("test".into()),
        message_type: vec![req_msg, resp_msg],
        service: vec![service],
        syntax: Some("proto3".into()),
        ..Default::default()
    };
    FileDescriptorSet { file: vec![file] }.encode_to_vec()
}

/// A valid protobuf encoding of `PingRequest { message: "hello" }`.
/// Field 1, wire type 2 (length-delimited), value = "hello".
pub fn valid_ping_request() -> Bytes {
    // tag = (1 << 3) | 2 = 0x0a, varint length 5, then "hello"
    Bytes::from_static(b"\x0a\x05hello")
}

/// Bytes that are not valid protobuf (truncated varint).
pub fn invalid_protobuf() -> Bytes {
    Bytes::from_static(&[0xFF])
}

pub const DB_GUEST_WASM: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/guests/db-guest/target/wasm32-wasip2/debug/db_guest.wasm"
);
pub const TRACING_GUEST_WASM: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/guests/tracing-guest/target/wasm32-wasip2/debug/tracing_guest.wasm"
);
pub const BLOBSTORE_GUEST_WASM: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/guests/blobstore-guest/target/wasm32-wasip2/debug/blobstore_guest.wasm"
);
pub const LLM_GUEST_WASM: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/guests/llm-guest/target/wasm32-wasip2/debug/llm_guest.wasm"
);
pub const HTTP_GUEST_WASM: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/guests/http-guest/target/wasm32-wasip2/debug/http_guest.wasm"
);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum TestGuest {
    Db,
    Tracing,
    Blobstore,
    Llm,
    Http,
}

impl TestGuest {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Db => "db-guest",
            Self::Tracing => "tracing-guest",
            Self::Blobstore => "blobstore-guest",
            Self::Llm => "llm-guest",
            Self::Http => "http-guest",
        }
    }

    pub const fn wasm_path(self) -> &'static str {
        match self {
            Self::Db => DB_GUEST_WASM,
            Self::Tracing => TRACING_GUEST_WASM,
            Self::Blobstore => BLOBSTORE_GUEST_WASM,
            Self::Llm => LLM_GUEST_WASM,
            Self::Http => HTTP_GUEST_WASM,
        }
    }

    const fn route_prefix(self) -> &'static str {
        match self {
            Self::Db => "/test.DbTestService",
            Self::Tracing => "/test.TracingTestService",
            Self::Blobstore => "/test.BlobstoreTestService",
            Self::Llm => "/test.LlmTestService",
            Self::Http => "/test.HttpTestService",
        }
    }

    pub fn skip_if_missing(self) -> bool {
        if !Path::new(self.wasm_path()).exists() {
            eprintln!(
                "SKIP: {} WASM not built — run `just build-test-guests`",
                self.label()
            );
            return true;
        }
        false
    }
}

static DB_GUEST_PRE: OnceCell<(Arc<Engine>, Arc<ProxyPre<ModuleState>>)> = OnceCell::const_new();
static TRACING_GUEST_PRE: OnceCell<(Arc<Engine>, Arc<ProxyPre<ModuleState>>)> =
    OnceCell::const_new();
static BLOBSTORE_GUEST_PRE: OnceCell<(Arc<Engine>, Arc<ProxyPre<ModuleState>>)> =
    OnceCell::const_new();
static LLM_GUEST_PRE: OnceCell<(Arc<Engine>, Arc<ProxyPre<ModuleState>>)> = OnceCell::const_new();
static HTTP_GUEST_PRE: OnceCell<(Arc<Engine>, Arc<ProxyPre<ModuleState>>)> = OnceCell::const_new();

async fn cached_guest_pre(guest: TestGuest) -> Result<(Arc<Engine>, Arc<ProxyPre<ModuleState>>)> {
    let cell = match guest {
        TestGuest::Db => &DB_GUEST_PRE,
        TestGuest::Tracing => &TRACING_GUEST_PRE,
        TestGuest::Blobstore => &BLOBSTORE_GUEST_PRE,
        TestGuest::Llm => &LLM_GUEST_PRE,
        TestGuest::Http => &HTTP_GUEST_PRE,
    };
    let (engine, pre) = cell
        .get_or_try_init(|| async { wasm_module_pre(guest.wasm_path()) })
        .await?;
    Ok((engine.clone(), pre.clone()))
}

#[derive(Clone)]
pub struct GuestHarness {
    guest: TestGuest,
    engine: Arc<Engine>,
    pre: Arc<ProxyPre<ModuleState>>,
}

impl GuestHarness {
    pub async fn load(guest: TestGuest) -> Result<Option<Self>> {
        if guest.skip_if_missing() {
            return Ok(None);
        }
        let (engine, pre) = cached_guest_pre(guest).await?;
        Ok(Some(Self { guest, engine, pre }))
    }

    pub async fn require(guest: TestGuest) -> Result<Self> {
        Self::load(guest)
            .await?
            .ok_or_else(|| anyhow::anyhow!("{} WASM not built", guest.label()))
    }

    pub fn guest(&self) -> TestGuest {
        self.guest
    }

    pub fn engine_pre(&self) -> (Arc<Engine>, Arc<ProxyPre<ModuleState>>) {
        (self.engine.clone(), self.pre.clone())
    }

    pub async fn dispatch<M: prost::Message>(
        &self,
        state: ModuleState,
        method_path: &str,
        message: M,
    ) -> Result<http::Response<Bytes>> {
        let route = format!("{}{}", self.guest.route_prefix(), method_path);
        dispatch_to_wasm(
            &self.engine,
            &self.pre,
            state,
            rpc_request(&route, message.encode_to_vec()),
        )
        .await
    }

    pub async fn dispatch_typed<Req, Resp>(
        &self,
        state: ModuleState,
        path: RpcPath,
        request: Req,
    ) -> Result<Resp>
    where
        Req: prost::Message,
        Resp: prost::Message + Default,
    {
        self.dispatch_decode(state, path.as_str(), request).await
    }

    pub async fn dispatch_decode<Req, Resp>(
        &self,
        state: ModuleState,
        path: &str,
        request: Req,
    ) -> Result<Resp>
    where
        Req: prost::Message,
        Resp: prost::Message + Default,
    {
        let resp = self.dispatch(state, path, request).await?;
        if resp.status() != StatusCode::OK {
            bail!("WASM dispatch to {path} returned status {}", resp.status());
        }
        Ok(Resp::decode(resp.into_body())?)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RpcPath(&'static str);

impl RpcPath {
    pub fn new(path: &'static str) -> Result<Self> {
        if !path.starts_with('/') || path.chars().any(char::is_whitespace) {
            bail!("invalid RPC path: {path}");
        }
        Ok(Self(path))
    }

    pub fn as_str(self) -> &'static str {
        self.0
    }
}

/// Raw request escape hatch for malformed-request tests.
pub fn rpc_request(path: &str, body: Vec<u8>) -> http::Request<Bytes> {
    http::Request::builder()
        .method("POST")
        .uri(format!("http://localhost{path}"))
        .body(Bytes::from(body))
        .unwrap()
}

fn test_pool_config() -> wr_engine::config::PoolConfig {
    wr_engine::config::PoolConfig {
        total_component_instances: 100,
        max_memory_size: 10 * 1024 * 1024,
        epoch_tick_interval_ms: 10,
    }
}

/// Set up a wasmtime `Engine` + `ProxyPre` from a compiled WASM component path.
///
/// Configures the pooling instance allocator (matching production) so that
/// concurrent instantiations reuse pre-allocated memory slots instead of
/// issuing per-request mmap/mprotect syscalls.
pub fn wasm_module_pre(wasm_path: &str) -> Result<(Arc<Engine>, Arc<ProxyPre<ModuleState>>)> {
    let engine = wr_engine::runtime::build_engine(&test_pool_config())?;
    {
        let e = engine.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(10));
            loop {
                interval.tick().await;
                e.increment_epoch();
            }
        });
    }
    let component = Component::from_file(&engine, wasm_path)?;
    let linker = wr_engine::runtime::configure_linker(&engine)?;
    let pre = wr_engine::runtime::instantiate_pre(&engine, &linker, &component)?;
    Ok((Arc::new(engine), Arc::new(pre)))
}

/// Dispatch a single HTTP request through a WASM component, returning the response.
pub async fn dispatch_to_wasm(
    engine: &Engine,
    pre: &ProxyPre<ModuleState>,
    state: ModuleState,
    request: http::Request<Bytes>,
) -> Result<http::Response<Bytes>> {
    wr_engine::runtime::run_incoming_handler(engine, pre, state, request).await
}

/// Spawn a WASM-backed HTTP/2 engine on an ephemeral port.
///
/// Each incoming request is dispatched through a fresh `ModuleState` + `Store`
/// using the provided pre-compiled WASM component.  Returns the engine base URL
/// and a shutdown sender.
pub async fn spawn_wasm_stub_engine(
    engine: Arc<Engine>,
    pre: Arc<ProxyPre<ModuleState>>,
    proxy_uri: &str,
    module_name: &str,
    module_namespace: &str,
) -> Result<(String, oneshot::Sender<()>)> {
    let (tx, rx) = oneshot::channel::<()>();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = format!("http://{}", listener.local_addr()?);
    let proxy_uri: hyper::Uri = proxy_uri.parse()?;
    let module_name = module_name.to_string();
    let module_namespace = module_namespace.to_string();

    tokio::spawn(async move {
        tokio::select! {
            _ = rx => {}
            _ = wasm_engine_serve(listener, engine, pre, proxy_uri, module_name, module_namespace) => {}
        }
    });
    Ok((addr, tx))
}

async fn wasm_engine_serve(
    listener: TcpListener,
    engine: Arc<Engine>,
    pre: Arc<ProxyPre<ModuleState>>,
    proxy_uri: hyper::Uri,
    module_name: String,
    module_namespace: String,
) {
    let pool = http_pool();
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            break;
        };
        let engine = engine.clone();
        let pre = pre.clone();
        let proxy_uri = proxy_uri.clone();
        let module_name = module_name.clone();
        let module_namespace = module_namespace.clone();
        let pool = pool.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let svc = hyper::service::service_fn(move |req: Request<hyper::body::Incoming>| {
                let engine = engine.clone();
                let pre = pre.clone();
                let proxy_uri = proxy_uri.clone();
                let module_name = module_name.clone();
                let module_namespace = module_namespace.clone();
                let pool = pool.clone();
                async move {
                    // Collect the body on this stream, then spawn the
                    // CPU-heavy WASM work onto a separate tokio task so
                    // hyper's HTTP/2 serve_connection can drive other
                    // streams concurrently.
                    let (parts, body) = req.into_parts();
                    let body_bytes = body
                        .collect()
                        .await
                        .map(|c| c.to_bytes())
                        .unwrap_or_default();
                    let request = Request::from_parts(parts, body_bytes);

                    let handle = tokio::spawn(async move {
                        let state = ModuleState::new(
                            module_name.into(),
                            module_namespace.into(),
                            proxy_uri,
                            pool,
                            ModuleServices::default(),
                        )
                        .expect("ModuleState");

                        dispatch_to_wasm(&engine, &pre, state, request).await
                    });

                    match handle.await.expect("wasm task panicked") {
                        Ok(resp) => {
                            let (parts, body) = resp.into_parts();
                            Ok::<_, Infallible>(Response::from_parts(parts, Full::new(body)))
                        }
                        Err(e) => Ok(Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .body(Full::new(Bytes::from(format!("WASM error: {e}"))))
                            .unwrap()),
                    }
                }
            });
            let _ = http2::Builder::new(TokioExecutor::new())
                .serve_connection(io, svc)
                .await;
        });
    }
}
pub fn tracing_state() -> ModuleState {
    ModuleState::new(
        "tracing-test".into(),
        "test-ns".into(),
        "http://127.0.0.1:9001".parse().unwrap(),
        http_pool(),
        ModuleServices::default(),
    )
    .expect("ModuleState")
}

pub fn tracing_state_with_limits(limits: wr_engine::config::ResourceLimits) -> ModuleState {
    ModuleState::new(
        "tracing-test".into(),
        "test-ns".into(),
        "http://127.0.0.1:9001".parse().unwrap(),
        http_pool(),
        ModuleServices {
            limits,
            ..Default::default()
        },
    )
    .expect("ModuleState")
}

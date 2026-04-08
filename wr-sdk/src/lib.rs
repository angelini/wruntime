/// Generated WASI bindings for the import-only sdk world.
/// Does not export `wasi:http/incoming-handler`; each module's own cargo-component
/// world definition provides that.
pub mod bindings {
    wit_bindgen::generate!({
        path: "wit",
        world: "sdk",
        generate_all,
    });
}

// Re-export wit_bindgen_rt so macro-generated code can reference it via $crate.
#[doc(hidden)]
pub use ::wit_bindgen_rt as _rt;

/// Handler export helpers. The `IncomingRequest` and `ResponseOutparam` types
/// come from `wr_sdk::bindings::wasi::http::types`, ensuring type compatibility
/// with all modules that import `wr_sdk::bindings`.
pub mod exports {
    pub mod incoming_handler {
        use crate::bindings::wasi::http::types::{IncomingRequest, ResponseOutparam};

        /// Implement this trait and use `wr_sdk::export!` to register an HTTP
        /// handler module.
        pub trait ServiceGuest {
            fn handle(request: IncomingRequest, response_out: ResponseOutparam);

            /// Called once before the first request is handled. Use this for
            /// one-time setup such as `db::enable_tracing()`.
            fn init() {}

            /// Called by the engine on each heartbeat to determine if this module
            /// instance is healthy. Return `false` to mark the module unhealthy in
            /// the routing table. The default implementation always returns `true`.
            fn health_check() -> bool {
                true
            }
        }

        #[doc(hidden)]
        pub unsafe fn _export_handle_cabi<T: ServiceGuest>(arg0: i32, arg1: i32) {
            use crate::bindings::wasi::http::types::Method;

            #[cfg(target_arch = "wasm32")]
            ::wit_bindgen_rt::run_ctors_once();

            static INIT: std::sync::Once = std::sync::Once::new();
            INIT.call_once(T::init);

            let request = unsafe { IncomingRequest::from_handle(arg0 as u32) };
            let response_out = unsafe { ResponseOutparam::from_handle(arg1 as u32) };

            let is_health_check = matches!(request.method(), Method::Get)
                && request.path_with_query().as_deref() == Some("/__health");

            if is_health_check {
                let status = if T::health_check() { 200 } else { 503 };
                crate::io::send_response(response_out, status, vec![]);
            } else {
                T::handle(request, response_out);
            }
        }
    }
}

/// Convenience re-export of the HTTP handler `ServiceGuest` trait.
pub use exports::incoming_handler::ServiceGuest;

pub mod blobstore;
pub mod db;
pub mod http;
pub mod io;
pub mod jobs;
pub mod llm;
pub mod log;
pub mod prelude;
pub mod tracing;

/// Error type returned by generated service handler traits.
pub struct ServiceError {
    pub status: u16,
    pub message: String,
}

impl ServiceError {
    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self {
            status: 400,
            message: msg.into(),
        }
    }
    pub fn not_found(msg: impl Into<String>) -> Self {
        Self {
            status: 404,
            message: msg.into(),
        }
    }
    pub fn conflict(msg: impl Into<String>) -> Self {
        Self {
            status: 409,
            message: msg.into(),
        }
    }
    pub fn internal(msg: impl Into<String>) -> Self {
        Self {
            status: 500,
            message: msg.into(),
        }
    }
}

impl std::fmt::Display for ServiceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HTTP {}: {}", self.status, self.message)
    }
}

impl From<crate::http::HttpError> for ServiceError {
    fn from(e: crate::http::HttpError) -> Self {
        match e {
            crate::http::HttpError::Status { code, body } => ServiceError {
                status: code,
                message: String::from_utf8_lossy(&body).into_owned(),
            },
            crate::http::HttpError::Transport(msg) => {
                ServiceError::internal(format!("transport: {msg}"))
            }
            crate::http::HttpError::Decode(msg) => ServiceError::internal(format!("decode: {msg}")),
        }
    }
}

/// Export macro for HTTP handler modules (those that export `wasi:http/incoming-handler`).
///
/// Usage (in the module's `lib.rs`):
/// ```rust,ignore
/// struct MyComponent;
/// wr_sdk::export!(MyComponent with_types_in wr_sdk::bindings);
/// impl wr_sdk::ServiceGuest for MyComponent { fn handle(...) { ... } }
/// ```
#[macro_export]
macro_rules! export {
    ($ty:ident) => {
        $crate::export!($ty with_types_in $crate::bindings);
    };
    ($ty:ident with_types_in $($path_to_types:tt)*) => {
        const _: () = {
            #[unsafe(export_name = "wasi:http/incoming-handler@0.2.6#handle")]
            unsafe extern "C" fn wr_sdk_export_handle(arg0: i32, arg1: i32) {
                unsafe {
                    $crate::exports::incoming_handler::_export_handle_cabi::<$ty>(arg0, arg1)
                }
            }
        };
    };
}

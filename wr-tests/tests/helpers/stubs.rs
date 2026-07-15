use std::convert::Infallible;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::Full;
use hyper::server::conn::{http1, http2};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

pub async fn spawn_http1_stub() -> Result<(String, oneshot::Sender<()>)> {
    let (tx, rx) = oneshot::channel::<()>();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = format!("http://{}", listener.local_addr()?);
    tokio::spawn(async move {
        tokio::select! {
            _ = rx => {}
            _ = http1_stub(listener) => {}
        }
    });
    Ok((addr, tx))
}

async fn http1_stub(listener: TcpListener) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            break;
        };
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let svc =
                hyper::service::service_fn(|req: Request<hyper::body::Incoming>| async move {
                    let path = req.uri().path().to_string();
                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(StatusCode::OK)
                            .body(Full::new(Bytes::from(format!("egress:{path}"))))
                            .unwrap(),
                    )
                });
            let _ = http1::Builder::new().serve_connection(io, svc).await;
        });
    }
}
pub async fn stub_engine(listener: TcpListener) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            break;
        };
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let svc =
                hyper::service::service_fn(|req: Request<hyper::body::Incoming>| async move {
                    let path = req.uri().path().to_string();
                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(StatusCode::OK)
                            .body(Full::new(Bytes::from(path)))
                            .unwrap(),
                    )
                });
            let _ = http2::Builder::new(TokioExecutor::new())
                .serve_connection(io, svc)
                .await;
        });
    }
}

/// Like `stub_engine` but responds with a fixed `id` string so callers can
/// tell which instance handled a request.
pub async fn identified_stub(listener: TcpListener, id: String) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            break;
        };
        let id = id.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let svc = hyper::service::service_fn(move |_req: Request<hyper::body::Incoming>| {
                let id = id.clone();
                async move {
                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(StatusCode::OK)
                            .body(Full::new(Bytes::from(id)))
                            .unwrap(),
                    )
                }
            });
            let _ = http2::Builder::new(TokioExecutor::new())
                .serve_connection(io, svc)
                .await;
        });
    }
}

/// Spawn a `stub_engine` task; returns the engine's HTTP address and a
/// shutdown sender.  Send or drop the sender to stop the stub.
pub async fn spawn_stub_engine() -> Result<(String, oneshot::Sender<()>)> {
    let (tx, rx) = oneshot::channel::<()>();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = format!("http://{}", listener.local_addr()?);
    tokio::spawn(async move {
        tokio::select! {
            _ = rx => {}
            _ = stub_engine(listener) => {}
        }
    });
    Ok((addr, tx))
}

/// Spawn an `identified_stub` task; returns the engine's HTTP address and a
/// shutdown sender.  Send or drop the sender to stop the stub.
pub async fn spawn_identified_stub(id: &str) -> Result<(String, oneshot::Sender<()>)> {
    let (tx, rx) = oneshot::channel::<()>();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = format!("http://{}", listener.local_addr()?);
    let id = id.to_string();
    tokio::spawn(async move {
        tokio::select! {
            _ = rx => {}
            _ = identified_stub(listener, id) => {}
        }
    });
    Ok((addr, tx))
}

// ── Configurable stub engines ────────────────────────────────────────────────

/// A stub engine that always responds with a fixed HTTP status code.
pub async fn status_stub(listener: TcpListener, status_code: StatusCode) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            break;
        };
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let svc = hyper::service::service_fn(move |_req: Request<hyper::body::Incoming>| {
                let status = status_code;
                async move {
                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(status)
                            .body(Full::new(Bytes::from(format!("{}", status.as_u16()))))
                            .unwrap(),
                    )
                }
            });
            let _ = http2::Builder::new(TokioExecutor::new())
                .serve_connection(io, svc)
                .await;
        });
    }
}

/// Spawn a stub engine that always responds with `status_code`.
pub async fn spawn_status_stub(status_code: StatusCode) -> Result<(String, oneshot::Sender<()>)> {
    let (tx, rx) = oneshot::channel::<()>();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = format!("http://{}", listener.local_addr()?);
    tokio::spawn(async move {
        tokio::select! {
            _ = rx => {}
            _ = status_stub(listener, status_code) => {}
        }
    });
    Ok((addr, tx))
}

/// A stub engine whose response status can be switched at runtime via an
/// `Arc<std::sync::atomic::AtomicU16>`.
pub async fn switchable_stub(listener: TcpListener, status: Arc<std::sync::atomic::AtomicU16>) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            break;
        };
        let status = status.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let svc = hyper::service::service_fn(move |_req: Request<hyper::body::Incoming>| {
                let code = status.load(std::sync::atomic::Ordering::Relaxed);
                async move {
                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(code)
                            .body(Full::new(Bytes::from(format!("{code}"))))
                            .unwrap(),
                    )
                }
            });
            let _ = http2::Builder::new(TokioExecutor::new())
                .serve_connection(io, svc)
                .await;
        });
    }
}

/// Spawn a stub engine whose status can be switched at runtime.
/// Returns the engine address, a shutdown sender, and the status control.
pub async fn spawn_switchable_stub(
    initial_status: u16,
) -> Result<(
    String,
    oneshot::Sender<()>,
    Arc<std::sync::atomic::AtomicU16>,
)> {
    let (tx, rx) = oneshot::channel::<()>();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = format!("http://{}", listener.local_addr()?);
    let status = Arc::new(std::sync::atomic::AtomicU16::new(initial_status));
    let s = status.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = rx => {}
            _ = switchable_stub(listener, s) => {}
        }
    });
    Ok((addr, tx, status))
}

// ── Proxy with custom circuit breaker ────────────────────────────────────────

use tokio::signal::unix::{signal, SignalKind};

/// Waits for either SIGINT or SIGTERM, then returns.
pub async fn shutdown_signal() {
    let mut sigint = signal(SignalKind::interrupt()).expect("failed to register SIGINT handler");
    let mut sigterm = signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
    tokio::select! {
        _ = sigint.recv()  => {},
        _ = sigterm.recv() => {},
    }
}

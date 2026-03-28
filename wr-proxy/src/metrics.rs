use std::time::Duration;
use tokio::sync::mpsc;
use tracing::warn;

use wr_common::wruntime::{
    manager_service_client::ManagerServiceClient, ReportMetricsRequest, RequestMetrics,
};

/// Background task: drains the metrics channel and flushes batches to
/// wr-manager at a fixed interval.
pub async fn flush_metrics(
    mut client: ManagerServiceClient<tonic::transport::Channel>,
    mut rx: mpsc::Receiver<RequestMetrics>,
    flush_interval_secs: u64,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(flush_interval_secs));
    let mut buffer: Vec<RequestMetrics> = Vec::new();

    loop {
        tokio::select! {
            _ = interval.tick() => {
                if !buffer.is_empty() {
                    let batch = std::mem::take(&mut buffer);
                    if let Err(e) = client
                        .report_metrics(ReportMetricsRequest { metrics: batch })
                        .await
                    {
                        warn!(error = %e, "metrics flush failed");
                    }
                }
            }
            Some(metric) = rx.recv() => {
                buffer.push(metric);
            }
        }
    }
}

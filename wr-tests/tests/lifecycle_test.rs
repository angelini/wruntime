mod helpers;

use std::time::Duration;

use anyhow::Result;
use tokio::net::TcpListener;
use tokio::process::Command;
use tokio::sync::oneshot;
use tokio::time::Instant;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

use helpers::lifecycle::{
    evaluate_state, lifecycle_client, wait_for_ready_with_child, wait_for_state,
    LifecycleObservation, LifecycleWaitError, StateEvaluation,
};
use wr_common::wruntime::{
    lifecycle_service_server::{LifecycleService, LifecycleServiceServer},
    DrainRequest, DrainResponse, GetLifecycleStatusRequest, GetLifecycleStatusResponse,
    LifecycleStatus, LifecycleTransitionReason, ProcessLifecycleState, ServiceKind, StopRequest,
    StopResponse,
};

#[derive(Clone)]
struct FixedLifecycle {
    status: LifecycleStatus,
}

#[tonic::async_trait]
impl LifecycleService for FixedLifecycle {
    async fn get_status(
        &self,
        _request: Request<GetLifecycleStatusRequest>,
    ) -> Result<Response<GetLifecycleStatusResponse>, Status> {
        Ok(Response::new(GetLifecycleStatusResponse {
            status: Some(self.status.clone()),
        }))
    }

    async fn drain(
        &self,
        _request: Request<DrainRequest>,
    ) -> Result<Response<DrainResponse>, Status> {
        Err(Status::unimplemented("not needed by helper tests"))
    }

    async fn stop(&self, _request: Request<StopRequest>) -> Result<Response<StopResponse>, Status> {
        Err(Status::unimplemented("not needed by helper tests"))
    }
}

fn status(state: ProcessLifecycleState) -> LifecycleStatus {
    LifecycleStatus {
        state: state as i32,
        service_kind: ServiceKind::Engine as i32,
        process_instance_id: "helper-test".to_owned(),
        transitioned_at: None,
        reason: LifecycleTransitionReason::ProcessStarted as i32,
        detail: String::new(),
    }
}

async fn spawn_lifecycle_server(
    fixed_status: LifecycleStatus,
) -> Result<(String, oneshot::Sender<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(LifecycleServiceServer::new(FixedLifecycle {
                status: fixed_status,
            }))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("lifecycle helper test server failed");
    });
    Ok((format!("http://{address}"), shutdown_tx))
}

#[test]
fn typed_state_evaluation_accepts_ready_and_rejects_terminal_or_invalid_state() {
    assert_eq!(
        evaluate_state(
            &status(ProcessLifecycleState::Ready),
            ProcessLifecycleState::Ready
        )
        .unwrap(),
        StateEvaluation::Reached
    );
    assert!(matches!(
        evaluate_state(
            &status(ProcessLifecycleState::Stopping),
            ProcessLifecycleState::Ready
        ),
        Err(LifecycleWaitError::TerminalBeforeReady(_))
    ));
    assert!(matches!(
        evaluate_state(
            &status(ProcessLifecycleState::Unspecified),
            ProcessLifecycleState::Ready
        ),
        Err(LifecycleWaitError::InvalidState(0))
    ));
}

#[tokio::test]
async fn state_wait_preserves_the_last_typed_observation_at_deadline() -> Result<()> {
    let (endpoint, shutdown) =
        spawn_lifecycle_server(status(ProcessLifecycleState::Starting)).await?;
    let mut client = lifecycle_client(endpoint).await?;

    let error = wait_for_state(
        &mut client,
        ProcessLifecycleState::Ready,
        Instant::now() + Duration::from_millis(120),
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        LifecycleWaitError::Deadline {
            last_observation: Some(LifecycleObservation::Status(LifecycleStatus {
                state,
                ..
            })),
            ..
        } if state == ProcessLifecycleState::Starting as i32
    ));
    let _ = shutdown.send(());
    Ok(())
}

#[tokio::test]
async fn ready_wait_fails_immediately_when_the_supervised_child_exits() -> Result<()> {
    let mut child = Command::new("sh").args(["-c", "exit 17"]).spawn()?;
    let _ = child.wait().await?;
    let mut client = lifecycle_client("http://127.0.0.1:9").await?;

    let error = wait_for_ready_with_child(
        &mut client,
        &mut child,
        Instant::now() + Duration::from_secs(1),
    )
    .await
    .unwrap_err();

    assert!(matches!(error, LifecycleWaitError::ChildExited(status) if status.contains("17")));
    Ok(())
}

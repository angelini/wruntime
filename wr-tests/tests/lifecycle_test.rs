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
    GetLifecycleStatusRequest, GetLifecycleStatusResponse, LifecycleStatus,
    LifecycleTransitionReason, ProcessLifecycleState, ServiceKind,
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
async fn ready_wait_rejects_wrong_kind_and_activation_identity() -> Result<()> {
    let mut command = Command::new("sh");
    command.args(["-c", "sleep 30"]).kill_on_drop(true);
    let mut child = command.spawn()?;

    let mut wrong_kind = status(ProcessLifecycleState::Ready);
    wrong_kind.service_kind = ServiceKind::Proxy as i32;
    let (endpoint, shutdown) = spawn_lifecycle_server(wrong_kind).await?;
    let mut client = lifecycle_client(endpoint).await?;
    let error = wait_for_ready_with_child(
        &mut client,
        &mut child,
        ServiceKind::Engine,
        "helper-test",
        Instant::now() + Duration::from_secs(1),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        error,
        LifecycleWaitError::ServiceKindMismatch {
            expected: ServiceKind::Engine,
            observed,
        } if observed == ServiceKind::Proxy as i32
    ));
    let _ = shutdown.send(());

    let mut wrong_instance = status(ProcessLifecycleState::Ready);
    wrong_instance.process_instance_id = "other-activation".to_owned();
    let (endpoint, shutdown) = spawn_lifecycle_server(wrong_instance).await?;
    let mut client = lifecycle_client(endpoint).await?;
    let error = wait_for_ready_with_child(
        &mut client,
        &mut child,
        ServiceKind::Engine,
        "helper-test",
        Instant::now() + Duration::from_secs(1),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        error,
        LifecycleWaitError::ProcessInstanceMismatch { expected, observed }
            if expected == "helper-test" && observed == "other-activation"
    ));
    let _ = shutdown.send(());

    child.start_kill()?;
    let _ = child.wait().await?;
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
        ServiceKind::Engine,
        "helper-test",
        Instant::now() + Duration::from_secs(1),
    )
    .await
    .unwrap_err();

    assert!(matches!(error, LifecycleWaitError::ChildExited(status) if status.contains("17")));
    Ok(())
}

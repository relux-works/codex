use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use codex_exec_server::ExecProcess;
use codex_exec_server::ExecProcessEventReceiver;
use codex_exec_server::ExecProcessFuture;
use codex_exec_server::ProcessId;
use codex_exec_server::ProcessSignal;
use codex_exec_server::ReadResponse;
use codex_exec_server::StartedExecProcess;
use codex_exec_server::WriteResponse;
use codex_exec_server::WriteStatus;
use codex_protocol::ThreadId;
use codex_protocol::items::CommandExecutionItem;
use codex_protocol::items::CommandExecutionStatus;
use codex_protocol::items::TurnItem;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::Event;
use codex_protocol::protocol::EventMsg;
use codex_sandboxing::SandboxType;
use codex_utils_output_truncation::TruncationPolicy;
#[cfg(unix)]
use core_test_support::skip_if_sandbox;
use pretty_assertions::assert_eq;
use tokio::sync::Notify;
use tokio::sync::watch;
use tokio::time::Duration;
use tokio::time::Instant;

use crate::sandboxing::SandboxPermissions;
use crate::session::session::Session;
use crate::session::tests::make_session_and_context_with_auth_and_config_and_rx;
use crate::session::turn_context::TurnContext;
use crate::shell::ShellType;
use crate::tools::context::ExecCommandToolOutput;
use crate::unified_exec::ExecCommandRequest;
use crate::unified_exec::ProcessEntry;
use crate::unified_exec::TerminalPermissions;
use crate::unified_exec::TerminalSandboxSource;
use crate::unified_exec::UnifiedExecContext;
use crate::unified_exec::UnifiedExecError;
use crate::unified_exec::UnifiedExecProcessManager;
use crate::unified_exec::WriteStdinRequest;
use crate::unified_exec::async_watcher::spawn_exit_watcher;
use crate::unified_exec::async_watcher::start_streaming_output;
use crate::unified_exec::completion_receipt::CancellationReason;
use crate::unified_exec::completion_receipt::CompletionReceiptStore;
use crate::unified_exec::completion_receipt::ExecCompletionMode;
use crate::unified_exec::completion_receipt::InitialResponseDecision;
use crate::unified_exec::completion_receipt::InitialResponseOutcome;
use crate::unified_exec::completion_receipt::MAX_COMPLETION_RECEIPTS;
use crate::unified_exec::completion_receipt::ReceiptError;
use crate::unified_exec::completion_receipt::ReceiptId;
use crate::unified_exec::completion_receipt::ReceiptOwner;
use crate::unified_exec::completion_receipt::ReceiptStatus;
use crate::unified_exec::completion_receipt::SamplingSource;
use crate::unified_exec::completion_receipt::TerminalCompletion;
use crate::unified_exec::process::NoopSpawnLifecycle;
use crate::unified_exec::process::OutputBuffers;
use crate::unified_exec::process::UnifiedExecProcess;
use crate::unified_exec::receipt_output::RetainedOutputSnapshot;

async fn hook_test_session() -> (
    Arc<Session>,
    Arc<TurnContext>,
    async_channel::Receiver<Event>,
) {
    make_session_and_context_with_auth_and_config_and_rx(
        codex_login::CodexAuth::from_api_key("Test API Key"),
        Vec::new(),
        |config| {
            config.permissions.approval_policy =
                codex_config::Constrained::allow_any(AskForApproval::Never);
            // Spawning through the production exec_command path applies the
            // Linux sandbox, which needs the helper executable resolved the
            // same way as the other unified exec tests.
            #[cfg(target_os = "linux")]
            {
                config.codex_linux_sandbox_exe = Some(
                    core_test_support::find_codex_linux_sandbox_exe()
                        .expect("codex-linux-sandbox helper should resolve for tests"),
                );
            }
        },
    )
    .await
}

fn test_context(
    session: &Arc<Session>,
    turn: &Arc<TurnContext>,
    call_id: &str,
) -> UnifiedExecContext {
    UnifiedExecContext::new(
        Arc::clone(session),
        crate::session::step_context::StepContext::for_test(Arc::clone(turn)),
        tokio_util::sync::CancellationToken::new(),
        call_id.to_string(),
    )
}

async fn exec_request_for_hooks(
    session: &Arc<Session>,
    turn: &Arc<TurnContext>,
    call_id: &str,
    shell_command: &str,
    yield_time_ms: u64,
) -> (ExecCommandRequest, UnifiedExecContext) {
    let manager = &session.services.unified_exec_manager;
    let process_id = manager.allocate_process_id().await;
    #[allow(deprecated)]
    let cwd = turn.cwd.clone();
    let request = ExecCommandRequest {
        command: vec![
            "bash".to_string(),
            "-lc".to_string(),
            shell_command.to_string(),
        ],
        shell_type: ShellType::Bash,
        hook_command: shell_command.to_string(),
        process_id,
        yield_time_ms,
        max_output_tokens: None,
        cwd: cwd.clone().into(),
        sandbox_cwd: cwd.into(),
        turn_environment: turn
            .initial_environments
            .primary()
            .cloned()
            .expect("primary environment"),
        shell_mode: codex_tools::UnifiedExecShellMode::Direct,
        network: None,
        tty: false,
        sandbox_permissions: SandboxPermissions::UseDefault,
        additional_permissions: None,
        additional_permissions_preapproved: false,
        justification: None,
        prefix_rule: None,
    };
    (request, test_context(session, turn, call_id))
}

#[cfg(unix)]
async fn exec_command_for_hooks(
    session: &Arc<Session>,
    turn: &Arc<TurnContext>,
    call_id: &str,
    shell_command: &str,
    yield_time_ms: u64,
    mode: ExecCompletionMode,
) -> Result<ExecCommandToolOutput, UnifiedExecError> {
    let (request, context) =
        exec_request_for_hooks(session, turn, call_id, shell_command, yield_time_ms).await;
    session
        .services
        .unified_exec_manager
        .exec_command_with_completion_mode(request, &context, mode)
        .await
}

async fn write_stdin_for_hooks(
    session: &Arc<Session>,
    turn: &Arc<TurnContext>,
    process_id: i32,
    input: &str,
    yield_time_ms: u64,
) -> Result<ExecCommandToolOutput, UnifiedExecError> {
    session
        .services
        .unified_exec_manager
        .write_stdin(
            &test_context(session, turn, "write-stdin-test"),
            WriteStdinRequest {
                process_id,
                input,
                yield_time_ms,
                max_output_tokens: None,
                truncation_policy: TruncationPolicy::Tokens(10_000),
                interaction_event: None,
            },
        )
        .await
}

struct DriverProcess {
    process: Arc<UnifiedExecProcess>,
    stdout_tx: tokio::sync::broadcast::Sender<Vec<u8>>,
    exit_tx: tokio::sync::oneshot::Sender<i32>,
    output_buffer: Arc<tokio::sync::Mutex<OutputBuffers>>,
}

async fn driver_process(channel_capacity: usize) -> anyhow::Result<DriverProcess> {
    let (writer_tx, _writer_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(1);
    let (stdout_tx, stdout_rx) = tokio::sync::broadcast::channel::<Vec<u8>>(channel_capacity);
    let (exit_tx, exit_rx) = tokio::sync::oneshot::channel::<i32>();
    let spawned = codex_utils_pty::spawn_from_driver(codex_utils_pty::ProcessDriver {
        writer_tx,
        stdout_rx,
        stderr_rx: None,
        exit_rx,
        terminator: None,
        writer_handle: None,
        resizer: None,
        #[cfg(windows)]
        tty: false,
    });
    let process = Arc::new(
        UnifiedExecProcess::from_spawned(spawned, SandboxType::None, Box::new(NoopSpawnLifecycle))
            .await?,
    );
    let output_buffer = Arc::clone(&process.output_handles().output_buffer);
    Ok(DriverProcess {
        process,
        stdout_tx,
        exit_tx,
        output_buffer,
    })
}

async fn insert_process_entry(
    session: &Arc<Session>,
    turn: &Arc<TurnContext>,
    process: Arc<UnifiedExecProcess>,
    process_id: i32,
    call_id: &str,
    hook_command: &str,
    tty: bool,
) {
    #[allow(deprecated)]
    let cwd = turn.cwd.clone();
    let entry = ProcessEntry {
        process,
        plugin_metrics_sidecar: None,
        call_id: call_id.to_string(),
        process_id,
        cwd: cwd.into(),
        initial_exec_command_active: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        hook_command: hook_command.to_string(),
        tty,
        environment_id: codex_exec_server::LOCAL_ENVIRONMENT_ID.to_string(),
        permissions: TerminalPermissions::for_launch(
            turn.initial_environments
                .primary()
                .expect("turn environment"),
            turn,
            TerminalSandboxSource::Native,
            SandboxPermissions::UseDefault,
            /*additional_permissions*/ None,
            /*internal_permissions*/ None,
        ),
        network_approval: None,
        session: Arc::downgrade(session),
        last_used: Instant::now(),
    };
    session
        .services
        .unified_exec_manager
        .process_store
        .lock()
        .await
        .processes
        .insert(process_id, entry);
}

async fn reserve_and_arm(
    manager: &UnifiedExecProcessManager,
    session: &Arc<Session>,
    turn: &Arc<TurnContext>,
    call_id: &str,
    process_id: i32,
) -> (ReceiptId, ReceiptOwner) {
    let context = test_context(session, turn, call_id);
    let owner = manager
        .receipt_owner_for(&context)
        .expect("receipt owner should build");
    let receipt_id = manager
        .reserve_completion_receipt(owner.clone(), process_id)
        .await
        .expect("reservation should succeed");
    assert_eq!(
        manager.receipt_store().resolve_initial_response(
            receipt_id,
            &owner,
            InitialResponseDecision::Arm,
        ),
        Ok(InitialResponseOutcome::Armed)
    );
    (receipt_id, owner)
}

async fn await_receipt_status(
    manager: &UnifiedExecProcessManager,
    receipt_id: ReceiptId,
    owner: &ReceiptOwner,
    expected: ReceiptStatus,
) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let status = manager.receipt_status(receipt_id, owner);
        if status == Ok(expected.clone()) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {expected:?}, last status: {status:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn await_transcript_bytes(process: &UnifiedExecProcess, min_bytes: usize) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let observed = process
            .output_handles()
            .output_buffer
            .lock()
            .await
            .transcript
            .total_bytes();
        if observed >= min_bytes {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for transcript output"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn await_process_exit(process: &UnifiedExecProcess) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if process.has_exited() {
            return;
        }
        assert!(Instant::now() < deadline, "timed out waiting for exit");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn await_flag(flag: &AtomicBool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !flag.load(Ordering::SeqCst) {
        assert!(Instant::now() < deadline, "timed out waiting for flag");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn await_command_end(
    rx_event: &async_channel::Receiver<Event>,
) -> anyhow::Result<CommandExecutionItem> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let event = rx_event.recv().await.expect("event channel stays open");
            if let EventMsg::ItemCompleted(completed) = event.msg
                && let TurnItem::CommandExecution(item) = completed.item
            {
                return item;
            }
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("timed out waiting for command end"))
}

async fn sampled_output_receipt(
    session: &Arc<Session>,
    turn: &Arc<TurnContext>,
    call_id: &str,
    process_id: i32,
    output: &[u8],
) -> anyhow::Result<(ReceiptId, ReceiptOwner)> {
    let manager = &session.services.unified_exec_manager;
    let context = test_context(session, turn, call_id);
    let owner = manager.receipt_owner_for(&context)?;
    let receipt_id = manager
        .reserve_completion_receipt(owner.clone(), process_id)
        .await?;
    assert_eq!(
        manager.receipt_store().resolve_initial_response(
            receipt_id,
            &owner,
            InitialResponseDecision::Arm,
        ),
        Ok(InitialResponseOutcome::Armed)
    );
    let DriverProcess {
        process,
        stdout_tx,
        exit_tx,
        output_buffer,
    } = driver_process(/*channel_capacity*/ 8).await?;
    start_streaming_output(&process, &context);
    #[allow(deprecated)]
    let cwd = turn.cwd.clone().into();
    spawn_exit_watcher(
        Arc::clone(&process),
        &context,
        vec!["sampled-proof".to_string()],
        cwd,
        process_id,
        /*plugin_attribution*/ None,
        output_buffer,
        Instant::now(),
        /*network_denial_monitor*/ None,
        /*plugin_metrics_sidecar*/ None,
        Some(manager.watcher_receipt_hook(receipt_id, owner.clone())),
    );
    stdout_tx.send(output.to_vec())?;
    await_transcript_bytes(&process, output.len()).await;
    exit_tx.send(0).expect("send exit");
    drop(stdout_tx);
    await_receipt_status(manager, receipt_id, &owner, ReceiptStatus::Queued).await;
    let lease = manager.lease_pushed_completion(receipt_id, &owner)?;
    manager.acknowledge_pushed_completion(&lease).await?;
    Ok((receipt_id, owner))
}

fn foreign_receipt_owners(
    thread_id: ThreadId,
    other_generation: u64,
    call_id: &str,
) -> [ReceiptOwner; 3] {
    [
        ReceiptOwner::new(
            ThreadId::from_u128(0x018f_0000_0000_7000_8000_0000_0000_00c3),
            other_generation,
            call_id.to_string(),
        )
        .expect("foreign thread owner should be valid"),
        ReceiptOwner::new(thread_id, other_generation, call_id.to_string())
            .expect("foreign generation owner should be valid"),
        ReceiptOwner::new(thread_id, other_generation, format!("{call_id}-other"))
            .expect("foreign call owner should be valid"),
    ]
}

struct GatedExecProcess {
    process_id: ProcessId,
    wake_tx: watch::Sender<u64>,
    block_terminate: AtomicBool,
    block_signal: AtomicBool,
    terminate_called: AtomicBool,
    signal_called: AtomicBool,
    release_terminate: Notify,
    release_signal: Notify,
}

impl GatedExecProcess {
    fn new(process_id: i32) -> Arc<Self> {
        let (wake_tx, _) = watch::channel(0);
        Arc::new(Self {
            process_id: process_id.to_string().into(),
            wake_tx,
            block_terminate: AtomicBool::new(false),
            block_signal: AtomicBool::new(false),
            terminate_called: AtomicBool::new(false),
            signal_called: AtomicBool::new(false),
            release_terminate: Notify::new(),
            release_signal: Notify::new(),
        })
    }

    async fn read(&self) -> Result<ReadResponse, codex_exec_server::ExecServerError> {
        Ok(ReadResponse {
            chunks: Vec::new(),
            next_seq: 1,
            exited: false,
            exit_code: None,
            closed: false,
            failure: None,
            sandbox_denied: false,
        })
    }

    async fn write(&self) -> Result<WriteResponse, codex_exec_server::ExecServerError> {
        Ok(WriteResponse {
            status: WriteStatus::Accepted,
        })
    }

    async fn signal(&self) -> Result<(), codex_exec_server::ExecServerError> {
        self.signal_called.store(true, Ordering::SeqCst);
        if self.block_signal.load(Ordering::SeqCst) {
            self.release_signal.notified().await;
        }
        Ok(())
    }

    async fn terminate(&self) -> Result<(), codex_exec_server::ExecServerError> {
        self.terminate_called.store(true, Ordering::SeqCst);
        if self.block_terminate.load(Ordering::SeqCst) {
            self.release_terminate.notified().await;
        }
        Ok(())
    }
}

impl ExecProcess for GatedExecProcess {
    fn process_id(&self) -> &ProcessId {
        &self.process_id
    }

    fn subscribe_wake(&self) -> watch::Receiver<u64> {
        self.wake_tx.subscribe()
    }

    fn subscribe_events(&self) -> ExecProcessEventReceiver {
        ExecProcessEventReceiver::empty()
    }

    fn read(
        &self,
        _after_seq: Option<u64>,
        _max_bytes: Option<usize>,
        _wait_ms: Option<u64>,
    ) -> ExecProcessFuture<'_, ReadResponse> {
        Box::pin(GatedExecProcess::read(self))
    }

    fn write(&self, _chunk: Vec<u8>) -> ExecProcessFuture<'_, WriteResponse> {
        Box::pin(GatedExecProcess::write(self))
    }

    fn signal(&self, _signal: ProcessSignal) -> ExecProcessFuture<'_, ()> {
        Box::pin(GatedExecProcess::signal(self))
    }

    fn terminate(&self) -> ExecProcessFuture<'_, ()> {
        Box::pin(GatedExecProcess::terminate(self))
    }
}

async fn gated_unified_process(
    backend: Arc<GatedExecProcess>,
) -> anyhow::Result<Arc<UnifiedExecProcess>> {
    Ok(Arc::new(
        UnifiedExecProcess::from_exec_server_started(StartedExecProcess {
            process: backend,
            sandbox_type: Some(SandboxType::None),
        })
        .await?,
    ))
}

#[tokio::test]
async fn receipt_exit_publishes_only_after_drain_denial_and_classification() -> anyhow::Result<()> {
    let (session, turn, rx_event) = hook_test_session().await;
    let manager = &session.services.unified_exec_manager;
    let context = test_context(&session, &turn, "call-latch");
    let owner = manager.receipt_owner_for(&context)?;
    let receipt_id = manager
        .reserve_completion_receipt(owner.clone(), /*process_id*/ -1)
        .await?;
    assert_eq!(
        manager.receipt_store().resolve_initial_response(
            receipt_id,
            &owner,
            InitialResponseDecision::Arm,
        ),
        Ok(InitialResponseOutcome::Armed)
    );

    let DriverProcess {
        process,
        stdout_tx,
        exit_tx,
        output_buffer,
    } = driver_process(/*channel_capacity*/ 8).await?;
    // Hold the output drain by not starting streaming, and hold the denial
    // monitor with a latched join handle.
    let (monitor_release_tx, monitor_release_rx) = tokio::sync::oneshot::channel::<()>();
    let network_denial_monitor = tokio::spawn(async move {
        let _ = monitor_release_rx.await;
    });
    #[allow(deprecated)]
    let cwd = turn.cwd.clone().into();
    spawn_exit_watcher(
        Arc::clone(&process),
        &context,
        vec!["latch-proof".to_string()],
        cwd,
        /*process_id*/ 4242,
        /*plugin_attribution*/ None,
        output_buffer,
        Instant::now(),
        Some(network_denial_monitor),
        /*plugin_metrics_sidecar*/ None,
        Some(manager.watcher_receipt_hook(receipt_id, owner.clone())),
    );

    stdout_tx.send(b"latch-output\n".to_vec())?;
    await_transcript_bytes(&process, b"latch-output\n".len()).await;
    exit_tx.send(0).expect("send exit");
    drop(stdout_tx);
    await_process_exit(&process).await;

    // Exit observed but the drain held: still Armed.
    assert_eq!(
        manager.receipt_status(receipt_id, &owner),
        Ok(ReceiptStatus::Armed)
    );

    // Drain released but the denial monitor held: still Armed.
    process.output_drained_notify().notify_one();
    assert_eq!(
        manager.receipt_status(receipt_id, &owner),
        Ok(ReceiptStatus::Armed)
    );

    // Monitor released: exactly one queued completion.
    monitor_release_tx.send(()).expect("release monitor");
    await_receipt_status(manager, receipt_id, &owner, ReceiptStatus::Queued).await;
    let lease = manager.lease_pushed_completion(receipt_id, &owner)?;
    assert_eq!(
        manager.acknowledge_pushed_completion(&lease).await?,
        TerminalCompletion {
            exit_code: Some(0),
            timed_out: false,
        }
    );

    // ExecCommandEnd emission is unchanged.
    let end = await_command_end(&rx_event).await?;
    assert_eq!(
        (end.status, end.exit_code),
        (CommandExecutionStatus::Completed, Some(0))
    );

    // Retained output is readable by receipt.
    assert_eq!(
        manager.read_retained_output(receipt_id, &owner).await?,
        RetainedOutputSnapshot {
            bytes: b"latch-output\n".to_vec(),
            truncated: false,
            omitted_bytes: 0,
        }
    );
    Ok(())
}

#[tokio::test]
async fn receipt_failed_exit_maps_to_failed_completion() -> anyhow::Result<()> {
    let (session, turn, rx_event) = hook_test_session().await;
    let manager = &session.services.unified_exec_manager;
    let context = test_context(&session, &turn, "call-failed");
    let owner = manager.receipt_owner_for(&context)?;
    let receipt_id = manager
        .reserve_completion_receipt(owner.clone(), /*process_id*/ -2)
        .await?;
    assert_eq!(
        manager.receipt_store().resolve_initial_response(
            receipt_id,
            &owner,
            InitialResponseDecision::Arm,
        ),
        Ok(InitialResponseOutcome::Armed)
    );

    let DriverProcess {
        process,
        stdout_tx,
        exit_tx: _exit_tx,
        output_buffer,
    } = driver_process(/*channel_capacity*/ 8).await?;
    start_streaming_output(&process, &context);
    #[allow(deprecated)]
    let cwd = turn.cwd.clone().into();
    spawn_exit_watcher(
        Arc::clone(&process),
        &context,
        vec!["failed-proof".to_string()],
        cwd,
        /*process_id*/ 4243,
        /*plugin_attribution*/ None,
        output_buffer,
        Instant::now(),
        /*network_denial_monitor*/ None,
        /*plugin_metrics_sidecar*/ None,
        Some(manager.watcher_receipt_hook(receipt_id, owner.clone())),
    );

    process.fail_and_terminate("BOOM".to_string());
    drop(stdout_tx);
    await_receipt_status(manager, receipt_id, &owner, ReceiptStatus::Queued).await;
    let lease = manager.lease_pushed_completion(receipt_id, &owner)?;
    assert_eq!(
        manager.acknowledge_pushed_completion(&lease).await?,
        TerminalCompletion {
            exit_code: None,
            timed_out: false,
        }
    );
    let end = await_command_end(&rx_event).await?;
    assert_eq!(
        (end.status, end.exit_code),
        (CommandExecutionStatus::Failed, Some(-1))
    );
    Ok(())
}

#[tokio::test]
async fn receipt_exit_preserves_timed_out() -> anyhow::Result<()> {
    let (session, turn, _rx_event) = hook_test_session().await;
    let manager = &session.services.unified_exec_manager;
    let context = test_context(&session, &turn, "call-timed-out");
    let owner = manager.receipt_owner_for(&context)?;
    let receipt_id = manager
        .reserve_completion_receipt(owner.clone(), /*process_id*/ -3)
        .await?;
    assert_eq!(
        manager.receipt_store().resolve_initial_response(
            receipt_id,
            &owner,
            InitialResponseDecision::Arm,
        ),
        Ok(InitialResponseOutcome::Armed)
    );

    let DriverProcess {
        process,
        stdout_tx,
        exit_tx,
        output_buffer,
    } = driver_process(/*channel_capacity*/ 8).await?;
    process.mark_timed_out();
    start_streaming_output(&process, &context);
    #[allow(deprecated)]
    let cwd = turn.cwd.clone().into();
    spawn_exit_watcher(
        Arc::clone(&process),
        &context,
        vec!["timed-out-proof".to_string()],
        cwd,
        /*process_id*/ 4244,
        /*plugin_attribution*/ None,
        output_buffer,
        Instant::now(),
        /*network_denial_monitor*/ None,
        /*plugin_metrics_sidecar*/ None,
        Some(manager.watcher_receipt_hook(receipt_id, owner.clone())),
    );

    exit_tx.send(0).expect("send exit");
    drop(stdout_tx);
    await_receipt_status(manager, receipt_id, &owner, ReceiptStatus::Queued).await;
    let lease = manager.lease_pushed_completion(receipt_id, &owner)?;
    assert_eq!(
        manager.acknowledge_pushed_completion(&lease).await?,
        TerminalCompletion {
            exit_code: Some(124),
            timed_out: true,
        }
    );
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn opted_in_exit_before_decision_returns_inline_and_frees_slot() -> anyhow::Result<()> {
    skip_if_sandbox!(Ok(()));
    let (session, turn, _rx_event) = hook_test_session().await;
    let response = exec_command_for_hooks(
        &session,
        &turn,
        "call-inline",
        "echo hello-inline",
        /*yield_time_ms*/ 30_000,
        ExecCompletionMode::NotifyOnExit,
    )
    .await?;
    assert_eq!(response.process_id, None);
    assert_eq!(response.exit_code, Some(0));
    assert!(
        String::from_utf8_lossy(&response.raw_output).contains("hello-inline"),
        "terminal result should be returned inline"
    );

    let manager = &session.services.unified_exec_manager;
    assert_eq!(manager.receipt_capacity_used().await?, 0);
    // No queued completion holds a slot: 64 fresh reservations succeed.
    for index in 0..MAX_COMPLETION_RECEIPTS {
        let context = test_context(&session, &turn, &format!("call-inline-fill-{index}"));
        let owner = manager.receipt_owner_for(&context)?;
        manager
            .reserve_completion_receipt(owner, -(1000 + index as i32))
            .await?;
    }
    assert_eq!(
        manager.receipt_capacity_used().await?,
        MAX_COMPLETION_RECEIPTS
    );
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn opted_in_decision_before_exit_queues_exactly_one_completion() -> anyhow::Result<()> {
    skip_if_sandbox!(Ok(()));
    let (session, turn, rx_event) = hook_test_session().await;
    let manager = &session.services.unified_exec_manager;
    let response = exec_command_for_hooks(
        &session,
        &turn,
        "call-queued",
        "sleep 2",
        /*yield_time_ms*/ 250,
        ExecCompletionMode::NotifyOnExit,
    )
    .await?;
    let process_id = response.process_id.expect("should yield a live process");
    let (receipt_id, owner) = manager
        .receipt_for_process(process_id)
        .await
        .expect("opted-in launch should bind a receipt");
    assert_eq!(
        manager.receipt_status(receipt_id, &owner),
        Ok(ReceiptStatus::Armed)
    );

    await_receipt_status(manager, receipt_id, &owner, ReceiptStatus::Queued).await;
    let lease = manager.lease_pushed_completion(receipt_id, &owner)?;
    assert_eq!(
        manager.acknowledge_pushed_completion(&lease).await?,
        TerminalCompletion {
            exit_code: Some(0),
            timed_out: false,
        }
    );
    assert_eq!(
        manager.lease_pushed_completion(receipt_id, &owner),
        Err(ReceiptError::AlreadyConsumed)
    );
    let end = await_command_end(&rx_event).await?;
    assert_eq!(end.exit_code, Some(0));
    manager
        .release_completion_receipt(receipt_id, &owner)
        .await?;
    assert_eq!(manager.receipt_capacity_used().await?, 0);
    Ok(())
}

#[tokio::test]
async fn retained_output_survives_process_entry_removal() -> anyhow::Result<()> {
    let (session, turn, _rx_event) = hook_test_session().await;
    let manager = &session.services.unified_exec_manager;
    let process_id = manager.allocate_process_id().await;
    let (receipt_id, owner) =
        reserve_and_arm(manager, &session, &turn, "call-retain", process_id).await;
    let DriverProcess {
        process,
        stdout_tx,
        exit_tx,
        output_buffer,
    } = driver_process(/*channel_capacity*/ 8).await?;
    insert_process_entry(
        &session,
        &turn,
        Arc::clone(&process),
        process_id,
        "call-retain",
        "retain-proof",
        /*tty*/ false,
    )
    .await;
    let context = test_context(&session, &turn, "call-retain");
    start_streaming_output(&process, &context);
    #[allow(deprecated)]
    let cwd = turn.cwd.clone().into();
    spawn_exit_watcher(
        Arc::clone(&process),
        &context,
        vec!["retain-proof".to_string()],
        cwd,
        process_id,
        /*plugin_attribution*/ None,
        output_buffer,
        Instant::now(),
        /*network_denial_monitor*/ None,
        /*plugin_metrics_sidecar*/ None,
        Some(manager.watcher_receipt_hook(receipt_id, owner.clone())),
    );
    stdout_tx.send(b"output-bytes-123".to_vec())?;
    await_transcript_bytes(&process, b"output-bytes-123".len()).await;
    exit_tx.send(0).expect("send exit");
    drop(stdout_tx);
    await_receipt_status(manager, receipt_id, &owner, ReceiptStatus::Queued).await;

    manager.release_process_id(process_id).await;
    assert!(
        !manager
            .process_store
            .lock()
            .await
            .processes
            .contains_key(&process_id),
        "process entry should be removed before the read"
    );
    assert_eq!(
        manager.read_retained_output(receipt_id, &owner).await?,
        RetainedOutputSnapshot {
            bytes: b"output-bytes-123".to_vec(),
            truncated: false,
            omitted_bytes: 0,
        }
    );
    Ok(())
}

#[tokio::test]
async fn retained_output_over_cap_keeps_head_tail_and_omitted_count() -> anyhow::Result<()> {
    let (session, turn, _rx_event) = hook_test_session().await;
    let manager = &session.services.unified_exec_manager;
    let context = test_context(&session, &turn, "call-head-tail");
    let owner = manager.receipt_owner_for(&context)?;
    let receipt_id = manager
        .reserve_completion_receipt(owner.clone(), /*process_id*/ -7)
        .await?;
    assert_eq!(
        manager.receipt_store().resolve_initial_response(
            receipt_id,
            &owner,
            InitialResponseDecision::Arm,
        ),
        Ok(InitialResponseOutcome::Armed)
    );

    let DriverProcess {
        process,
        stdout_tx,
        exit_tx,
        output_buffer,
    } = driver_process(/*channel_capacity*/ 512).await?;
    start_streaming_output(&process, &context);
    #[allow(deprecated)]
    let cwd = turn.cwd.clone().into();
    spawn_exit_watcher(
        Arc::clone(&process),
        &context,
        vec!["head-tail-proof".to_string()],
        cwd,
        /*process_id*/ -7,
        /*plugin_attribution*/ None,
        output_buffer,
        Instant::now(),
        /*network_denial_monitor*/ None,
        /*plugin_metrics_sidecar*/ None,
        Some(manager.watcher_receipt_hook(receipt_id, owner.clone())),
    );
    const ONE_MIB: usize = 1024 * 1024;
    stdout_tx.send(vec![b'H'; ONE_MIB])?;
    stdout_tx.send(vec![b'M'; ONE_MIB])?;
    stdout_tx.send(vec![b'T'; ONE_MIB])?;
    await_transcript_bytes(&process, 3 * ONE_MIB).await;
    exit_tx.send(0).expect("send exit");
    drop(stdout_tx);
    await_receipt_status(manager, receipt_id, &owner, ReceiptStatus::Queued).await;

    let snapshot = manager.read_retained_output(receipt_id, &owner).await?;
    assert_eq!(snapshot.bytes.len(), ONE_MIB);
    assert_eq!(&snapshot.bytes[..8], b"HHHHHHHH");
    assert_eq!(&snapshot.bytes[snapshot.bytes.len() - 8..], b"TTTTTTTT");
    assert_eq!(snapshot.truncated, true);
    assert_eq!(snapshot.omitted_bytes, 2 * ONE_MIB);
    Ok(())
}

#[tokio::test]
async fn retained_output_read_refuses_unknown_and_foreign_receipts() -> anyhow::Result<()> {
    let (session, turn, _rx_event) = hook_test_session().await;
    let manager = &session.services.unified_exec_manager;
    let (receipt_id, owner) = sampled_output_receipt(
        &session,
        &turn,
        "call-owned",
        /*process_id*/ -8,
        b"owned",
    )
    .await?;

    // Unknown: an id reserved on a separate store.
    let separate = CompletionReceiptStore::default();
    let unknown_owner = manager.receipt_owner_for(&test_context(&session, &turn, "call-owned"))?;
    let unknown_id = separate.reserve(unknown_owner.clone())?;
    assert_eq!(
        manager
            .read_retained_output(unknown_id, &unknown_owner)
            .await,
        Err(ReceiptError::UnknownReceipt)
    );

    let other_manager = UnifiedExecProcessManager::default();
    for foreign_owner in foreign_receipt_owners(
        session.thread_id,
        other_manager.receipt_generation,
        "call-owned",
    ) {
        assert_eq!(
            manager
                .read_retained_output(receipt_id, &foreign_owner)
                .await,
            Err(ReceiptError::ForeignOwner)
        );
    }
    assert!(
        manager
            .read_retained_output(receipt_id, &owner)
            .await
            .is_ok(),
        "matching owner should still read"
    );
    Ok(())
}

#[tokio::test]
async fn receipt_capacity_refuses_65th_unsampled_reservation() -> anyhow::Result<()> {
    let (session, turn, _rx_event) = hook_test_session().await;
    let manager = &session.services.unified_exec_manager;
    for index in 0..MAX_COMPLETION_RECEIPTS {
        let context = test_context(&session, &turn, &format!("call-fill-{index}"));
        let owner = manager.receipt_owner_for(&context)?;
        manager
            .reserve_completion_receipt(owner, -(1000 + index as i32))
            .await?;
    }
    assert_eq!(
        manager.receipt_capacity_used().await?,
        MAX_COMPLETION_RECEIPTS
    );

    let context = test_context(&session, &turn, "call-overflow");
    let owner = manager.receipt_owner_for(&context)?;
    assert_eq!(
        manager
            .reserve_completion_receipt(owner, /*process_id*/ -2000)
            .await,
        Err(ReceiptError::CapacityExceeded {
            capacity: MAX_COMPLETION_RECEIPTS,
        })
    );
    Ok(())
}

#[tokio::test]
async fn opted_in_exec_refuses_65th_before_spawning() -> anyhow::Result<()> {
    let (session, turn, _rx_event) = hook_test_session().await;
    let manager = &session.services.unified_exec_manager;
    for index in 0..MAX_COMPLETION_RECEIPTS {
        let context = test_context(&session, &turn, &format!("call-prefill-{index}"));
        let owner = manager.receipt_owner_for(&context)?;
        manager
            .reserve_completion_receipt(owner, -(2000 + index as i32))
            .await?;
    }

    let scratch = tempfile::tempdir()?;
    let sentinel = scratch.path().join("must-not-spawn");
    let (request, context) = exec_request_for_hooks(
        &session,
        &turn,
        "call-refused",
        &format!("touch {}", sentinel.display()),
        /*yield_time_ms*/ 5_000,
    )
    .await;
    let process_id = request.process_id;
    let result = manager
        .exec_command_with_completion_mode(request, &context, ExecCompletionMode::NotifyOnExit)
        .await;
    match result {
        Err(UnifiedExecError::ReceiptCapacityExceeded { capacity }) => {
            assert_eq!(capacity, MAX_COMPLETION_RECEIPTS);
        }
        other => panic!("65th opted-in launch should be refused before spawning, got {other:?}"),
    }
    assert!(
        !sentinel.exists(),
        "refused launch must not spawn a child process"
    );
    assert!(
        !manager
            .process_store
            .lock()
            .await
            .processes
            .contains_key(&process_id),
        "refused launch must not leave a process-store entry"
    );
    Ok(())
}

#[tokio::test]
async fn new_reservation_retires_least_recently_sampled_output() -> anyhow::Result<()> {
    let (session, turn, _rx_event) = hook_test_session().await;
    let manager = &session.services.unified_exec_manager;
    for index in 0..MAX_COMPLETION_RECEIPTS - 2 {
        let context = test_context(&session, &turn, &format!("call-lru-fill-{index}"));
        let owner = manager.receipt_owner_for(&context)?;
        manager
            .reserve_completion_receipt(owner, -(3000 + index as i32))
            .await?;
    }
    let (first_id, first_owner) = sampled_output_receipt(
        &session,
        &turn,
        "call-sampled-first",
        /*process_id*/ -4001,
        b"first-output",
    )
    .await?;
    let (second_id, second_owner) = sampled_output_receipt(
        &session,
        &turn,
        "call-sampled-second",
        /*process_id*/ -4002,
        b"second-output",
    )
    .await?;
    assert_eq!(
        manager.receipt_capacity_used().await?,
        MAX_COMPLETION_RECEIPTS
    );

    let context = test_context(&session, &turn, "call-new");
    let new_owner = manager.receipt_owner_for(&context)?;
    manager
        .reserve_completion_receipt(new_owner, /*process_id*/ -4000)
        .await?;

    assert_eq!(
        manager.read_retained_output(first_id, &first_owner).await,
        Err(ReceiptError::Retired)
    );
    assert_eq!(
        manager
            .read_retained_output(second_id, &second_owner)
            .await?
            .bytes,
        b"second-output"
    );
    assert_eq!(
        manager.receipt_capacity_used().await?,
        MAX_COMPLETION_RECEIPTS
    );
    Ok(())
}

#[tokio::test]
async fn release_frees_active_and_sampled_slots() -> anyhow::Result<()> {
    let (session, turn, _rx_event) = hook_test_session().await;
    let manager = &session.services.unified_exec_manager;

    let context = test_context(&session, &turn, "call-release-active");
    let owner = manager.receipt_owner_for(&context)?;
    let receipt_id = manager
        .reserve_completion_receipt(owner.clone(), /*process_id*/ -5001)
        .await?;
    manager
        .release_completion_receipt(receipt_id, &owner)
        .await?;
    assert_eq!(
        manager.read_retained_output(receipt_id, &owner).await,
        Err(ReceiptError::Cancelled {
            reason: CancellationReason::Released,
        })
    );

    let (sampled_id, sampled_owner) = sampled_output_receipt(
        &session,
        &turn,
        "call-release-sampled",
        /*process_id*/ -5002,
        b"sampled-output",
    )
    .await?;
    manager
        .release_completion_receipt(sampled_id, &sampled_owner)
        .await?;
    assert_eq!(
        manager
            .read_retained_output(sampled_id, &sampled_owner)
            .await,
        Err(ReceiptError::AlreadyConsumed)
    );
    assert_eq!(manager.receipt_capacity_used().await?, 0);
    Ok(())
}

#[tokio::test]
async fn terminal_stdin_claim_consumes_the_single_claim_first() -> anyhow::Result<()> {
    let (session, turn, _rx_event) = hook_test_session().await;
    let manager = &session.services.unified_exec_manager;
    let process_id = manager.allocate_process_id().await;
    let (receipt_id, owner) =
        reserve_and_arm(manager, &session, &turn, "call-stdin-first", process_id).await;
    let DriverProcess {
        process,
        stdout_tx,
        exit_tx,
        output_buffer,
    } = driver_process(/*channel_capacity*/ 8).await?;
    insert_process_entry(
        &session,
        &turn,
        Arc::clone(&process),
        process_id,
        "call-stdin-first",
        "stdin-first-proof",
        /*tty*/ false,
    )
    .await;
    let context = test_context(&session, &turn, "call-stdin-first");
    start_streaming_output(&process, &context);
    #[allow(deprecated)]
    let cwd = turn.cwd.clone().into();
    spawn_exit_watcher(
        Arc::clone(&process),
        &context,
        vec!["stdin-first-proof".to_string()],
        cwd,
        process_id,
        /*plugin_attribution*/ None,
        output_buffer,
        Instant::now(),
        /*network_denial_monitor*/ None,
        /*plugin_metrics_sidecar*/ None,
        Some(manager.watcher_receipt_hook(receipt_id, owner.clone())),
    );
    stdout_tx.send(b"stdin-terminal\n".to_vec())?;
    await_transcript_bytes(&process, b"stdin-terminal\n".len()).await;
    exit_tx.send(0).expect("send exit");
    drop(stdout_tx);
    await_receipt_status(manager, receipt_id, &owner, ReceiptStatus::Queued).await;

    let response =
        write_stdin_for_hooks(&session, &turn, process_id, "", /*yield_time_ms*/ 250).await?;
    assert_eq!(response.process_id, None);
    assert_eq!(
        manager.receipt_status(receipt_id, &owner),
        Ok(ReceiptStatus::Sampled {
            source: SamplingSource::TerminalStdinOutput,
        })
    );
    assert_eq!(
        manager.lease_pushed_completion(receipt_id, &owner),
        Err(ReceiptError::AlreadyConsumed)
    );
    Ok(())
}

#[tokio::test]
async fn terminal_stdin_and_pushed_claims_race_exactly_once() -> anyhow::Result<()> {
    let (session, turn, _rx_event) = hook_test_session().await;
    let manager = &session.services.unified_exec_manager;
    let process_id = manager.allocate_process_id().await;
    let (receipt_id, owner) =
        reserve_and_arm(manager, &session, &turn, "call-stdin-race", process_id).await;
    let DriverProcess {
        process,
        stdout_tx,
        exit_tx,
        output_buffer,
    } = driver_process(/*channel_capacity*/ 8).await?;
    insert_process_entry(
        &session,
        &turn,
        Arc::clone(&process),
        process_id,
        "call-stdin-race",
        "stdin-race-proof",
        /*tty*/ false,
    )
    .await;
    let context = test_context(&session, &turn, "call-stdin-race");
    start_streaming_output(&process, &context);
    #[allow(deprecated)]
    let cwd = turn.cwd.clone().into();
    spawn_exit_watcher(
        Arc::clone(&process),
        &context,
        vec!["stdin-race-proof".to_string()],
        cwd,
        process_id,
        /*plugin_attribution*/ None,
        output_buffer,
        Instant::now(),
        /*network_denial_monitor*/ None,
        /*plugin_metrics_sidecar*/ None,
        Some(manager.watcher_receipt_hook(receipt_id, owner.clone())),
    );
    exit_tx.send(0).expect("send exit");
    drop(stdout_tx);
    await_receipt_status(manager, receipt_id, &owner, ReceiptStatus::Queued).await;

    let (stdin_result, pushed_lease) = tokio::join!(
        write_stdin_for_hooks(&session, &turn, process_id, "", /*yield_time_ms*/ 250),
        async { manager.lease_pushed_completion(receipt_id, &owner) }
    );
    assert_eq!(stdin_result?.process_id, None);
    match pushed_lease {
        Ok(lease) => {
            manager.acknowledge_pushed_completion(&lease).await?;
        }
        Err(ReceiptError::AlreadyLeased | ReceiptError::AlreadyConsumed) => {}
        Err(err) => panic!("unexpected pushed lease error: {err:?}"),
    }

    assert!(
        matches!(
            manager.receipt_status(receipt_id, &owner),
            Ok(ReceiptStatus::Sampled { .. })
        ),
        "exactly one sampling path should win"
    );
    assert_eq!(
        manager.lease_pushed_completion(receipt_id, &owner),
        Err(ReceiptError::AlreadyConsumed)
    );
    assert_eq!(
        manager.receipt_store().lease_for_sampling(
            receipt_id,
            &owner,
            SamplingSource::TerminalStdinOutput,
        ),
        Err(ReceiptError::AlreadyConsumed)
    );
    Ok(())
}

#[tokio::test]
async fn release_cancels_receipt_and_keeps_process_running() -> anyhow::Result<()> {
    let (session, turn, rx_event) = hook_test_session().await;
    let manager = &session.services.unified_exec_manager;
    let process_id = manager.allocate_process_id().await;
    let (receipt_id, owner) =
        reserve_and_arm(manager, &session, &turn, "call-release", process_id).await;
    let DriverProcess {
        process,
        stdout_tx,
        exit_tx,
        output_buffer,
    } = driver_process(/*channel_capacity*/ 8).await?;
    insert_process_entry(
        &session,
        &turn,
        Arc::clone(&process),
        process_id,
        "call-release",
        "release-proof",
        /*tty*/ false,
    )
    .await;
    let context = test_context(&session, &turn, "call-release");
    start_streaming_output(&process, &context);
    #[allow(deprecated)]
    let cwd = turn.cwd.clone().into();
    spawn_exit_watcher(
        Arc::clone(&process),
        &context,
        vec!["release-proof".to_string()],
        cwd,
        process_id,
        /*plugin_attribution*/ None,
        output_buffer,
        Instant::now(),
        /*network_denial_monitor*/ None,
        /*plugin_metrics_sidecar*/ None,
        Some(manager.watcher_receipt_hook(receipt_id, owner.clone())),
    );

    manager
        .release_completion_receipt(receipt_id, &owner)
        .await?;
    assert_eq!(
        manager.receipt_status(receipt_id, &owner),
        Ok(ReceiptStatus::Cancelled {
            reason: CancellationReason::Released,
        })
    );
    assert!(!process.has_exited(), "release must not kill the process");
    assert!(
        manager
            .process_store
            .lock()
            .await
            .processes
            .contains_key(&process_id),
        "release must keep the process entry"
    );

    // A later exit queues nothing.
    exit_tx.send(0).expect("send exit");
    drop(stdout_tx);
    await_command_end(&rx_event).await?;
    assert_eq!(
        manager.receipt_status(receipt_id, &owner),
        Ok(ReceiptStatus::Cancelled {
            reason: CancellationReason::Released,
        })
    );
    assert_eq!(
        manager.lease_pushed_completion(receipt_id, &owner),
        Err(ReceiptError::Cancelled {
            reason: CancellationReason::Released,
        })
    );

    // Unknown and foreign releases are refused.
    let separate = CompletionReceiptStore::default();
    let unknown_owner =
        manager.receipt_owner_for(&test_context(&session, &turn, "call-release"))?;
    let unknown_id = separate.reserve(unknown_owner.clone())?;
    assert_eq!(
        manager
            .release_completion_receipt(unknown_id, &unknown_owner)
            .await,
        Err(ReceiptError::UnknownReceipt)
    );
    let other_manager = UnifiedExecProcessManager::default();
    for foreign_owner in foreign_receipt_owners(
        session.thread_id,
        other_manager.receipt_generation,
        "call-release",
    ) {
        assert_eq!(
            manager
                .release_completion_receipt(receipt_id, &foreign_owner)
                .await,
            Err(ReceiptError::ForeignOwner)
        );
    }
    // Releasing again is idempotent.
    assert_eq!(
        manager.release_completion_receipt(receipt_id, &owner).await,
        Ok(())
    );
    Ok(())
}

#[tokio::test]
async fn terminate_process_cancels_before_killing() -> anyhow::Result<()> {
    let (session, turn, _rx_event) = hook_test_session().await;
    let manager = &session.services.unified_exec_manager;
    let process_id = manager.allocate_process_id().await;
    let (receipt_id, owner) =
        reserve_and_arm(manager, &session, &turn, "call-terminate", process_id).await;
    let backend = GatedExecProcess::new(process_id);
    backend.block_terminate.store(true, Ordering::SeqCst);
    let process = gated_unified_process(Arc::clone(&backend)).await?;
    insert_process_entry(
        &session,
        &turn,
        process,
        process_id,
        "call-terminate",
        "terminate-proof",
        /*tty*/ false,
    )
    .await;

    let terminate_session = Arc::clone(&session);
    let terminate_task = tokio::spawn(async move {
        terminate_session
            .services
            .unified_exec_manager
            .terminate_process(process_id)
            .await
    });
    await_flag(&backend.terminate_called).await;
    // The kill is blocked inside the backend: cancellation precedes it.
    assert_eq!(
        manager.receipt_status(receipt_id, &owner),
        Ok(ReceiptStatus::Cancelled {
            reason: CancellationReason::OwnerStopped,
        })
    );
    backend.release_terminate.notify_one();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), terminate_task)
            .await
            .expect("terminate should finish")
            .expect("terminate task should not panic"),
        "terminate should succeed"
    );
    assert!(
        !manager
            .process_store
            .lock()
            .await
            .processes
            .contains_key(&process_id)
    );
    assert_eq!(manager.receipt_capacity_used().await?, 0);
    assert_eq!(
        manager.lease_pushed_completion(receipt_id, &owner),
        Err(ReceiptError::Cancelled {
            reason: CancellationReason::OwnerStopped,
        })
    );
    Ok(())
}

#[tokio::test]
async fn interrupt_cancels_before_signalling() -> anyhow::Result<()> {
    let (session, turn, _rx_event) = hook_test_session().await;
    let manager = &session.services.unified_exec_manager;
    let process_id = manager.allocate_process_id().await;
    let (receipt_id, owner) =
        reserve_and_arm(manager, &session, &turn, "call-interrupt", process_id).await;
    let backend = GatedExecProcess::new(process_id);
    backend.block_signal.store(true, Ordering::SeqCst);
    let process = gated_unified_process(Arc::clone(&backend)).await?;
    insert_process_entry(
        &session,
        &turn,
        process,
        process_id,
        "call-interrupt",
        "interrupt-proof",
        /*tty*/ false,
    )
    .await;

    let stdin_session = Arc::clone(&session);
    let stdin_turn = Arc::clone(&turn);
    let stdin_task = tokio::spawn(async move {
        write_stdin_for_hooks(
            &stdin_session,
            &stdin_turn,
            process_id,
            "\u{3}",
            /*yield_time_ms*/ 250,
        )
        .await
    });
    await_flag(&backend.signal_called).await;
    // The signal is blocked inside the backend: cancellation precedes it.
    assert_eq!(
        manager.receipt_status(receipt_id, &owner),
        Ok(ReceiptStatus::Cancelled {
            reason: CancellationReason::Interrupted,
        })
    );
    backend.release_signal.notify_one();
    let response = tokio::time::timeout(Duration::from_secs(5), stdin_task)
        .await
        .expect("write_stdin should finish")
        .expect("write_stdin task should not panic")?;
    assert_eq!(response.process_id, Some(process_id));
    assert_eq!(
        manager.lease_pushed_completion(receipt_id, &owner),
        Err(ReceiptError::Cancelled {
            reason: CancellationReason::Interrupted,
        })
    );
    Ok(())
}

#[tokio::test]
async fn shutdown_cancels_all_receipts_and_frees_slots() -> anyhow::Result<()> {
    let (session, turn, _rx_event) = hook_test_session().await;
    let manager = &session.services.unified_exec_manager;
    let mut backends = Vec::new();
    let mut bound = Vec::new();
    for (index, call_id) in ["call-shutdown-a", "call-shutdown-b"]
        .into_iter()
        .enumerate()
    {
        let process_id = manager.allocate_process_id().await;
        bound.push(reserve_and_arm(manager, &session, &turn, call_id, process_id).await);
        let backend = GatedExecProcess::new(process_id);
        let process = gated_unified_process(Arc::clone(&backend)).await?;
        insert_process_entry(
            &session,
            &turn,
            process,
            process_id,
            call_id,
            &format!("shutdown-proof-{index}"),
            /*tty*/ false,
        )
        .await;
        backends.push(backend);
    }
    let (sampled_id, sampled_owner) = sampled_output_receipt(
        &session,
        &turn,
        "call-shutdown-sampled",
        /*process_id*/ -6000,
        b"shutdown-sampled",
    )
    .await?;
    assert_eq!(manager.receipt_capacity_used().await?, 3);

    manager.terminate_all_processes().await;

    for backend in &backends {
        await_flag(&backend.terminate_called).await;
    }
    for (receipt_id, owner) in &bound {
        assert_eq!(
            manager.receipt_status(*receipt_id, owner),
            Ok(ReceiptStatus::Cancelled {
                reason: CancellationReason::Shutdown,
            })
        );
    }
    assert_eq!(manager.receipt_capacity_used().await?, 0);
    assert_eq!(
        manager
            .read_retained_output(sampled_id, &sampled_owner)
            .await,
        Err(ReceiptError::AlreadyConsumed)
    );
    for index in 0..MAX_COMPLETION_RECEIPTS {
        let context = test_context(&session, &turn, &format!("call-shutdown-fill-{index}"));
        let owner = manager.receipt_owner_for(&context)?;
        manager
            .reserve_completion_receipt(owner, -(7000 + index as i32))
            .await?;
    }
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn default_launches_reserve_no_receipts() -> anyhow::Result<()> {
    skip_if_sandbox!(Ok(()));
    let (session, turn, _rx_event) = hook_test_session().await;
    for index in 0..70 {
        let response = exec_command_for_hooks(
            &session,
            &turn,
            &format!("call-default-{index}"),
            &format!("echo default-{index}"),
            /*yield_time_ms*/ 5_000,
            ExecCompletionMode::Default,
        )
        .await?;
        assert_eq!(response.process_id, None);
        assert_eq!(response.exit_code, Some(0));
    }

    let manager = &session.services.unified_exec_manager;
    // A live default launch binds no receipt either: inline delivery frees
    // slots, so only a yielded process distinguishes default from opted-in.
    let live = exec_command_for_hooks(
        &session,
        &turn,
        "call-default-live",
        "sleep 30",
        /*yield_time_ms*/ 250,
        ExecCompletionMode::Default,
    )
    .await?;
    let live_pid = live.process_id.expect("live default process should yield");
    assert!(
        manager.receipt_for_process(live_pid).await.is_none(),
        "default launch must not bind a receipt"
    );
    assert!(manager.terminate_process(live_pid).await);

    assert_eq!(manager.receipt_capacity_used().await?, 0);
    for index in 0..MAX_COMPLETION_RECEIPTS {
        let context = test_context(&session, &turn, &format!("call-default-fill-{index}"));
        let owner = manager.receipt_owner_for(&context)?;
        manager
            .reserve_completion_receipt(owner, -(8000 + index as i32))
            .await?;
    }
    Ok(())
}

#[tokio::test]
async fn notification_owner_resolves_launch_owner_across_tool_calls() -> anyhow::Result<()> {
    let (session, turn, _rx_event) = hook_test_session().await;
    let manager = &session.services.unified_exec_manager;
    let launch = test_context(&session, &turn, "launch-call");
    let owner = manager.receipt_owner_for(&launch)?;
    let receipt_id = manager
        .reserve_completion_receipt(owner.clone(), /*process_id*/ 5001)
        .await?;

    // A later exec_notification call carries a different call id; only the
    // thread and runtime generation are verified against the caller.
    let tool_call = test_context(&session, &turn, "exec-notification-call");
    assert_eq!(
        manager
            .notification_owner_for_receipt(receipt_id, &tool_call)
            .await?,
        owner
    );

    // Unknown: an id reserved on a separate store.
    let separate = CompletionReceiptStore::default();
    let unknown_id = separate.reserve(owner.clone())?;
    assert_eq!(
        manager
            .notification_owner_for_receipt(unknown_id, &tool_call)
            .await,
        Err(ReceiptError::UnknownReceipt)
    );

    // Foreign: same store, different thread, generation, or (with a
    // generation mismatch) call.
    let other_manager = UnifiedExecProcessManager::default();
    for (index, foreign_owner) in foreign_receipt_owners(
        session.thread_id,
        other_manager.receipt_generation,
        "launch-call",
    )
    .into_iter()
    .enumerate()
    {
        let foreign_id = manager
            .reserve_completion_receipt(foreign_owner, /*process_id*/ -(5100 + index as i32))
            .await?;
        assert_eq!(
            manager
                .notification_owner_for_receipt(foreign_id, &tool_call)
                .await,
            Err(ReceiptError::ForeignOwner)
        );
    }
    Ok(())
}

#[tokio::test]
async fn enqueue_published_completion_enqueues_live_queued_receipt() -> anyhow::Result<()> {
    let (session, turn, _rx_event) = hook_test_session().await;
    let manager = &session.services.unified_exec_manager;
    let context = test_context(&session, &turn, "launch-call");
    let owner = manager.receipt_owner_for(&context)?;
    let receipt_id = manager
        .reserve_completion_receipt(owner.clone(), /*process_id*/ 5201)
        .await?;
    assert_eq!(
        manager.receipt_store().resolve_initial_response(
            receipt_id,
            &owner,
            InitialResponseDecision::Arm,
        ),
        Ok(InitialResponseOutcome::Armed)
    );
    let completion = TerminalCompletion {
        exit_code: Some(3),
        timed_out: false,
    };
    manager
        .receipt_store()
        .publish_exit(receipt_id, &owner, completion)?;
    manager
        .watcher_receipt_hook(receipt_id, owner.clone())
        .hooks
        .lock()
        .await
        .retention
        .insert_pending(receipt_id, owner.clone(), b"kept".to_vec(), /*omitted_bytes*/ 0);

    // Suppress the idle wake: an active turn makes the wake attempt a no-op,
    // so this test observes the enqueue decision deterministically.
    *session.active_turn.lock().await = Some(crate::state::ActiveTurn::default());
    manager
        .enqueue_published_completion(
            &session,
            receipt_id,
            &owner,
            completion,
            /*process_id*/ 5201,
            Some("boom".to_string()),
        )
        .await;
    *session.active_turn.lock().await = None;

    assert!(session.input_queue.has_pending_mailbox_items().await);
    let leases = session.input_queue.lease_runtime_notifications().await;
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].receipt_id(), receipt_id);
    assert_eq!(leases[0].completion().process_id, 5201);
    assert_eq!(leases[0].completion().exit_code, Some(3));
    assert_eq!(leases[0].completion().failure.as_deref(), Some("boom"));
    assert!(
        manager
            .read_retained_output(receipt_id, &owner)
            .await
            .is_ok(),
        "enqueue should keep retained output"
    );
    Ok(())
}

#[tokio::test]
async fn enqueue_published_completion_skips_disarmed_receipt() -> anyhow::Result<()> {
    let (session, turn, _rx_event) = hook_test_session().await;
    let manager = &session.services.unified_exec_manager;
    let context = test_context(&session, &turn, "launch-call");
    let owner = manager.receipt_owner_for(&context)?;
    let receipt_id = manager
        .reserve_completion_receipt(owner.clone(), /*process_id*/ 5301)
        .await?;
    manager.receipt_store().resolve_initial_response(
        receipt_id,
        &owner,
        InitialResponseDecision::Arm,
    )?;
    manager.receipt_store().publish_exit(
        receipt_id,
        &owner,
        TerminalCompletion {
            exit_code: Some(0),
            timed_out: false,
        },
    )?;
    manager
        .watcher_receipt_hook(receipt_id, owner.clone())
        .hooks
        .lock()
        .await
        .retention
        .insert_pending(receipt_id, owner.clone(), b"kept".to_vec(), /*omitted_bytes*/ 0);
    manager
        .release_completion_receipt(receipt_id, &owner)
        .await?;

    // No active-turn guard needed: a disarmed receipt earns no wake attempt.
    manager
        .enqueue_published_completion(
            &session,
            receipt_id,
            &owner,
            TerminalCompletion {
                exit_code: Some(0),
                timed_out: false,
            },
            /*process_id*/ 5301,
            None,
        )
        .await;

    assert!(
        !session.input_queue.has_pending_mailbox_items().await,
        "disarmed receipt should earn no mailbox entry"
    );
    assert_eq!(
        manager.read_retained_output(receipt_id, &owner).await,
        Err(ReceiptError::Cancelled {
            reason: CancellationReason::Released
        })
    );
    Ok(())
}

#[tokio::test]
async fn enqueue_published_completion_drops_ghost_retention() -> anyhow::Result<()> {
    let (session, turn, _rx_event) = hook_test_session().await;
    let manager = &session.services.unified_exec_manager;
    let context = test_context(&session, &turn, "launch-call");
    let owner = manager.receipt_owner_for(&context)?;
    let receipt_id = manager
        .reserve_completion_receipt(owner.clone(), /*process_id*/ 5401)
        .await?;
    manager.receipt_store().resolve_initial_response(
        receipt_id,
        &owner,
        InitialResponseDecision::Arm,
    )?;
    manager.receipt_store().publish_exit(
        receipt_id,
        &owner,
        TerminalCompletion {
            exit_code: Some(0),
            timed_out: false,
        },
    )?;
    // Simulate a release racing the watcher insert: the store is cancelled
    // but retention was inserted afterwards anyway.
    manager
        .receipt_store()
        .cancel(receipt_id, &owner, CancellationReason::Released)?;
    manager
        .watcher_receipt_hook(receipt_id, owner.clone())
        .hooks
        .lock()
        .await
        .retention
        .insert_pending(receipt_id, owner.clone(), b"ghost".to_vec(), /*omitted_bytes*/ 0);

    manager
        .enqueue_published_completion(
            &session,
            receipt_id,
            &owner,
            TerminalCompletion {
                exit_code: Some(0),
                timed_out: false,
            },
            /*process_id*/ 5401,
            None,
        )
        .await;

    assert!(
        !session.input_queue.has_pending_mailbox_items().await,
        "disarmed receipt should earn no mailbox entry"
    );
    assert_eq!(
        manager
            .receipt_hooks
            .lock()
            .await
            .retention
            .lookup(receipt_id, &owner),
        crate::unified_exec::receipt_output::RetentionLookup::Absent,
        "ghost retention should be dropped"
    );
    Ok(())
}

#[tokio::test]
async fn default_launch_acknowledges_no_receipt() -> anyhow::Result<()> {
    let (session, turn, _rx_event) = hook_test_session().await;
    let manager = &session.services.unified_exec_manager;
    let output = exec_command_for_hooks(
        &session,
        &turn,
        "call-default-no-receipt",
        "sleep 5",
        /*yield_time_ms*/ 250,
        ExecCompletionMode::Default,
    )
    .await?;
    let process_id = output.process_id.expect("default launch should yield");
    assert_eq!(
        output.completion_receipt, None,
        "default launch should acknowledge no receipt"
    );
    assert!(
        manager.receipt_for_process(process_id).await.is_none(),
        "default launch should bind no receipt"
    );
    assert_eq!(manager.receipt_capacity_used().await?, 0);
    assert!(manager.terminate_process(process_id).await);
    Ok(())
}

#[tokio::test]
async fn opted_in_launch_arms_and_release_silences_later_exit() -> anyhow::Result<()> {
    let (session, turn, rx_event) = hook_test_session().await;
    let manager = &session.services.unified_exec_manager;
    let output = exec_command_for_hooks(
        &session,
        &turn,
        "call-opted-in",
        "sleep 30",
        /*yield_time_ms*/ 250,
        ExecCompletionMode::NotifyOnExit,
    )
    .await?;
    let process_id = output.process_id.expect("opted-in launch should yield");
    let handle = output
        .completion_receipt
        .clone()
        .expect("opted-in yield should acknowledge a receipt");
    assert_eq!(manager.receipt_capacity_used().await?, 1);
    let (receipt_id, owner) = manager
        .receipt_for_process(process_id)
        .await
        .expect("opted-in launch should bind a receipt");
    assert_eq!(receipt_id.model_handle(), handle);

    manager
        .release_completion_receipt(receipt_id, &owner)
        .await?;
    assert_eq!(
        manager.receipt_capacity_used().await?,
        0,
        "release should free the slot"
    );
    // The process survives release: termination still finds it alive.
    assert!(
        manager.terminate_process(process_id).await,
        "release should not kill the process"
    );
    // The watcher ran to its terminal event; a released receipt publishes
    // nothing, so no wake or retention follows.
    await_command_end(&rx_event).await?;
    assert!(
        !session.input_queue.has_pending_mailbox_items().await,
        "later exit after release should produce no wake"
    );
    assert_eq!(
        manager.read_retained_output(receipt_id, &owner).await,
        Err(ReceiptError::Cancelled {
            reason: CancellationReason::Released
        })
    );
    Ok(())
}

#[tokio::test]
async fn opted_in_exit_enqueues_wake_without_turn() -> anyhow::Result<()> {
    let (session, turn, rx_event) = hook_test_session().await;
    // Suppress the idle wake so the enqueued entry stays observable.
    *session.active_turn.lock().await = Some(crate::state::ActiveTurn::default());
    let output = exec_command_for_hooks(
        &session,
        &turn,
        "call-opted-in-exit",
        "sleep 1",
        /*yield_time_ms*/ 250,
        ExecCompletionMode::NotifyOnExit,
    )
    .await?;
    assert!(
        output.completion_receipt.is_some(),
        "opted-in yield should acknowledge a receipt"
    );
    await_command_end(&rx_event).await?;
    assert!(
        session.input_queue.has_pending_mailbox_items().await,
        "opted-in exit should enqueue a wake"
    );
    let leases = session.input_queue.lease_runtime_notifications().await;
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].completion().exit_code, Some(0));
    *session.active_turn.lock().await = None;
    Ok(())
}

#[tokio::test]
async fn sixty_fifth_opted_in_launch_refused_before_execution() -> anyhow::Result<()> {
    let (session, turn, _rx_event) = hook_test_session().await;
    let manager = &session.services.unified_exec_manager;
    for index in 0..MAX_COMPLETION_RECEIPTS {
        let context = test_context(&session, &turn, &format!("call-fill-{index}"));
        let owner = manager.receipt_owner_for(&context)?;
        manager
            .reserve_completion_receipt(owner, -(6000 + index as i32))
            .await?;
    }
    assert_eq!(
        manager.receipt_capacity_used().await?,
        MAX_COMPLETION_RECEIPTS
    );

    let Err(err) = exec_command_for_hooks(
        &session,
        &turn,
        "call-65th",
        "echo hi",
        /*yield_time_ms*/ 250,
        ExecCompletionMode::NotifyOnExit,
    )
    .await
    else {
        panic!("65th opted-in launch should be refused");
    };
    assert!(
        matches!(
            err,
            UnifiedExecError::ReceiptCapacityExceeded { capacity }
                if capacity == MAX_COMPLETION_RECEIPTS
        ),
        "expected receipt capacity refusal, got {err:?}"
    );
    assert_eq!(
        manager.receipt_capacity_used().await?,
        MAX_COMPLETION_RECEIPTS
    );
    assert!(
        manager.list_processes().await.is_empty(),
        "refusal should precede process launch"
    );

    // The process cap is independent: a default launch still runs while
    // receipt slots are full.
    let output = exec_command_for_hooks(
        &session,
        &turn,
        "call-default-while-full",
        "echo hi",
        /*yield_time_ms*/ 250,
        ExecCompletionMode::Default,
    )
    .await?;
    assert_eq!(output.completion_receipt, None);
    Ok(())
}

use crate::browser::{BrowserAdmission, BrowserRoutePrefix};
use crate::connection::{
    AdmissionRequest, ConnectionState, MAX_CONCURRENT_CONTROL_HANDLERS, SNAPSHOT_CHANNEL_DEPTH,
    SnapshotPublication, handle_connection, snapshot_publication,
};
use crate::control::{
    ControlContext, ControlService, ControlServiceError, MatchControlService,
    RejectMatchControlService,
};
use crate::host::{MatchHost, MatchId};
use crate::host_recovery::{MatchHostRecoveryPlan, consume_recovery_bundle, write_recovery_bundle};
use crate::recovery::RecoveryImage;
use crate::runtime::{MatchRuntime, RuntimeError};
use crate::simulation::GameSimulation;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock, Semaphore, broadcast, mpsc, oneshot};
use tokio::task::{JoinSet, spawn_blocking};
use tokio::time::MissedTickBehavior;
use wtransport::{Endpoint, Identity, ServerConfig};

#[derive(Clone, Debug)]
pub struct MatchHostWebTransportConfig {
    pub port: u16,
    pub certificate_pem: PathBuf,
    pub private_key_pem: PathBuf,
    pub route_prefix: BrowserRoutePrefix,
    pub drain_grace: Duration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MatchHostTransportError {
    Identity(String),
    Endpoint(String),
    Recovery(String),
    EmptyHost,
    HostAlreadyDraining,
    InvalidTickRate(MatchId),
    PlayerCapacityTooLarge { match_id: MatchId, capacity: usize },
}

impl fmt::Display for MatchHostTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identity(error) => write!(formatter, "TLS identity error: {error}"),
            Self::Endpoint(error) => write!(formatter, "WebTransport endpoint error: {error}"),
            Self::Recovery(error) => write!(formatter, "host recovery error: {error}"),
            Self::EmptyHost => write!(formatter, "match host must contain at least one match"),
            Self::HostAlreadyDraining => {
                write!(
                    formatter,
                    "match host cannot start transport while already draining"
                )
            }
            Self::InvalidTickRate(match_id) => {
                write!(
                    formatter,
                    "match {match_id} has a zero simulation tick rate"
                )
            }
            Self::PlayerCapacityTooLarge { match_id, capacity } => write!(
                formatter,
                "match {match_id} player capacity {capacity} exceeds wire limit"
            ),
        }
    }
}

impl Error for MatchHostTransportError {}

struct HostedMatch<S> {
    runtime: Arc<Mutex<MatchRuntime<S>>>,
    snapshots: broadcast::Sender<SnapshotPublication>,
}

struct HostedServerState<S> {
    matches: Arc<BTreeMap<MatchId, HostedMatch<S>>>,
    admission_gate: Arc<RwLock<bool>>,
    control: Arc<dyn MatchControlService>,
    control_handlers: Arc<Semaphore>,
    shutdown: broadcast::Sender<()>,
}

impl<S> Clone for HostedServerState<S> {
    fn clone(&self) -> Self {
        Self {
            matches: Arc::clone(&self.matches),
            admission_gate: Arc::clone(&self.admission_gate),
            control: Arc::clone(&self.control),
            control_handlers: Arc::clone(&self.control_handlers),
            shutdown: self.shutdown.clone(),
        }
    }
}

impl<S: GameSimulation> HostedServerState<S> {
    async fn with_runtime_mut<R>(
        &self,
        match_id: &MatchId,
        operation: impl FnOnce(&mut MatchRuntime<S>) -> R,
    ) -> Option<R> {
        let hosted = self.matches.get(match_id)?;
        let mut runtime = hosted.runtime.lock().await;
        Some(operation(&mut runtime))
    }

    fn connection_state(&self, match_id: &MatchId) -> Option<ConnectionState<S>> {
        let hosted = self.matches.get(match_id)?;
        Some(ConnectionState {
            runtime: Arc::clone(&hosted.runtime),
            control: Arc::new(HostedControl {
                match_id: match_id.clone(),
                service: Arc::clone(&self.control),
            }),
            control_handlers: Arc::clone(&self.control_handlers),
            snapshots: hosted.snapshots.clone(),
            shutdown: self.shutdown.clone(),
            admission_gate: Some(Arc::clone(&self.admission_gate)),
        })
    }

    async fn begin_process_drain(&self) {
        let mut draining = self.admission_gate.write().await;
        *draining = true;
        for hosted in self.matches.values() {
            hosted.runtime.lock().await.begin_drain();
        }
    }
}

// Bind match identity once; the shared connection module only knows ControlService.
struct HostedControl {
    match_id: MatchId,
    service: Arc<dyn MatchControlService>,
}

impl ControlService for HostedControl {
    fn handle(
        &self,
        context: ControlContext,
        payload: &[u8],
    ) -> Result<Vec<u8>, ControlServiceError> {
        self.service.handle(&self.match_id, context, payload)
    }
}

pub async fn serve_match_host<S>(
    host: MatchHost<S>,
    config: MatchHostWebTransportConfig,
) -> Result<(), MatchHostTransportError>
where
    S: GameSimulation,
{
    serve_match_host_with_control(host, RejectMatchControlService, config).await
}

pub async fn serve_match_host_with_control<S, C>(
    host: MatchHost<S>,
    control: C,
    config: MatchHostWebTransportConfig,
) -> Result<(), MatchHostTransportError>
where
    S: GameSimulation,
    C: MatchControlService,
{
    let (shutdown_sender, shutdown_receiver) = mpsc::channel(1);
    let result =
        serve_match_host_with_control_and_shutdown(host, control, config, shutdown_receiver).await;
    drop(shutdown_sender);
    result
}

pub async fn serve_match_host_with_shutdown<S>(
    host: MatchHost<S>,
    config: MatchHostWebTransportConfig,
    shutdown_requests: mpsc::Receiver<()>,
) -> Result<(), MatchHostTransportError>
where
    S: GameSimulation,
{
    serve_match_host_with_control_and_shutdown(
        host,
        RejectMatchControlService,
        config,
        shutdown_requests,
    )
    .await
}

pub async fn serve_match_host_with_control_and_shutdown<S, C>(
    host: MatchHost<S>,
    control: C,
    config: MatchHostWebTransportConfig,
    shutdown_requests: mpsc::Receiver<()>,
) -> Result<(), MatchHostTransportError>
where
    S: GameSimulation,
    C: MatchControlService,
{
    serve_match_host_with_control_and_shutdown_notifying_ready(
        host,
        control,
        config,
        shutdown_requests,
        None,
        None,
    )
    .await
}

pub(crate) async fn serve_match_host_with_control_and_shutdown_notifying_ready<S, C>(
    host: MatchHost<S>,
    control: C,
    config: MatchHostWebTransportConfig,
    mut shutdown_requests: mpsc::Receiver<()>,
    ready: Option<oneshot::Sender<()>>,
    recovery: Option<MatchHostRecoveryPlan>,
) -> Result<(), MatchHostTransportError>
where
    S: GameSimulation,
    C: MatchControlService,
{
    let match_tick_rates = validate_host(&host)?;
    let identity = Identity::load_pemfiles(&config.certificate_pem, &config.private_key_pem)
        .await
        .map_err(|error| MatchHostTransportError::Identity(error.to_string()))?;
    let server_config = ServerConfig::builder()
        .with_bind_default(config.port)
        .with_identity(identity)
        .keep_alive_interval(Some(Duration::from_secs(3)))
        .build();
    let endpoint = Endpoint::server(server_config)
        .map_err(|error| MatchHostTransportError::Endpoint(error.to_string()))?;

    if recovery.as_ref().is_some_and(|plan| plan.consume_on_start) {
        let directory = recovery
            .as_ref()
            .expect("checked recovery plan")
            .directory
            .clone();
        let consumption = spawn_blocking(move || {
            let result = consume_recovery_bundle(&directory);
            let active_exists = directory.exists();
            let consumed_exists = directory.file_name().is_some_and(|file_name| {
                let mut consumed_name = file_name.to_os_string();
                consumed_name.push(".consumed");
                directory.with_file_name(consumed_name).exists()
            });
            (result, active_exists, consumed_exists)
        })
        .await;
        match consumption {
            Ok((Ok(()), _, _)) => {}
            Ok((Err(error), false, false)) => {
                eprintln!(
                    "hosted recovery bundle was fully consumed but the final durability step reported an error; continuing with the already-restored authoritative state: {error}"
                );
            }
            Ok((Err(error), _, _)) => {
                return Err(MatchHostTransportError::Recovery(error.to_string()));
            }
            Err(error) => {
                return Err(MatchHostTransportError::Recovery(format!(
                    "recovery consumption task failed: {error}"
                )));
            }
        }
    }

    let (shutdown, _) = broadcast::channel::<()>(1);
    let state = HostedServerState {
        matches: Arc::new(isolate_hosted_runtimes(host)),
        admission_gate: Arc::new(RwLock::new(false)),
        control: Arc::new(control),
        control_handlers: Arc::new(Semaphore::new(MAX_CONCURRENT_CONTROL_HANDLERS)),
        shutdown,
    };
    let mut tick_tasks = spawn_tick_loops(state.clone(), &match_tick_rates);
    let route_prefix = config.route_prefix.clone();
    if let Some(ready) = ready {
        let _ = ready.send(());
    }

    let mut connections = JoinSet::new();
    let incoming_session = |incoming: wtransport::endpoint::IncomingSession| {
        let state = state.clone();
        let route_prefix = route_prefix.clone();
        async move {
            let request = match incoming.await {
                Ok(request) => request,
                Err(error) => {
                    eprintln!("WebTransport negotiation failed: {error}");
                    return;
                }
            };
            let route = match route_prefix.parse(request.path()) {
                Ok(Some(route)) if state.matches.contains_key(&route.match_id) => route,
                Ok(Some(_)) | Ok(None) | Err(_) => {
                    let _ = request.not_found().await;
                    return;
                }
            };
            let connection = match request.accept().await {
                Ok(connection) => connection,
                Err(error) => {
                    eprintln!("WebTransport acceptance failed: {error}");
                    return;
                }
            };
            let Some(connection_state) = state.connection_state(&route.match_id) else {
                return;
            };
            let admission = match route.admission {
                BrowserAdmission::New => AdmissionRequest::New,
                BrowserAdmission::Reconnect(token) => AdmissionRequest::Reconnect(token),
            };
            if let Err(error) = handle_connection(connection, connection_state, admission).await {
                eprintln!("hosted game session failed: {error}");
            }
        }
    };

    let mut shutdown_channel_open = true;
    loop {
        tokio::select! {
            incoming = endpoint.accept() => { connections.spawn(incoming_session(incoming)); },
            completed = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = completed {
                    eprintln!("connection task failed: {error}");
                }
            },
            shutdown = shutdown_requests.recv(), if shutdown_channel_open => {
                let Some(()) = shutdown else {
                    shutdown_channel_open = false;
                    continue;
                };

                state.begin_process_drain().await;
                let drain_deadline = tokio::time::sleep(config.drain_grace);
                tokio::pin!(drain_deadline);
                loop {
                    tokio::select! {
                        _ = &mut drain_deadline => break,
                        incoming = endpoint.accept() => { connections.spawn(incoming_session(incoming)); },
                        completed = connections.join_next(), if !connections.is_empty() => {
                            if let Some(Err(error)) = completed {
                                eprintln!("connection task failed: {error}");
                            }
                        },
                    }
                }

                if let Some(plan) = &recovery
                    && let Err(error) = persist_host_recovery(&state, &plan.directory).await
                {
                    eprintln!(
                        "graceful hosted shutdown aborted because recovery persistence failed: {error}"
                    );
                    continue;
                }

                let _ = state.shutdown.send(());
                tick_tasks.shutdown().await;
                connections.shutdown().await;
                return Ok(());
            }
        }
    }
}

async fn persist_host_recovery<S: GameSimulation>(
    state: &HostedServerState<S>,
    directory: &Path,
) -> Result<(), String> {
    let mut images = BTreeMap::<MatchId, RecoveryImage>::new();
    for (id, hosted) in state.matches.iter() {
        let image = {
            let mut runtime = hosted.runtime.lock().await;
            runtime.freeze_for_recovery();
            runtime.recovery_image()
        };
        match image {
            Ok(image) => {
                images.insert(id.clone(), image);
            }
            Err(error) => {
                resume_host_after_failed_recovery(state).await;
                return Err(format!("match {id}: {error}"));
            }
        }
    }

    let directory = directory.to_path_buf();
    let persistence = spawn_blocking(move || {
        let existed_before = directory.exists();
        let result = write_recovery_bundle(&directory, &images);
        let exists_after = directory.exists();
        (result, existed_before, exists_after)
    })
    .await;
    let result = match persistence {
        Ok((Ok(()), _, _)) => Ok(()),
        Ok((Err(error), false, true)) => {
            eprintln!(
                "hosted recovery bundle exists after an atomic publish reported an error; treating it as committed to avoid resuming beyond that snapshot: {error}"
            );
            Ok(())
        }
        Ok((Err(error), _, _)) => Err(error.to_string()),
        Err(error) => Err(format!("hosted recovery persistence task failed: {error}")),
    };
    if result.is_err() {
        resume_host_after_failed_recovery(state).await;
    }
    result
}

async fn resume_host_after_failed_recovery<S: GameSimulation>(state: &HostedServerState<S>) {
    for hosted in state.matches.values() {
        hosted.runtime.lock().await.resume_after_failed_recovery();
    }
}

fn validate_host<S: GameSimulation>(
    host: &MatchHost<S>,
) -> Result<Vec<(MatchId, u16)>, MatchHostTransportError> {
    if host.is_empty() {
        return Err(MatchHostTransportError::EmptyHost);
    }
    if host.is_draining() {
        return Err(MatchHostTransportError::HostAlreadyDraining);
    }

    host.statuses()
        .into_iter()
        .map(|status| {
            let runtime = host
                .runtime(&status.id)
                .expect("host status must reference an existing runtime");
            let tick_hz = runtime.tick_hz();
            if tick_hz == 0 {
                return Err(MatchHostTransportError::InvalidTickRate(status.id));
            }
            if runtime.max_players() > usize::from(u16::MAX) {
                return Err(MatchHostTransportError::PlayerCapacityTooLarge {
                    match_id: status.id,
                    capacity: runtime.max_players(),
                });
            }
            Ok((status.id, tick_hz))
        })
        .collect()
}

fn isolate_hosted_runtimes<S: GameSimulation>(
    host: MatchHost<S>,
) -> BTreeMap<MatchId, HostedMatch<S>> {
    host.into_runtimes()
        .into_iter()
        .map(|(match_id, runtime)| {
            let (snapshots, _) = broadcast::channel::<SnapshotPublication>(SNAPSHOT_CHANNEL_DEPTH);
            (
                match_id,
                HostedMatch {
                    runtime: Arc::new(Mutex::new(runtime)),
                    snapshots,
                },
            )
        })
        .collect()
}

fn spawn_tick_loops<S: GameSimulation>(
    state: HostedServerState<S>,
    match_tick_rates: &[(MatchId, u16)],
) -> JoinSet<()> {
    let mut tasks = JoinSet::new();
    for (match_id, tick_hz) in match_tick_rates {
        let state = state.clone();
        let match_id = match_id.clone();
        let tick_hz = *tick_hz;
        let snapshots = state
            .matches
            .get(&match_id)
            .expect("validated match must have a snapshot channel")
            .snapshots
            .clone();
        tasks.spawn(async move {
            let mut ticker =
                tokio::time::interval(Duration::from_micros(1_000_000_u64 / u64::from(tick_hz)));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                let (scope, snapshot) = match state
                    .with_runtime_mut(&match_id, |runtime| {
                        runtime.advance_tick().map(|snapshot| {
                            let scope = runtime.snapshot_scope();
                            (scope, snapshot)
                        })
                    })
                    .await
                {
                    Some(Ok(result)) => result,
                    Some(Err(RuntimeError::Frozen)) => continue,
                    Some(Err(error)) => {
                        eprintln!("authoritative tick failed for match {match_id}: {error}");
                        continue;
                    }
                    None => return,
                };
                match snapshot_publication(scope, snapshot) {
                    Ok(publication) => {
                        let _ = snapshots.send(publication);
                    }
                    Err(error) => {
                        eprintln!("snapshot encoding failed for match {match_id}: {error}")
                    }
                }
            }
        });
    }
    tasks
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DEFAULT_RECONNECT_GRACE_TICKS, DemoSimulation};

    fn host_with(ids: &[&str]) -> MatchHost<DemoSimulation> {
        let mut host = MatchHost::new(ids.len()).unwrap();
        for value in ids {
            host.insert(
                MatchId::new(*value).unwrap(),
                MatchRuntime::new(DemoSimulation::new(), DEFAULT_RECONNECT_GRACE_TICKS),
            )
            .unwrap();
        }
        host
    }

    #[test]
    fn host_validation_binds_one_tick_loop_per_match() {
        let host = host_with(&["alpha", "beta"]);
        let tick_rates = validate_host(&host).unwrap();
        assert_eq!(tick_rates.len(), 2);
        assert_eq!(tick_rates[0].0.as_str(), "alpha");
        assert_eq!(tick_rates[1].0.as_str(), "beta");
        assert!(tick_rates.iter().all(|(_, tick_hz)| *tick_hz > 0));
    }

    #[test]
    fn hosted_runtimes_have_independent_locks() {
        let matches = isolate_hosted_runtimes(host_with(&["alpha", "beta"]));
        let alpha = matches.get(&MatchId::new("alpha").unwrap()).unwrap();
        let beta = matches.get(&MatchId::new("beta").unwrap()).unwrap();

        let _alpha_guard = alpha.runtime.try_lock().unwrap();
        let _beta_guard = beta.runtime.try_lock().unwrap();
    }

    #[test]
    fn empty_or_pre_draining_hosts_fail_closed() {
        let empty = MatchHost::<DemoSimulation>::new(1).unwrap();
        assert_eq!(
            validate_host(&empty),
            Err(MatchHostTransportError::EmptyHost)
        );

        let mut draining = host_with(&["alpha"]);
        draining.begin_drain();
        assert_eq!(
            validate_host(&draining),
            Err(MatchHostTransportError::HostAlreadyDraining)
        );
    }
    #[tokio::test]
    async fn dropping_host_tick_owner_closes_every_snapshot_source() {
        let host = host_with(&["alpha", "beta"]);
        let rates = validate_host(&host).unwrap();
        let matches = isolate_hosted_runtimes(host);
        let mut publications: Vec<_> = matches
            .values()
            .map(|hosted| hosted.snapshots.subscribe())
            .collect();
        let (shutdown, _) = broadcast::channel(1);
        let state = HostedServerState {
            matches: Arc::new(matches),
            admission_gate: Arc::new(RwLock::new(false)),
            control: Arc::new(RejectMatchControlService),
            control_handlers: Arc::new(Semaphore::new(MAX_CONCURRENT_CONTROL_HANDLERS)),
            shutdown,
        };
        let ticks = spawn_tick_loops(state, &rates);
        for published in &mut publications {
            published.recv().await.unwrap();
        }
        drop(ticks);
        tokio::time::timeout(Duration::from_secs(1), async {
            for published in &mut publications {
                while published.recv().await.is_ok() {}
            }
        })
        .await
        .expect("cancelled host must release every tick task and runtime");
    }
}

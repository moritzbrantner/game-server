use crate::browser::{BrowserAdmission, BrowserOriginAllowlist, BrowserRoutePrefix};
use crate::connection::{
    AdmissionRequest, ConnectionState, MAX_CONCURRENT_CONTROL_HANDLERS, handle_connection,
};
use crate::control::{
    ControlContext, ControlService, ControlServiceError, MatchControlService,
    RejectMatchControlService,
};
use crate::host::{MatchHost, MatchId};
use crate::host_recovery::{MatchHostRecoveryPlan, consume_recovery_bundle, write_recovery_bundle};
use crate::live_host::LiveMatchHost;
use crate::recovery::RecoveryImage;
use crate::simulation::GameSimulation;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::task::{JoinSet, spawn_blocking};
use wtransport::{Endpoint, Identity, ServerConfig};

#[derive(Clone, Debug)]
pub struct MatchHostWebTransportConfig {
    pub port: u16,
    pub certificate_pem: PathBuf,
    pub private_key_pem: PathBuf,
    pub route_prefix: BrowserRoutePrefix,
    /// None permits native/local clients without Origin. Browser deployments should configure this.
    pub allowed_origins: Option<BrowserOriginAllowlist>,
    pub drain_grace: Duration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MatchHostTransportError {
    Identity(String),
    Endpoint(String),
    Recovery(String),
    EmptyHost,
    HostAlreadyStarted,
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
            Self::HostAlreadyStarted => {
                write!(formatter, "match host transport has already started")
            }
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

struct HostedServerState<S> {
    host: LiveMatchHost<S>,
    control: Arc<dyn MatchControlService>,
    control_handlers: Arc<Semaphore>,
}
impl<S> Clone for HostedServerState<S> {
    fn clone(&self) -> Self {
        Self {
            host: self.host.clone(),
            control: Arc::clone(&self.control),
            control_handlers: Arc::clone(&self.control_handlers),
        }
    }
}
impl<S: GameSimulation> HostedServerState<S> {
    fn connection_state(&self, id: &MatchId) -> Option<ConnectionState<S>> {
        let hosted = self.host.get(id)?;
        Some(ConnectionState {
            runtime: Arc::clone(&hosted.runtime),
            control: Arc::new(HostedControl {
                match_id: id.clone(),
                service: Arc::clone(&self.control),
            }),
            control_handlers: Arc::clone(&self.control_handlers),
            snapshots: hosted.snapshots.clone(),
            shutdown: hosted.shutdown.clone(),
            admission_gate: Some(self.host.admission_gate()),
        })
    }
}
struct ServingOwner<S: GameSimulation>(LiveMatchHost<S>);
impl<S: GameSimulation> Drop for ServingOwner<S> {
    fn drop(&mut self) {
        self.0.close();
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
    shutdown_requests: mpsc::Receiver<()>,
    ready: Option<oneshot::Sender<()>>,
    recovery: Option<MatchHostRecoveryPlan>,
) -> Result<(), MatchHostTransportError>
where
    S: GameSimulation,
    C: MatchControlService,
{
    serve_live_host_inner(
        LiveMatchHost::new(host),
        control,
        config,
        shutdown_requests,
        ready,
        recovery,
        false,
    )
    .await
}

/// Serves a live management capability; empty hosts can accept placement once readiness is established.
pub async fn serve_live_match_host_with_control_and_shutdown<
    S: GameSimulation,
    C: MatchControlService,
>(
    host: LiveMatchHost<S>,
    control: C,
    config: MatchHostWebTransportConfig,
    shutdown: mpsc::Receiver<()>,
) -> Result<(), MatchHostTransportError> {
    serve_live_host_inner(host, control, config, shutdown, None, None, true).await
}

pub(crate) async fn serve_live_host_inner<S: GameSimulation, C: MatchControlService>(
    host: LiveMatchHost<S>,
    control: C,
    config: MatchHostWebTransportConfig,
    mut shutdown_requests: mpsc::Receiver<()>,
    ready: Option<oneshot::Sender<()>>,
    recovery: Option<MatchHostRecoveryPlan>,
    allow_empty: bool,
) -> Result<(), MatchHostTransportError> {
    let entries = host.entries();
    if entries.is_empty() && !allow_empty {
        return Err(MatchHostTransportError::EmptyHost);
    }
    if host.is_draining().await {
        return Err(MatchHostTransportError::HostAlreadyDraining);
    }
    for (id, hosted) in &entries {
        let runtime = hosted.runtime.lock().await;
        if runtime.tick_hz() == 0 {
            return Err(MatchHostTransportError::InvalidTickRate(id.clone()));
        }
        if runtime.max_players() > usize::from(u16::MAX) {
            return Err(MatchHostTransportError::PlayerCapacityTooLarge {
                match_id: id.clone(),
                capacity: runtime.max_players(),
            });
        }
    }
    drop(entries);
    host.claim()
        .map_err(|_| MatchHostTransportError::HostAlreadyStarted)?;
    let _owner = ServingOwner(host.clone());
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

    let state = HostedServerState {
        host,
        control: Arc::new(control),
        control_handlers: Arc::new(Semaphore::new(MAX_CONCURRENT_CONTROL_HANDLERS)),
    };
    state.host.start().await;
    let route_prefix = config.route_prefix.clone();
    let allowed_origins = Arc::new(config.allowed_origins);
    if let Some(ready) = ready {
        let _ = ready.send(());
    }

    let mut connections = JoinSet::new();
    let incoming_session = |incoming: wtransport::endpoint::IncomingSession| {
        let state = state.clone();
        let route_prefix = route_prefix.clone();
        let allowed_origins = Arc::clone(&allowed_origins);
        async move {
            let request = match incoming.await {
                Ok(request) => request,
                Err(error) => {
                    eprintln!("WebTransport negotiation failed: {error}");
                    return;
                }
            };
            if let Some(allowed) = allowed_origins.as_ref()
                && !allowed.allows(request.origin())
            {
                request.forbidden().await;
                return;
            }
            let route = match route_prefix.parse(request.path()) {
                Ok(Some(route)) if state.host.get(&route.match_id).is_some() => route,
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

                state.host.begin_drain().await;
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

                state.host.stop().await;
                endpoint.close(wtransport::VarInt::from_u32(0), b"server shutting down");
                connections.shutdown().await;
                endpoint.wait_idle().await;
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
    for (id, hosted) in state.host.entries() {
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
    for (_, hosted) in state.host.entries() {
        hosted.runtime.lock().await.resume_after_failed_recovery();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DEFAULT_RECONNECT_GRACE_TICKS, DemoSimulation, MatchRuntime};

    fn config() -> MatchHostWebTransportConfig {
        MatchHostWebTransportConfig {
            port: 0,
            certificate_pem: PathBuf::from("unused-cert.pem"),
            private_key_pem: PathBuf::from("unused-key.pem"),
            route_prefix: BrowserRoutePrefix::new("/matches").unwrap(),
            allowed_origins: None,
            drain_grace: Duration::ZERO,
        }
    }

    #[tokio::test]
    async fn configured_origins_gate_new_and_reconnect_sessions_before_admission_or_epoch_fencing()
    {
        use crate::test_support::TestDirectory;
        use crate::{BrowserOriginAllowlist, ReconnectToken, WELCOME_BYTES, decode_welcome};
        use wtransport::{ClientConfig, endpoint::ConnectOptions, error::ConnectingError};

        tokio::time::timeout(Duration::from_secs(20), async {
            let directory = TestDirectory::new();
            let identity = Identity::self_signed(["localhost", "127.0.0.1"]).unwrap();
            let hash = identity.certificate_chain().as_slice()[0].hash();
            let certificate = directory.path().join("cert.pem");
            let key = directory.path().join("key.pem");
            identity
                .certificate_chain()
                .store_pemfile(&certificate)
                .await
                .unwrap();
            identity
                .private_key()
                .store_secret_pemfile(&key)
                .await
                .unwrap();
            let reservation = std::net::UdpSocket::bind(("127.0.0.1", 0)).unwrap();
            let port = reservation.local_addr().unwrap().port();
            drop(reservation);
            let prefix = BrowserRoutePrefix::new("/game").unwrap();
            let id = MatchId::new("alpha").unwrap();
            let mut matches = MatchHost::new(1).unwrap();
            matches
                .insert(id.clone(), MatchRuntime::new(DemoSimulation::new(), 1000))
                .unwrap();
            let host = LiveMatchHost::new(matches);
            let (shutdown, receiver) = mpsc::channel(1);
            let (ready, started) = oneshot::channel();
            let mut tasks = JoinSet::new();
            tasks.spawn(serve_live_host_inner(
                host.clone(),
                RejectMatchControlService,
                MatchHostWebTransportConfig {
                    port,
                    certificate_pem: certificate,
                    private_key_pem: key,
                    route_prefix: prefix.clone(),
                    drain_grace: Duration::ZERO,
                    allowed_origins: Some(
                        BrowserOriginAllowlist::new(["https://board.example"]).unwrap(),
                    ),
                },
                receiver,
                Some(ready),
                None,
                false,
            ));
            started.await.unwrap();
            let client = Endpoint::client(
                ClientConfig::builder()
                    .with_bind_default()
                    .with_server_certificate_hashes([hash])
                    .build(),
            )
            .unwrap();
            let url = format!("https://127.0.0.1:{port}{}", prefix.match_path(&id));
            assert!(matches!(
                client.connect(url.clone()).await,
                Err(ConnectingError::SessionRejected)
            ));
            assert!(matches!(
                client
                    .connect(
                        ConnectOptions::builder(url.clone())
                            .add_header("origin", "https://other.example")
                            .build()
                    )
                    .await,
                Err(ConnectingError::SessionRejected)
            ));
            assert_eq!(host.statuses().await[0].occupied_player_slots, 0);
            let admitted = client
                .connect(
                    ConnectOptions::builder(url)
                        .add_header("origin", "https://board.example")
                        .build(),
                )
                .await
                .unwrap();
            let mut stream = admitted.accept_uni().await.unwrap();
            let mut bytes = [0; WELCOME_BYTES];
            stream.read_exact(&mut bytes).await.unwrap();
            let original = decode_welcome(&bytes).unwrap();
            assert_eq!(original.player_id, 1);
            let reconnect = format!(
                "https://127.0.0.1:{port}{}",
                prefix.reconnect_path(&id, ReconnectToken(original.reconnect_token))
            );
            assert!(matches!(
                client
                    .connect(
                        ConnectOptions::builder(reconnect.clone())
                            .add_header("origin", "https://other.example")
                            .build()
                    )
                    .await,
                Err(ConnectingError::SessionRejected)
            ));
            assert!(matches!(
                client.connect(reconnect.clone()).await,
                Err(ConnectingError::SessionRejected)
            ));
            host.inspect(&id, |runtime| {
                runtime.validate_connection(original.player_id, original.connection_epoch)
            })
            .await
            .unwrap()
            .unwrap();
            assert_eq!(host.statuses().await[0].occupied_player_slots, 1);
            admitted.close(0_u32.into(), b"");
            admitted.closed().await;
            while host.statuses().await[0].active_players != 0 {
                tokio::task::yield_now().await;
            }
            let resumed = client
                .connect(
                    ConnectOptions::builder(reconnect)
                        .add_header("origin", "https://board.example")
                        .build(),
                )
                .await
                .unwrap();
            let mut stream = resumed.accept_uni().await.unwrap();
            stream.read_exact(&mut bytes).await.unwrap();
            let current = decode_welcome(&bytes).unwrap();
            assert_eq!(current.player_id, original.player_id);
            assert!(current.connection_epoch > original.connection_epoch);
            assert_eq!(host.statuses().await[0].occupied_player_slots, 1);
            shutdown.send(()).await.unwrap();
            tasks.join_next().await.unwrap().unwrap().unwrap();
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn static_empty_or_pre_draining_hosts_fail_closed_before_binding() {
        let empty = MatchHost::<DemoSimulation>::new(1).unwrap();
        let (_, shutdown) = mpsc::channel(1);
        assert_eq!(
            serve_match_host_with_shutdown(empty, config(), shutdown).await,
            Err(MatchHostTransportError::EmptyHost)
        );
        let mut draining = MatchHost::new(1).unwrap();
        draining
            .insert(
                MatchId::new("alpha").unwrap(),
                MatchRuntime::new(DemoSimulation::new(), DEFAULT_RECONNECT_GRACE_TICKS),
            )
            .unwrap();
        draining.begin_drain();
        let (_, shutdown) = mpsc::channel(1);
        assert_eq!(
            serve_match_host_with_shutdown(draining, config(), shutdown).await,
            Err(MatchHostTransportError::HostAlreadyDraining)
        );
    }

    #[tokio::test]
    async fn dropping_host_tick_owner_closes_every_snapshot_source() {
        let host = LiveMatchHost::new(MatchHost::new(2).unwrap());
        host.claim().unwrap();
        host.start().await;
        let owner = ServingOwner(host.clone());
        let mut publications = Vec::new();
        for name in ["alpha", "beta"] {
            let id = MatchId::new(name).unwrap();
            host.place(
                id.clone(),
                MatchRuntime::new(DemoSimulation::new(), DEFAULT_RECONNECT_GRACE_TICKS),
            )
            .await
            .unwrap();
            publications.push(host.get(&id).unwrap().snapshots.subscribe());
        }
        for published in &mut publications {
            published.recv().await.unwrap();
        }
        drop(owner);
        tokio::time::timeout(Duration::from_secs(1), async {
            for published in &mut publications {
                while published.recv().await.is_ok() {}
            }
        })
        .await
        .expect("cancelled host must release every tick task and runtime");
        assert!(host.statuses().await.is_empty());
        assert_eq!(
            host.place(
                MatchId::new("gamma").unwrap(),
                MatchRuntime::new(DemoSimulation::new(), DEFAULT_RECONNECT_GRACE_TICKS)
            )
            .await
            .unwrap_err()
            .error(),
            &crate::LiveHostError::NotServing
        );
    }
}

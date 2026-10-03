use crate::LiveMatchHost;
use crate::MatchControlService;
use crate::host::{MatchHost, MatchId};
use crate::host_recovery::{MatchHostRecoveryPlan, PreparedLiveMatchHost, PreparedMatchHost};
use crate::host_transport::{
    MatchHostTransportError, MatchHostWebTransportConfig, serve_live_host_inner,
};
use crate::simulation::GameSimulation;
use std::error::Error;
use std::fmt;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::task::{AbortHandle, JoinHandle, JoinSet};

struct AuxiliaryTasks(Vec<AbortHandle>);
impl Drop for AuxiliaryTasks {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}

pub const HOST_STATUS_CONTRACT_VERSION: u16 = 1;
pub const DEFAULT_HOST_STATUS_PORT: u16 = 8080;

const MAX_REQUEST_HEADER_BYTES: usize = 4 * 1024;
const MAX_CONCURRENT_STATUS_CONNECTIONS: usize = 64;
const REQUEST_HEADER_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MatchHostStatusConfig {
    pub port: u16,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HostStatusServerError {
    Bind(String),
    Serve(String),
    Task(String),
    ExitedUnexpectedly,
    Transport(MatchHostTransportError),
}

impl fmt::Display for HostStatusServerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bind(error) => write!(formatter, "host status listener bind failed: {error}"),
            Self::Serve(error) => write!(formatter, "host status listener failed: {error}"),
            Self::Task(error) => write!(formatter, "host status task failed: {error}"),
            Self::ExitedUnexpectedly => {
                write!(
                    formatter,
                    "host status listener exited before transport stopped"
                )
            }
            Self::Transport(error) => error.fmt(formatter),
        }
    }
}

impl Error for HostStatusServerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error),
            _ => None,
        }
    }
}

impl From<MatchHostTransportError> for HostStatusServerError {
    fn from(error: MatchHostTransportError) -> Self {
        Self::Transport(error)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MatchFacts {
    id: MatchId,
    draining: bool,
    frozen: bool,
    max_players: usize,
}

#[derive(Debug)]
struct ProcessFacts {
    hosted_matches: usize,
    max_matches: usize,
    player_capacity: usize,
    matches: Vec<MatchFacts>,
}

#[derive(Debug)]
struct StatusState {
    process: ProcessFacts,
    serving: AtomicBool,
    draining: AtomicBool,
    accepts_placement: bool,
}

impl StatusState {
    fn starting(max_matches: usize, accepts_placement: bool) -> Self {
        Self {
            process: ProcessFacts {
                hosted_matches: 0,
                max_matches,
                player_capacity: 0,
                matches: Vec::new(),
            },
            serving: AtomicBool::new(false),
            draining: AtomicBool::new(false),
            accepts_placement,
        }
    }

    async fn snapshot<S: GameSimulation>(&self, host: &LiveMatchHost<S>) -> Self {
        let matches: Vec<_> = host
            .statuses()
            .await
            .into_iter()
            .map(|status| MatchFacts {
                id: status.id,
                draining: status.draining,
                frozen: status.frozen,
                max_players: status.max_players,
            })
            .collect();
        Self {
            process: ProcessFacts {
                hosted_matches: matches.len(),
                max_matches: host.max_matches(),
                player_capacity: matches.iter().map(|facts| facts.max_players).sum(),
                matches,
            },
            serving: AtomicBool::new(self.serving() && host.is_serving()),
            draining: AtomicBool::new(self.process_draining() || host.is_draining().await),
            accepts_placement: self.accepts_placement,
        }
    }

    fn mark_serving(&self) {
        self.serving.store(true, Ordering::Release);
    }

    fn serving(&self) -> bool {
        self.serving.load(Ordering::Acquire)
    }

    fn begin_process_drain(&self) {
        self.draining.store(true, Ordering::Release);
    }

    fn process_draining(&self) -> bool {
        self.draining.load(Ordering::Acquire)
    }

    fn match_facts(&self, id: &str) -> Option<&MatchFacts> {
        self.process
            .matches
            .iter()
            .find(|facts| facts.id.as_str() == id)
    }

    fn match_draining(&self, facts: &MatchFacts) -> bool {
        self.process_draining() || facts.draining
    }

    fn match_ready(&self, facts: &MatchFacts) -> bool {
        self.serving() && !self.match_draining(facts) && !facts.frozen
    }

    fn process_ready(&self) -> bool {
        self.serving()
            && !self.process_draining()
            && ((self.accepts_placement && self.process.hosted_matches == 0)
                || self
                    .process
                    .matches
                    .iter()
                    .any(|facts| self.match_ready(facts)))
    }

    fn process_status_json(&self) -> String {
        let matches = self
            .process
            .matches
            .iter()
            .map(|facts| self.match_status_json(facts))
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "{{\"version\":{HOST_STATUS_CONTRACT_VERSION},\"healthy\":true,\"ready\":{},\"draining\":{},\"capacity\":{{\"hostedMatches\":{},\"maxMatches\":{},\"remainingMatches\":{},\"playerCapacity\":{}}},\"matches\":[{matches}]}}",
            self.process_ready(),
            self.process_draining(),
            self.process.hosted_matches,
            self.process.max_matches,
            self.process
                .max_matches
                .saturating_sub(self.process.hosted_matches),
            self.process.player_capacity,
        )
    }

    fn match_status_json(&self, facts: &MatchFacts) -> String {
        format!(
            "{{\"id\":\"{}\",\"healthy\":true,\"ready\":{},\"draining\":{},\"frozen\":{},\"maxPlayers\":{}}}",
            facts.id,
            self.match_ready(facts),
            self.match_draining(facts),
            facts.frozen,
            facts.max_players,
        )
    }
}

pub async fn serve_match_host_with_status_and_control_and_shutdown<S, C>(
    host: MatchHost<S>,
    control: C,
    transport_config: MatchHostWebTransportConfig,
    status_config: MatchHostStatusConfig,
    shutdown_requests: mpsc::Receiver<()>,
) -> Result<(), HostStatusServerError>
where
    S: GameSimulation,
    C: MatchControlService,
{
    serve_match_host_with_status_control_shutdown_inner(
        LiveMatchHost::new(host),
        control,
        transport_config,
        status_config,
        shutdown_requests,
        None,
        false,
    )
    .await
}

pub async fn serve_prepared_match_host_with_status_and_control_and_shutdown<S, C>(
    prepared: PreparedMatchHost<S>,
    control: C,
    transport_config: MatchHostWebTransportConfig,
    status_config: MatchHostStatusConfig,
    shutdown_requests: mpsc::Receiver<()>,
) -> Result<(), HostStatusServerError>
where
    S: GameSimulation,
    C: MatchControlService,
{
    let PreparedMatchHost { host, recovery } = prepared;
    serve_match_host_with_status_control_shutdown_inner(
        LiveMatchHost::new(host),
        control,
        transport_config,
        status_config,
        shutdown_requests,
        Some(recovery),
        false,
    )
    .await
}

/// Serve a host whose trusted application can place and retire matches while running.
/// An empty live host is ready once transport is listening; capacity remains a separate fact.
pub async fn serve_live_match_host_with_status_and_control_and_shutdown<S, C>(
    host: LiveMatchHost<S>,
    control: C,
    transport_config: MatchHostWebTransportConfig,
    status_config: MatchHostStatusConfig,
    shutdown_requests: mpsc::Receiver<()>,
) -> Result<(), HostStatusServerError>
where
    S: GameSimulation,
    C: MatchControlService,
{
    serve_match_host_with_status_control_shutdown_inner(
        host,
        control,
        transport_config,
        status_config,
        shutdown_requests,
        None,
        true,
    )
    .await
}

pub async fn serve_prepared_live_match_host_with_status_and_control_and_shutdown<S, C>(
    prepared: PreparedLiveMatchHost<S>,
    control: C,
    transport_config: MatchHostWebTransportConfig,
    status_config: MatchHostStatusConfig,
    shutdown_requests: mpsc::Receiver<()>,
) -> Result<(), HostStatusServerError>
where
    S: GameSimulation,
    C: MatchControlService,
{
    serve_match_host_with_status_control_shutdown_inner(
        prepared.host,
        control,
        transport_config,
        status_config,
        shutdown_requests,
        Some(prepared.recovery),
        true,
    )
    .await
}

async fn serve_match_host_with_status_control_shutdown_inner<S, C>(
    host: LiveMatchHost<S>,
    control: C,
    transport_config: MatchHostWebTransportConfig,
    status_config: MatchHostStatusConfig,
    mut shutdown_requests: mpsc::Receiver<()>,
    recovery: Option<MatchHostRecoveryPlan>,
    allow_empty: bool,
) -> Result<(), HostStatusServerError>
where
    S: GameSimulation,
    C: MatchControlService,
{
    let listener = TcpListener::bind(("0.0.0.0", status_config.port))
        .await
        .map_err(|error| HostStatusServerError::Bind(error.to_string()))?;
    let state = Arc::new(StatusState::starting(host.max_matches(), allow_empty));
    let (transport_shutdown_sender, transport_shutdown_receiver) = mpsc::channel(4);
    let (status_stop_sender, status_stop_receiver) = mpsc::channel(1);
    let (transport_ready_sender, transport_ready_receiver) = oneshot::channel();

    let forward_state = Arc::clone(&state);
    let forward_transport_shutdown = transport_shutdown_sender.clone();
    let shutdown_forwarder = tokio::spawn(async move {
        while let Some(()) = shutdown_requests.recv().await {
            forward_state.begin_process_drain();
            if forward_transport_shutdown.send(()).await.is_err() {
                break;
            }
        }
    });

    let ready_state = Arc::clone(&state);
    let mut readiness_task = tokio::spawn(async move {
        if transport_ready_receiver.await.is_ok() {
            ready_state.mark_serving();
        }
    });
    let mut status_task = tokio::spawn(serve_status_listener(
        listener,
        Arc::clone(&state),
        host.clone(),
        status_stop_receiver,
    ));
    let _auxiliary = AuxiliaryTasks(vec![
        shutdown_forwarder.abort_handle(),
        readiness_task.abort_handle(),
        status_task.abort_handle(),
    ]);
    let transport = serve_live_host_inner(
        host,
        control,
        transport_config,
        transport_shutdown_receiver,
        Some(transport_ready_sender),
        recovery,
        allow_empty,
    );
    tokio::pin!(transport);

    tokio::select! {
        transport_result = &mut transport => {
            let _ = status_stop_sender.send(()).await;
            let status_result = join_status_task(&mut status_task).await;
            let _ = (&mut readiness_task).await;
            shutdown_forwarder.abort();
            let _ = shutdown_forwarder.await;
            transport_result?;
            status_result
        }
        status_result = &mut status_task => {
            let status_result = status_result
                .map_err(|error| HostStatusServerError::Task(error.to_string()))?;
            state.begin_process_drain();
            let _ = transport_shutdown_sender.send(()).await;
            let transport_result = transport.await;
            let _ = (&mut readiness_task).await;
            shutdown_forwarder.abort();
            let _ = shutdown_forwarder.await;
            transport_result?;
            match status_result {
                Ok(()) => Err(HostStatusServerError::ExitedUnexpectedly),
                Err(error) => Err(error),
            }
        }
    }
}

async fn join_status_task(
    task: &mut JoinHandle<Result<(), HostStatusServerError>>,
) -> Result<(), HostStatusServerError> {
    task.await
        .map_err(|error| HostStatusServerError::Task(error.to_string()))?
}

async fn serve_status_listener<S: GameSimulation>(
    listener: TcpListener,
    state: Arc<StatusState>,
    host: LiveMatchHost<S>,
    mut stop: mpsc::Receiver<()>,
) -> Result<(), HostStatusServerError> {
    let connections = Arc::new(Semaphore::new(MAX_CONCURRENT_STATUS_CONNECTIONS));
    let mut requests = JoinSet::new();
    loop {
        tokio::select! {
            _ = stop.recv() => return Ok(()),
            _ = requests.join_next(), if !requests.is_empty() => {},
            accepted = listener.accept() => {
                let (stream, _) = accepted
                    .map_err(|error| HostStatusServerError::Serve(error.to_string()))?;
                let Ok(permit) = Arc::clone(&connections).try_acquire_owned() else {
                    continue;
                };
                let state = Arc::clone(&state);
                let host = host.clone();
                requests.spawn(async move {
                    let _permit = permit;
                    if let Err(error) = handle_status_connection(stream, &state, &host).await {
                        eprintln!("host status request failed: {error}");
                    }
                });
            }
        }
    }
}

async fn handle_status_connection<S: GameSimulation>(
    mut stream: TcpStream,
    state: &StatusState,
    host: &LiveMatchHost<S>,
) -> Result<(), io::Error> {
    let request = match tokio::time::timeout(
        REQUEST_HEADER_TIMEOUT,
        read_request_header(&mut stream),
    )
    .await
    {
        Ok(Ok(request)) => request,
        Ok(Err(RequestReadError::Io(error))) => return Err(error),
        Ok(Err(RequestReadError::TooLarge)) => {
            return write_response(
                &mut stream,
                431,
                "Request Header Fields Too Large",
                "{\"error\":\"request-header-too-large\"}",
                &[],
            )
            .await;
        }
        Ok(Err(RequestReadError::Incomplete)) => {
            return write_response(
                &mut stream,
                400,
                "Bad Request",
                "{\"error\":\"incomplete-request\"}",
                &[],
            )
            .await;
        }
        Err(_) => {
            return write_response(
                &mut stream,
                408,
                "Request Timeout",
                "{\"error\":\"request-timeout\"}",
                &[],
            )
            .await;
        }
    };

    let snapshot = state.snapshot(host).await;
    let response = route_request(&request, &snapshot);
    write_response(
        &mut stream,
        response.status,
        response.reason,
        &response.body,
        response.extra_headers,
    )
    .await
}

#[derive(Debug)]
enum RequestReadError {
    Io(io::Error),
    TooLarge,
    Incomplete,
}

async fn read_request_header(stream: &mut TcpStream) -> Result<String, RequestReadError> {
    let mut buffer = [0_u8; MAX_REQUEST_HEADER_BYTES];
    let mut used = 0_usize;
    loop {
        if used == buffer.len() {
            return Err(RequestReadError::TooLarge);
        }
        let read = stream
            .read(&mut buffer[used..])
            .await
            .map_err(RequestReadError::Io)?;
        if read == 0 {
            return Err(RequestReadError::Incomplete);
        }
        used += read;
        if buffer[..used]
            .windows(4)
            .any(|window| window == b"\r\n\r\n")
        {
            return String::from_utf8(buffer[..used].to_vec())
                .map_err(|_| RequestReadError::Incomplete);
        }
    }
}

struct HttpResponse {
    status: u16,
    reason: &'static str,
    body: String,
    extra_headers: &'static [(&'static str, &'static str)],
}

fn route_request(request: &str, state: &StatusState) -> HttpResponse {
    let Some(line) = request.lines().next() else {
        return bad_request();
    };
    let mut parts = line.split_whitespace();
    let Some(method) = parts.next() else {
        return bad_request();
    };
    let Some(target) = parts.next() else {
        return bad_request();
    };
    let Some(version) = parts.next() else {
        return bad_request();
    };
    if parts.next().is_some() || !matches!(version, "HTTP/1.0" | "HTTP/1.1") {
        return bad_request();
    }
    if method != "GET" {
        return HttpResponse {
            status: 405,
            reason: "Method Not Allowed",
            body: "{\"error\":\"method-not-allowed\"}".to_owned(),
            extra_headers: &[("Allow", "GET")],
        };
    }

    let path = target.split_once('?').map_or(target, |(path, _)| path);
    match path {
        "/healthz" => ok("{\"healthy\":true}".to_owned()),
        "/readyz" => readiness_response(state.process_ready(), state.process_draining()),
        "/status" => ok(state.process_status_json()),
        _ => route_match_request(path, state).unwrap_or_else(not_found),
    }
}

fn route_match_request(path: &str, state: &StatusState) -> Option<HttpResponse> {
    let rest = path.strip_prefix("/matches/")?;
    let (id, endpoint) = rest.split_once('/')?;
    if id.is_empty() || endpoint.contains('/') {
        return None;
    }
    let facts = state.match_facts(id)?;
    match endpoint {
        "healthz" => Some(ok(format!("{{\"id\":\"{}\",\"healthy\":true}}", facts.id))),
        "readyz" => Some(match_readiness_response(state, facts)),
        "status" => Some(ok(state.match_status_json(facts))),
        _ => None,
    }
}

fn match_readiness_response(state: &StatusState, facts: &MatchFacts) -> HttpResponse {
    let ready = state.match_ready(facts);
    let body = format!(
        "{{\"id\":\"{}\",\"ready\":{ready},\"draining\":{},\"frozen\":{}}}",
        facts.id,
        state.match_draining(facts),
        facts.frozen,
    );
    response_for_readiness(ready, body)
}

fn readiness_response(ready: bool, draining: bool) -> HttpResponse {
    response_for_readiness(
        ready,
        format!("{{\"ready\":{ready},\"draining\":{draining}}}"),
    )
}

fn response_for_readiness(ready: bool, body: String) -> HttpResponse {
    if ready {
        ok(body)
    } else {
        HttpResponse {
            status: 503,
            reason: "Service Unavailable",
            body,
            extra_headers: &[],
        }
    }
}

fn ok(body: String) -> HttpResponse {
    HttpResponse {
        status: 200,
        reason: "OK",
        body,
        extra_headers: &[],
    }
}

fn bad_request() -> HttpResponse {
    HttpResponse {
        status: 400,
        reason: "Bad Request",
        body: "{\"error\":\"bad-request\"}".to_owned(),
        extra_headers: &[],
    }
}

fn not_found() -> HttpResponse {
    HttpResponse {
        status: 404,
        reason: "Not Found",
        body: "{\"error\":\"not-found\"}".to_owned(),
        extra_headers: &[],
    }
}

async fn write_response(
    stream: &mut TcpStream,
    status: u16,
    reason: &str,
    body: &str,
    extra_headers: &[(&str, &str)],
) -> Result<(), io::Error> {
    let mut response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n",
        body.len()
    );
    for (name, value) in extra_headers {
        response.push_str(name);
        response.push_str(": ");
        response.push_str(value);
        response.push_str("\r\n");
    }
    response.push_str("\r\n");
    response.push_str(body);
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test_support::TestDirectory;
    use crate::{
        BrowserRoutePrefix, DemoSimulation, MatchHostRecoveryConfig, MatchRuntime,
        RejectMatchControlService, WELCOME_BYTES, decode_welcome,
        prepare_live_match_host_for_recovery,
    };
    use wtransport::{ClientConfig, Endpoint, Identity};

    async fn http(port: u16, path: &str) -> String {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        stream
            .write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes())
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        response
    }
    async fn welcome(connection: &wtransport::Connection) -> crate::Welcome {
        let mut stream = connection.accept_uni().await.unwrap();
        let mut bytes = [0; WELCOME_BYTES];
        stream.read_exact(&mut bytes).await.unwrap();
        decode_welcome(&bytes).unwrap()
    }
    fn factory(_: &MatchId) -> Result<DemoSimulation, std::convert::Infallible> {
        Ok(DemoSimulation::new())
    }

    #[tokio::test]
    async fn live_transport_status_retirement_and_manifest_recovery_work_without_process_reconfiguration()
     {
        tokio::time::timeout(Duration::from_secs(20), async {
            let directory = TestDirectory::new();
            let identity = Identity::self_signed(["localhost", "127.0.0.1"]).unwrap();
            let hash = identity.certificate_chain().as_slice()[0].hash();
            let cert = directory.path().join("cert.pem");
            let key = directory.path().join("key.pem");
            identity
                .certificate_chain()
                .store_pemfile(&cert)
                .await
                .unwrap();
            identity
                .private_key()
                .store_secret_pemfile(&key)
                .await
                .unwrap();
            let udp = std::net::UdpSocket::bind(("127.0.0.1", 0)).unwrap();
            let port = udp.local_addr().unwrap().port();
            drop(udp);
            let tcp = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
            let status_port = tcp.local_addr().unwrap().port();
            drop(tcp);
            let prefix = BrowserRoutePrefix::new("/game").unwrap();
            let config = MatchHostWebTransportConfig {
                port,
                certificate_pem: cert,
                private_key_pem: key,
                route_prefix: prefix.clone(),
                drain_grace: Duration::ZERO,
            };
            let recovery = MatchHostRecoveryConfig {
                directory: directory.path().join("recovery"),
            };
            let prepared = prepare_live_match_host_for_recovery(
                Vec::new(),
                factory,
                2,
                1000,
                recovery.clone(),
            )
            .await
            .unwrap();
            let host = prepared.host();
            let (shutdown, receiver) = mpsc::channel(1);
            let mut tasks = JoinSet::new();
            let first_config = config.clone();
            tasks.spawn(async move {
                serve_prepared_live_match_host_with_status_and_control_and_shutdown(
                    prepared,
                    RejectMatchControlService,
                    first_config,
                    MatchHostStatusConfig { port: status_port },
                    receiver,
                )
                .await
            });
            while !host.is_serving() {
                tokio::task::yield_now().await;
            }
            while !http(status_port, "/readyz")
                .await
                .starts_with("HTTP/1.1 200")
            {
                tokio::task::yield_now().await;
            }
            let status = http(status_port, "/status").await;
            assert!(status.contains("\"hostedMatches\":0"));
            assert!(status.contains("\"remainingMatches\":2"));
            let client = Endpoint::client(
                ClientConfig::builder()
                    .with_bind_default()
                    .with_server_certificate_hashes([hash])
                    .build(),
            )
            .unwrap();
            let alpha = MatchId::new("created-alpha").unwrap();
            let beta = MatchId::new("created-beta").unwrap();
            let url = |path: String| format!("https://127.0.0.1:{port}{path}");
            assert!(
                client
                    .connect(url(prefix.match_path(&alpha)))
                    .await
                    .is_err()
            );
            host.place(
                alpha.clone(),
                MatchRuntime::new_with_replay_capture(DemoSimulation::new(), 1000),
            )
            .await
            .unwrap();
            host.place(
                beta.clone(),
                MatchRuntime::new_with_replay_capture(DemoSimulation::new(), 1000),
            )
            .await
            .unwrap();
            let status = http(status_port, "/status").await;
            assert!(status.contains("\"hostedMatches\":2"));
            assert!(status.contains("\"remainingMatches\":0"));
            assert!(
                http(status_port, "/readyz")
                    .await
                    .starts_with("HTTP/1.1 200")
            );
            let alpha_connection = client
                .connect(url(prefix.match_path(&alpha)))
                .await
                .unwrap();
            let alpha_welcome = welcome(&alpha_connection).await;
            let beta_connection = client.connect(url(prefix.match_path(&beta))).await.unwrap();
            let beta_welcome = welcome(&beta_connection).await;
            alpha_connection.receive_datagram().await.unwrap();
            beta_connection.receive_datagram().await.unwrap();
            host.retire(&beta).await.unwrap();
            beta_connection.closed().await;
            assert!(
                http(status_port, "/matches/created-beta/status")
                    .await
                    .starts_with("HTTP/1.1 404")
            );
            assert!(client.connect(url(prefix.match_path(&beta))).await.is_err());
            assert!(
                client
                    .connect(url(prefix.reconnect_path(
                        &beta,
                        crate::ReconnectToken(beta_welcome.reconnect_token)
                    )))
                    .await
                    .is_err()
            );
            // The unrelated live match keeps publishing after retirement.
            alpha_connection.receive_datagram().await.unwrap();
            host.place(
                beta.clone(),
                MatchRuntime::new_with_replay_capture(DemoSimulation::new(), 1000),
            )
            .await
            .unwrap();
            if let Ok(stale) = client
                .connect(url(prefix.reconnect_path(
                    &beta,
                    crate::ReconnectToken(beta_welcome.reconnect_token),
                )))
                .await
            {
                assert!(
                    stale.accept_uni().await.is_err(),
                    "reused ID must reject the previous runtime's token"
                );
            }
            let replacement = client.connect(url(prefix.match_path(&beta))).await.unwrap();
            welcome(&replacement).await;
            host.retire(&beta).await.unwrap();
            replacement.closed().await;
            shutdown.send(()).await.unwrap();
            tasks.join_next().await.unwrap().unwrap().unwrap();
            alpha_connection.closed().await;
            assert!(!host.is_serving());
            assert!(recovery.directory.join("manifest").exists());
            let prepared = prepare_live_match_host_for_recovery(
                Vec::new(),
                factory,
                2,
                1000,
                recovery.clone(),
            )
            .await
            .unwrap();
            let recovered = prepared.host();
            assert_eq!(recovered.statuses().await.len(), 1);
            assert_eq!(recovered.statuses().await[0].id, alpha);
            let (shutdown, receiver) = mpsc::channel(1);
            tasks.spawn(async move {
                serve_prepared_live_match_host_with_status_and_control_and_shutdown(
                    prepared,
                    RejectMatchControlService,
                    config,
                    MatchHostStatusConfig { port: status_port },
                    receiver,
                )
                .await
            });
            while !recovered.is_serving() {
                tokio::select! {
                    result = tasks.join_next() => panic!("restarted server exited before readiness: {result:?}"),
                    _ = tokio::task::yield_now() => {},
                }
            }
            assert!(
                !recovery.directory.exists(),
                "bound startup must consume recovery before serving"
            );
            let restored = client
                .connect(url(prefix.reconnect_path(
                    &alpha,
                    crate::ReconnectToken(alpha_welcome.reconnect_token),
                )))
                .await
                .unwrap();
            let restored_welcome = welcome(&restored).await;
            assert_eq!(restored_welcome.player_id, alpha_welcome.player_id);
            assert!(restored_welcome.connection_epoch > alpha_welcome.connection_epoch);
            assert!(client.connect(url(prefix.match_path(&beta))).await.is_err());
            shutdown.send(()).await.unwrap();
            tasks.join_next().await.unwrap().unwrap().unwrap();
        })
        .await
        .expect("live placement, retirement and recovery must finish on loopback");
    }

    fn state() -> StatusState {
        StatusState {
            process: ProcessFacts {
                hosted_matches: 2,
                max_matches: 2,
                player_capacity: 8,
                matches: vec![
                    MatchFacts {
                        id: MatchId::new("alpha").unwrap(),
                        draining: false,
                        frozen: false,
                        max_players: 4,
                    },
                    MatchFacts {
                        id: MatchId::new("beta").unwrap(),
                        draining: false,
                        frozen: true,
                        max_players: 4,
                    },
                ],
            },
            serving: AtomicBool::new(true),
            draining: AtomicBool::new(false),
            accepts_placement: false,
        }
    }

    #[test]
    fn startup_is_unready_until_transport_signals_serving() {
        let state = state();
        state.serving.store(false, Ordering::Release);

        let starting = route_request("GET /readyz HTTP/1.1\r\n\r\n", &state);
        assert_eq!(starting.status, 503);
        assert!(!state.process_ready());

        state.mark_serving();
        let ready = route_request("GET /readyz HTTP/1.1\r\n\r\n", &state);
        assert_eq!(ready.status, 200);
    }

    #[test]
    fn status_separates_service_readiness_from_match_placement_capacity() {
        let state = state();
        assert!(state.process_ready());
        let response = route_request("GET /readyz HTTP/1.1\r\n\r\n", &state);
        assert_eq!(response.status, 200);
        assert_eq!(state.process.max_matches, state.process.hosted_matches);
    }

    #[test]
    fn process_drain_makes_process_and_matches_unready_without_failing_health() {
        let state = state();
        state.begin_process_drain();

        let process_ready = route_request("GET /readyz HTTP/1.1\r\n\r\n", &state);
        assert_eq!(process_ready.status, 503);
        assert!(process_ready.body.contains("\"draining\":true"));

        let match_ready = route_request("GET /matches/alpha/readyz HTTP/1.1\r\n\r\n", &state);
        assert_eq!(match_ready.status, 503);
        assert!(match_ready.body.contains("\"draining\":true"));

        let health = route_request("GET /healthz HTTP/1.1\r\n\r\n", &state);
        assert_eq!(health.status, 200);
    }

    #[test]
    fn frozen_match_is_unready_while_other_match_keeps_process_ready() {
        let state = state();
        let frozen = route_request("GET /matches/beta/readyz HTTP/1.1\r\n\r\n", &state);
        assert_eq!(frozen.status, 503);
        assert!(frozen.body.contains("\"frozen\":true"));

        let process = route_request("GET /readyz HTTP/1.1\r\n\r\n", &state);
        assert_eq!(process.status, 200);
    }

    #[test]
    fn status_routes_fail_closed_for_unknown_matches_and_mutating_methods() {
        let state = state();
        let missing = route_request("GET /matches/missing/status HTTP/1.1\r\n\r\n", &state);
        assert_eq!(missing.status, 404);

        let post = route_request("POST /status HTTP/1.1\r\n\r\n", &state);
        assert_eq!(post.status, 405);
        assert_eq!(post.extra_headers, &[("Allow", "GET")]);
    }
}

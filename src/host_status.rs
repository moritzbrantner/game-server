use crate::MatchControlService;
use crate::host::{MatchHost, MatchId};
use crate::host_transport::{
    MatchHostTransportError, MatchHostWebTransportConfig,
    serve_match_host_with_control_and_shutdown_notifying_ready,
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
use tokio::task::JoinHandle;

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
}

impl StatusState {
    fn from_host<S: GameSimulation>(host: &MatchHost<S>) -> Self {
        let process = host.status();
        let matches = host
            .statuses()
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
                hosted_matches: process.hosted_matches,
                max_matches: process.max_matches,
                player_capacity: process.player_capacity,
                matches,
            },
            serving: AtomicBool::new(false),
            draining: AtomicBool::new(process.draining),
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
        !self.process_draining()
            && self
                .process
                .matches
                .iter()
                .any(|facts| self.match_ready(facts))
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
    mut shutdown_requests: mpsc::Receiver<()>,
) -> Result<(), HostStatusServerError>
where
    S: GameSimulation,
    C: MatchControlService,
{
    let listener = TcpListener::bind(("0.0.0.0", status_config.port))
        .await
        .map_err(|error| HostStatusServerError::Bind(error.to_string()))?;
    let state = Arc::new(StatusState::from_host(&host));
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
        status_stop_receiver,
    ));
    let transport = serve_match_host_with_control_and_shutdown_notifying_ready(
        host,
        control,
        transport_config,
        transport_shutdown_receiver,
        Some(transport_ready_sender),
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

async fn serve_status_listener(
    listener: TcpListener,
    state: Arc<StatusState>,
    mut stop: mpsc::Receiver<()>,
) -> Result<(), HostStatusServerError> {
    let connections = Arc::new(Semaphore::new(MAX_CONCURRENT_STATUS_CONNECTIONS));
    loop {
        tokio::select! {
            _ = stop.recv() => return Ok(()),
            accepted = listener.accept() => {
                let (stream, _) = accepted
                    .map_err(|error| HostStatusServerError::Serve(error.to_string()))?;
                let Ok(permit) = Arc::clone(&connections).try_acquire_owned() else {
                    continue;
                };
                let state = Arc::clone(&state);
                tokio::spawn(async move {
                    let _permit = permit;
                    if let Err(error) = handle_status_connection(stream, &state).await {
                        eprintln!("host status request failed: {error}");
                    }
                });
            }
        }
    }
}

async fn handle_status_connection(
    mut stream: TcpStream,
    state: &StatusState,
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

    let response = route_request(&request, state);
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

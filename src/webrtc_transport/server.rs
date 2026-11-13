use super::peer::{self, Channels, NEGOTIATE_TIMEOUT, Peer};
use super::{Lobby, Result, WebRtcError};
use crate::connection::{self, AdmissionRequest, ConnectionState, SnapshotPublication};
use crate::{
    ControlContext, ControlService, ControlServiceError, GameSimulation, LiveMatchHost, MatchId,
    MatchRuntime, ReconnectToken, SessionLease, Welcome,
};
use ring::rand::{SecureRandom, SystemRandom};
use serde_json::json;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore, broadcast};
use tokio::task::JoinSet;
use tokio::time::timeout;

/// Native bind address and diagnostic ICE policy. Normal hosts use direct-first ICE.
#[derive(Clone, Debug)]
pub struct WebRtcHostConfig {
    pub bind_ip: String,
    pub relay_only: bool,
}

/// Game-owned control receives the authenticated signaling participant, never a capability.
/// The ordinary dispatch path still validates the connection epoch before invocation.
pub trait WebRtcControlService: Send + Sync + 'static {
    fn handle(
        &self,
        participant_id: &str,
        match_id: &MatchId,
        context: ControlContext,
        payload: &[u8],
    ) -> std::result::Result<Vec<u8>, ControlServiceError>;
}

struct ParticipantControl {
    service: Arc<dyn WebRtcControlService>,
    participant_id: String,
    match_id: MatchId,
}
impl ControlService for ParticipantControl {
    fn handle(
        &self,
        context: ControlContext,
        payload: &[u8],
    ) -> std::result::Result<Vec<u8>, ControlServiceError> {
        self.service
            .handle(&self.participant_id, &self.match_id, context, payload)
    }
}

struct AuthorizedControl {
    inner: Arc<dyn ControlService>,
    authorize: Arc<dyn Fn() -> bool + Send + Sync>,
}
impl ControlService for AuthorizedControl {
    fn handle(
        &self,
        context: ControlContext,
        payload: &[u8],
    ) -> std::result::Result<Vec<u8>, ControlServiceError> {
        if !(self.authorize)() {
            return Err(ControlServiceError::new("game admission revoked"));
        }
        self.inner.handle(context, payload)
    }
}

struct ManagedAttachment<S> {
    host: LiveMatchHost<S>,
    stopped: broadcast::Receiver<()>,
    control: Arc<dyn WebRtcControlService>,
}

/// One authoritative match and one setup lobby. Games retain their runtime/storage.
pub struct WebRtcHost<S> {
    lobby: Lobby,
    match_id: MatchId,
    state: ConnectionState<S>,
    config: WebRtcHostConfig,
    managed: Option<ManagedAttachment<S>>,
    content: Option<Arc<dyn super::WebRtcContentService>>,
    content_permits: Arc<Semaphore>,
}

impl<S: GameSimulation> WebRtcHost<S> {
    pub fn new(
        lobby: Lobby,
        match_id: MatchId,
        runtime: Arc<Mutex<MatchRuntime<S>>>,
        control: Arc<dyn ControlService>,
        config: WebRtcHostConfig,
    ) -> Result<Self> {
        if lobby.participant_id() != lobby.host_participant_id() {
            return Err(WebRtcError("native host must create its lobby"));
        }
        let (snapshots, _) = broadcast::channel(connection::SNAPSHOT_CHANNEL_DEPTH);
        let (shutdown, _) = broadcast::channel(1);
        let state = ConnectionState {
            runtime,
            control,
            control_handlers: Arc::new(Semaphore::new(connection::MAX_CONCURRENT_CONTROL_HANDLERS)),
            snapshots,
            shutdown,
            admission_gate: None,
        };
        Ok(Self {
            lobby,
            match_id,
            state,
            config,
            managed: None,
            content: None,
            content_permits: Arc::new(Semaphore::new(4)),
        })
    }

    /// Attaches to an already serving managed match. Its existing owner retains
    /// ticking, persistence, recovery and shutdown. Stopping this sidecar closes
    /// only its peers; it never drains or freezes the shared runtime.
    pub fn attach(
        lobby: Lobby,
        host: LiveMatchHost<S>,
        match_id: MatchId,
        control: Arc<dyn WebRtcControlService>,
        config: WebRtcHostConfig,
    ) -> Result<Self> {
        let (state, stopped) = managed_state(&host, &match_id)?;
        if lobby.participant_id() != lobby.host_participant_id() {
            return Err(WebRtcError("native host must create its lobby"));
        }
        Ok(Self {
            lobby,
            match_id,
            state,
            config,
            content: None,
            content_permits: Arc::new(Semaphore::new(4)),
            managed: Some(ManagedAttachment {
                host,
                stopped,
                control,
            }),
        })
    }

    /// Enables opaque bounded requests on an optional channel of the admitted peer.
    pub fn with_content_service(mut self, service: Arc<dyn super::WebRtcContentService>) -> Self {
        self.content = Some(service);
        self
    }

    pub fn lobby_id(&self) -> &str {
        self.lobby.lobby_id()
    }

    /// The game authorizes an authenticated lobby participant before any ticket or
    /// peer connection is created. An invite alone never admits a game session.
    /// Keep this future alive for the host lifetime; resolve `shutdown` to stop.
    pub async fn run(
        self,
        authorize: impl Fn(&str) -> bool + Send + Sync + 'static,
        shutdown: impl Future<Output = ()>,
    ) -> Result<()> {
        let authorize = Arc::new(authorize);
        let approval = Arc::clone(&authorize);
        self.run_with_authorization(
            move |participant, _authorization| {
                let approved = approval(&participant);
                async move { approved }
            },
            move |participant| authorize(participant),
            shutdown,
        )
        .await
    }

    /// Approval has a 60-second bound and runs without blocking signaling or
    /// heartbeat. Revalidation must be cheap and current; it runs before ticket
    /// creation and again under the runtime lock immediately before admission.
    pub async fn run_with_authorization<F, A>(
        mut self,
        authorize: impl Fn(String, Vec<u8>) -> F + Send + Sync + 'static,
        revalidate: A,
        shutdown: impl Future<Output = ()>,
    ) -> Result<()>
    where
        F: Future<Output = bool> + Send + 'static,
        A: Fn(&str) -> bool + Send + Sync + 'static,
    {
        let tick_hz = self.state.runtime.lock().await.tick_hz();
        if tick_hz == 0 {
            return Err(WebRtcError("invalid tick frequency"));
        }
        let _ticks = self
            .managed
            .is_none()
            .then(|| crate::transport::spawn_tick_loop(self.state.clone(), tick_hz));
        let mut lobby = self.lobby;
        let authorize = Arc::new(authorize);
        let revalidate = Arc::new(revalidate);
        let mut approvals = JoinSet::new();
        let mut pending = std::collections::BTreeSet::new();
        let mut managed_poll = tokio::time::interval(Duration::from_millis(100));
        let mut peers = JoinSet::new();
        let mut participants: BTreeMap<String, Arc<dyn webrtc::peer_connection::PeerConnection>> =
            BTreeMap::new();
        tokio::pin!(shutdown);
        let mut heartbeat = tokio::time::interval(Duration::from_secs(20));
        heartbeat.tick().await;
        let result = loop {
            tokio::select! {
                _ = &mut shutdown => break Ok(()),
                _ = async { match &mut self.managed { Some(managed) => {let _ = managed.stopped.recv().await;}, None => std::future::pending().await } } => break Ok(()),
                _ = managed_poll.tick(), if self.managed.is_some() => {
                    if let Some(managed) = &self.managed && (!managed.host.is_serving() || managed.host.is_draining().await) { break Ok(()); }
                },
                _ = heartbeat.tick() => {
                    if let Err(error) = lobby.heartbeat().await {break Err(error);}
                },
                event = async {
                    tokio::select! {
                        event = lobby.receive() => event.map(|event| (event, false)),
                        approved = approvals.join_next(), if !approvals.is_empty() => {
                            match approved {
                                Some(Ok((participant, event, approved))) => {
                                    pending.remove(&participant);
                                    Ok((if approved {event} else {serde_json::Value::Null}, true))
                                },
                                _ => Ok((serde_json::Value::Null, true)),
                            }
                        }
                    }
                } => {
                    let (event, approved) = match event { Ok(event)=>event, Err(error)=>break Err(error) };
                    match event["type"].as_str() {
                        Some("signal") => {
                            let Some(from) = event["from"].as_str() else {continue};
                            let payload = &event["payload"];
                            if payload["adapter"] != "game-server-webrtc-v1" || payload["matchId"] != self.match_id.as_str()
                                || payload["kind"] != "offer" { continue; }
                            if !approved {
                                let authorization = match payload.get("authorization") {
                                    None => Vec::new(),
                                    Some(value) => match serde_json::from_value::<Vec<u8>>(value.clone()) {
                                        Ok(bytes) if bytes.len() <= super::MAX_AUTHORIZATION_BYTES => bytes,
                                        _ => continue,
                                    },
                                };
                                if approvals.len() >= 32 || !pending.insert(from.to_owned()) {continue;}
                                let participant = from.to_owned();
                                let authorize = Arc::clone(&authorize);
                                approvals.spawn(async move {
                                    let approved = timeout(Duration::from_secs(60), async {authorize(participant.clone(), authorization).await}).await.unwrap_or(false);
                                    (participant, event, approved)
                                });
                                continue;
                            }
                            if !revalidate(from) {continue;}
                            if peers.len() >= 32 {continue;}
                            if let Some(previous) = participants.remove(from) {previous.close().await?;}
                            if participants.len() >= 16 {continue;}
                            let offer = match serde_json::from_value(payload["description"].clone()) {Ok(offer)=>offer,Err(_)=>continue};
                            let servers = match lobby.turn_servers().await {Ok(servers)=>servers,Err(error)=>break Err(error)};
                            let content_enabled = self.content.is_some() && payload["contentVersion"] == 1;
                            let negotiation = async {
                                let mut peer = Peer::new(servers,self.config.relay_only,&self.config.bind_ip).await?;
                                peer.pc.set_remote_description(offer).await?;
                                let mut channels = peer.create_channels().await?;
                                if content_enabled {channels.content = Some(peer.create_content_channel().await?);}
                                let answer = peer.local_description(false).await?;
                                let mut ticket = [0_u8;32];
                                SystemRandom::new().fill(&mut ticket).map_err(|_| WebRtcError("ticket randomness unavailable"))?;
                                // WSS targets the authenticated participant; this ticket is single-use,
                                // match/lobby/DTLS-offer scoped and expires with the negotiation deadline.
                                lobby.signal(from,json!({"adapter":"game-server-webrtc-v1","kind":"answer","matchId":self.match_id.as_str(),"description":answer,"ticket":ticket,"contentVersion":if content_enabled {Some(1)} else {None}})).await?;
                                Ok::<_,WebRtcError>((peer,channels,ticket))
                            };
                            let (peer,channels,ticket) = match timeout(NEGOTIATE_TIMEOUT,negotiation).await {Ok(Ok(result))=>result,Ok(Err(error))=>{eprintln!("WebRTC negotiation failed: {error}");continue},Err(_)=>{eprintln!("WebRTC host negotiation timed out");continue}};
                            let mut state = self.state.clone();
                            if let Some(managed) = &self.managed {
                                state.control = Arc::new(ParticipantControl { service: Arc::clone(&managed.control), participant_id: from.to_owned(), match_id: self.match_id.clone() });
                            }
                            let participant = from.to_owned();
                            let finished = participant.clone();
                            let connection = Arc::clone(&peer.pc);
                            let finished_connection = Arc::clone(&connection);
                            let authorize = Arc::clone(&revalidate);
                            let managed_host = self.managed.as_ref().map(|managed| managed.host.clone());
                            let content = self.content.as_ref().filter(|_|content_enabled).map(|service| super::content::ContentBinding {service:Arc::clone(service), participant: participant.clone(), match_id: self.match_id.clone(), permits: Arc::clone(&self.content_permits)});
                            peers.spawn(async move {
                                let authorized_participant = finished.clone();
                                let result = serve_peer(peer,channels,ticket,state,content,move ||authorize(&authorized_participant) && managed_host.as_ref().is_none_or(|host|host.is_serving())).await;
                                if let Err(error) = &result {eprintln!("WebRTC peer ended: {error}");}
                                (finished,finished_connection,result)
                            });
                            participants.insert(participant,connection);
                        },
                        Some("participant-disconnected") => {
                            if let Some(id) = event["participantId"].as_str()
                                && let Some(connection) = participants.remove(id) { connection.close().await?; }
                        },
                        Some("connected"|"participant-connected"|"pong") => {},
                        // A peer can leave while its answer is gathering. Refusing
                        // that one targeted signal must not stop the whole match.
                        Some("error") => eprintln!("setup service refused a signaling operation"),
                        _ => {},
                    }
                },
                completed = peers.join_next(), if !peers.is_empty() => {
                    if let Some(Ok((participant,connection,_result))) = completed
                        && participants.get(&participant).is_some_and(|current|Arc::ptr_eq(current,&connection)) {participants.remove(&participant);}
                },
            }
        };
        if self.managed.is_none() {
            self.state.runtime.lock().await.begin_drain();
        }
        let _ = self.state.shutdown.send(());
        // Gracefully release leases and peer connections before dropping their owners.
        while !peers.is_empty() {
            if timeout(Duration::from_secs(6), peers.join_next())
                .await
                .is_err()
            {
                break;
            }
        }
        if self.managed.is_none() {
            self.state.runtime.lock().await.freeze_for_recovery();
        }
        result
    }
}

fn managed_state<S: GameSimulation>(
    host: &LiveMatchHost<S>,
    id: &MatchId,
) -> Result<(ConnectionState<S>, broadcast::Receiver<()>)> {
    if !host.is_serving() {
        return Err(WebRtcError("managed host is not serving"));
    }
    let hosted = host.get(id).ok_or(WebRtcError("unknown managed match"))?;
    let stopped = hosted.shutdown.subscribe();
    let (shutdown, _) = broadcast::channel(1);
    Ok((
        ConnectionState {
            runtime: Arc::clone(&hosted.runtime),
            snapshots: hosted.snapshots.clone(),
            shutdown,
            control: Arc::new(crate::RejectControlService),
            control_handlers: Arc::new(Semaphore::new(connection::MAX_CONCURRENT_CONTROL_HANDLERS)),
            admission_gate: Some(host.admission_gate()),
        },
        stopped,
    ))
}

async fn serve_peer<S: GameSimulation>(
    mut peer: Peer,
    channels: Channels,
    ticket: [u8; 32],
    mut state: ConnectionState<S>,
    content: Option<super::content::ContentBinding>,
    authorize: impl Fn() -> bool + Send + Sync + 'static,
) -> Result<()> {
    let authorize: Arc<dyn Fn() -> bool + Send + Sync> = Arc::new(authorize);
    state.control = Arc::new(AuthorizedControl {
        inner: Arc::clone(&state.control),
        authorize: Arc::clone(&authorize),
    });
    let mut shutdown = state.shutdown.subscribe();
    let setup = async {
        peer::opened(&channels.session).await?;
        peer::send(&channels.session, &super::ADMISSION_READY).await?;
        let admission = peer::receive(&channels.session, 48).await?;
        if admission.len() != 32 && admission.len() != 48 || admission[..32] != ticket {
            return Err(WebRtcError("invalid admission ticket"));
        }
        let admission = if admission.len() == 48 {
            let mut token = [0; 16];
            token.copy_from_slice(&admission[32..]);
            AdmissionRequest::Reconnect(ReconnectToken(token))
        } else {
            AdmissionRequest::New
        };
        Ok::<_, WebRtcError>((channels, admission))
    };
    let (channels, admission) = timeout(NEGOTIATE_TIMEOUT, setup)
        .await
        .map_err(|_| WebRtcError("admission timed out"))??;
    // Authorization may have changed while ICE and the ticket exchange ran.
    if !authorize() {
        return Err(WebRtcError("game admission revoked"));
    }
    let token = connection::generate_reconnect_token()
        .map_err(|_| WebRtcError("reconnect randomness unavailable"))?;
    let (lease, welcome) = {
        let gate = match &state.admission_gate {
            Some(gate) => Some(gate.read().await),
            None => None,
        };
        if gate.as_ref().is_some_and(|gate| **gate) || !authorize() {
            return Err(WebRtcError("game admission revoked"));
        }
        let mut runtime = state.runtime.lock().await;
        if !authorize() {
            return Err(WebRtcError("game admission revoked"));
        }
        let lease = match admission {
            AdmissionRequest::New => runtime.admit(token),
            AdmissionRequest::Reconnect(old) => runtime.reconnect(old, token),
        }
        .map_err(|_| WebRtcError("authoritative admission refused"))?;
        let welcome = crate::encode_welcome(Welcome {
            player_id: lease.player_id,
            tick_hz: runtime.tick_hz(),
            max_players: u16::try_from(runtime.max_players())
                .map_err(|_| WebRtcError("capacity exceeds wire limit"))?,
            current_tick: runtime.current_tick(),
            connection_epoch: lease.connection_epoch,
            reconnect_token: lease.reconnect_token.0,
            reconnect_grace_ticks: lease.reconnect_grace_ticks,
        });
        (lease, welcome)
    };
    let result = match peer::send(&channels.session, &welcome).await {
        Err(error) => {
            connection::rollback_failed_welcome(&mut *state.runtime.lock().await, admission, lease)
                .map_err(|_| WebRtcError("welcome rollback failed"))?;
            Err(error)
        }
        Ok(()) => {
            established(
                &channels,
                &mut peer,
                lease,
                &state,
                &mut shutdown,
                &authorize,
                content,
            )
            .await
        }
    };
    state
        .runtime
        .lock()
        .await
        .disconnect(lease.player_id, lease.connection_epoch);
    peer.pc.close().await?;
    result
}

async fn established<S: GameSimulation>(
    channels: &Channels,
    peer: &mut Peer,
    lease: SessionLease,
    state: &ConnectionState<S>,
    shutdown: &mut broadcast::Receiver<()>,
    authorize: &Arc<dyn Fn() -> bool + Send + Sync>,
    content: Option<super::content::ContentBinding>,
) -> Result<()> {
    let mut snapshots = state.snapshots.subscribe();
    let mut newest_tick = None;
    // Translate ordered message boundaries to the existing reliable byte-stream
    // interpreter, preserving its header/ack/watermark/rejection rules.
    let (lane_local, lane_remote) = tokio::io::duplex(8 * 1024);
    let (lane_read, lane_write) = tokio::io::split(lane_remote);
    let mut lane_tasks = JoinSet::new();
    let lane_authorize = Arc::clone(authorize);
    lane_tasks.spawn(connection::run_command_lane_with_authorization(
        lane_read,
        lane_write,
        Arc::clone(&state.runtime),
        lease,
        Duration::from_secs(10),
        move || lane_authorize(),
    ));
    let (mut lane_recv, mut lane_send) = tokio::io::split(lane_local);
    let mut lane_header = false;
    let mut acks = [0_u8; crate::RELIABLE_COMMAND_ACK_BYTES];
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut controls = JoinSet::new();
    let mut content_tasks: JoinSet<Result<Vec<u8>>> = JoinSet::new();
    let content = content.map(Arc::new);
    let mut authorization_poll = tokio::time::interval(Duration::from_millis(100));
    loop {
        if !authorize() {
            return Err(WebRtcError("game admission revoked"));
        }
        tokio::select! {
            frame = peer::receive(&channels.realtime,peer::DATAGRAM_BUDGET) => {
                let command = crate::decode_command(&frame?).map_err(|_| WebRtcError("invalid command"))?;
                let mut runtime = state.runtime.lock().await;
                if !authorize() {return Err(WebRtcError("game admission revoked"));}
                let outcome = runtime.submit_command(lease.player_id,lease.connection_epoch,command.sequence,&command.payload);
                drop(runtime);
                match outcome {
                    Ok(_) | Err(crate::RuntimeError::Frozen) => {},
                    Err(crate::RuntimeError::Simulation(error)) if error.command_rejection().is_some()=> {
                        let frame = crate::encode_command_rejection(command.sequence,error.command_rejection().unwrap_or_default()).map_err(|_| WebRtcError("invalid rejection"))?;
                        peer::send(&channels.session,&frame).await?;
                    },
                    Err(_)=>return Err(WebRtcError("authoritative command refused")),
                }
            },
            frame = peer::receive(&channels.commands,crate::MAX_COMMAND_PAYLOAD_BYTES+crate::COMMAND_HEADER_BYTES) => {
                let frame = frame?;
                if !authorize() {return Err(WebRtcError("game admission revoked"));}
                if !lane_header {
                    crate::decode_command_lane_header(&frame).map_err(|_| WebRtcError("invalid command lane header"))?;
                    // The shared interpreter consumes the remainder after GSCL.
                    lane_send.write_all(&frame[4..]).await.map_err(|_| WebRtcError("command lane closed"))?;
                    lane_header = true;
                } else {
                    crate::decode_command(&frame).map_err(|_| WebRtcError("invalid reliable command"))?;
                    lane_send.write_all(&frame).await.map_err(|_| WebRtcError("command lane closed"))?;
                }
            },
            ack = lane_recv.read_exact(&mut acks) => {
                ack.map_err(|_| WebRtcError("command lane closed"))?;
                peer::send(&channels.commands,&acks).await?;
            },
            frame = peer::receive(&channels.session,peer::RELIABLE_LIMIT) => {
                if !authorize() {return Err(WebRtcError("game admission revoked"));}
                let request = crate::decode_control_request(&frame?).map_err(|_| WebRtcError("invalid reliable control"))?;
                // GSCT has no request id: this ordered channel carries one exchange at a time.
                if !controls.is_empty() {return Err(WebRtcError("control concurrency exceeded"));}
                let runtime = Arc::clone(&state.runtime);
                let control = Arc::clone(&state.control);
                let handlers = Arc::clone(&state.control_handlers);
                controls.spawn(async move {
                    timeout(Duration::from_secs(5),connection::dispatch_control(runtime,ControlContext{player_id:lease.player_id,connection_epoch:lease.connection_epoch},control,handlers,request.payload)).await
                });
            },
            result = controls.join_next(), if !controls.is_empty() => {
                let result = result.ok_or(WebRtcError("control missing"))?.map_err(|_| WebRtcError("control task failed"))?
                    .map_err(|_| WebRtcError("control timed out"))?.map_err(|_| WebRtcError("control dispatch failed"))?;
                let frame = match result {Ok(payload)=>crate::encode_control_response(true,&payload),Err(_)=>crate::encode_control_response(false,&[])}.map_err(|_| WebRtcError("invalid control response"))?;
                peer::send(&channels.session,&frame).await?;
            },
            publication = snapshots.recv()=> {
                let publication = match publication {Ok(publication)=>publication,Err(broadcast::error::RecvError::Lagged(_))=>continue,Err(_)=>return Err(WebRtcError("snapshot source stopped"))};
                let runtime = state.runtime.lock().await;
                if !authorize() {return Err(WebRtcError("game admission revoked"));}
                if let SnapshotPublication::CommittedPlayers(frames) = &publication && !frames.contains_key(&lease.player_id) {
                    runtime.validate_connection(lease.player_id,lease.connection_epoch).map_err(|_| WebRtcError("snapshot fencing refused"))?;
                    continue;
                }
                let frame = connection::snapshot_for_connection(&runtime,lease,&publication).map_err(|_| WebRtcError("snapshot fencing refused"))?;
                let tick = crate::protocol::snapshot_frame_tick(&frame).map_err(|_| WebRtcError("invalid snapshot"))?;
                if newest_tick.is_some_and(|newest|tick<=newest) {continue;}
                let fragments = if frame.len() <= peer::DATAGRAM_BUDGET {vec![frame.into_owned()]}
                    else {crate::encode_snapshot_fragments(&frame,peer::DATAGRAM_BUDGET).map_err(|_| WebRtcError("snapshot exceeds fragment bound"))?};
                // The nonblocking SCTP enqueue and epoch validation share the lock,
                // so a replacement cannot race a stale projection into publication.
                for frame in fragments {if channels.realtime.try_send(bytes::BytesMut::from(frame.as_slice())).await.is_err() {break;}}
                newest_tick = Some(tick);
            },
            _ = authorization_poll.tick()=>{},
            _ = shutdown.recv()=>return Ok(()),
            _ = peer.closed.changed()=>return Ok(()),
            _ = peer.channels.recv()=>return Err(WebRtcError("extra data channel refused")),
            _ = lane_tasks.join_next()=>return Err(WebRtcError("command lane stopped")),
            result = content_tasks.join_next(), if !content_tasks.is_empty() => {
                let response = result.ok_or(WebRtcError("content task missing"))?.map_err(|_|WebRtcError("content task failed"))??;
                let runtime = state.runtime.lock().await;
                if !authorize() || runtime.is_draining() {return Err(WebRtcError("content access revoked"));}
                runtime.validate_connection(lease.player_id,lease.connection_epoch).map_err(|_|WebRtcError("content fencing refused"))?;
                // Only nonblocking provider enqueue shares this lock, never content I/O.
                channels.content.as_ref().ok_or(WebRtcError("content unavailable"))?.try_send(bytes::BytesMut::from(response.as_slice())).await?;
            },
            request = async {match &channels.content {Some(channel)=>peer::receive(channel,super::MAX_CONTENT_BYTES).await,None=>std::future::pending().await}} => {
                let request = request?;
                if !content_tasks.is_empty() {return Err(WebRtcError("content concurrency exceeded"));}
                let binding = Arc::clone(content.as_ref().ok_or(WebRtcError("content unavailable"))?);
                let runtime = Arc::clone(&state.runtime);
                let authorize = Arc::clone(authorize);
                content_tasks.spawn(async move {
                    timeout(Duration::from_secs(5), async {
                        // One active operation per peer; four across the host. No queued
                        // external jobs. A paced chunk leaves room for gameplay traffic.
                        let Ok(_permit) = binding.permits.try_acquire() else {return Ok(vec![0]);};
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        {
                            let runtime = runtime.lock().await;
                            if !authorize() || runtime.is_draining() {return Err(WebRtcError("content access revoked"));}
                            runtime.validate_connection(lease.player_id,lease.connection_epoch).map_err(|_|WebRtcError("content fencing refused"))?;
                        }
                        let result = binding.service.handle(&binding.participant,&binding.match_id,ControlContext{player_id:lease.player_id,connection_epoch:lease.connection_epoch},&request).await;
                        let mut response = vec![u8::from(result.is_ok())];
                        if let Ok(payload) = result {
                            if payload.len()>super::MAX_CONTENT_BYTES {return Err(WebRtcError("content response exceeds limit"));}
                            response.extend_from_slice(&payload);
                        }
                        Ok(response)
                    }).await.map_err(|_|WebRtcError("content service timed out"))?
                });
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct EchoContent;
    impl super::super::WebRtcContentService for EchoContent {
        fn handle<'a>(
            &'a self,
            _: &'a str,
            _: &'a MatchId,
            _: ControlContext,
            request: &'a [u8],
        ) -> super::super::WebRtcContentFuture<'a> {
            Box::pin(async move { Ok(request.to_vec()) })
        }
    }
    impl WebRtcControlService for EchoContent {
        fn handle(
            &self,
            _: &str,
            _: &MatchId,
            _: ControlContext,
            payload: &[u8],
        ) -> std::result::Result<Vec<u8>, ControlServiceError> {
            Ok(payload.to_vec())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn saturated_content_refuses_without_disconnect_and_recovers() {
        timeout(Duration::from_secs(10), async {
            let id = MatchId::new("saturated").unwrap();
            let mut matches = crate::MatchHost::new(1).unwrap();
            matches
                .insert(
                    id.clone(),
                    MatchRuntime::new(crate::DemoSimulation::default(), 400),
                )
                .unwrap();
            let host = LiveMatchHost::new(matches);
            host.claim().unwrap();
            host.start().await;
            let (lobby, guest, _signaling) = super::super::setup::test_support::lobbies().await;
            let adapter = WebRtcHost::attach(
                lobby,
                host.clone(),
                id.clone(),
                Arc::new(EchoContent),
                WebRtcHostConfig {
                    bind_ip: "127.0.0.1".to_owned(),
                    relay_only: false,
                },
            )
            .unwrap()
            .with_content_service(Arc::new(EchoContent));
            // Exhaust the actual shared handler pool without extra peers or jobs.
            let pool = Arc::clone(&adapter.content_permits);
            let saturated = pool.acquire_many(4).await.unwrap();
            let (stop, shutdown) = tokio::sync::oneshot::channel();
            let mut tasks = JoinSet::new();
            tasks.spawn(adapter.run(|_| true, async {
                let _ = shutdown.await;
            }));
            let mut client = super::super::WebRtcClient::connect_with_content_authorization(
                guest,
                id,
                "127.0.0.1",
                false,
                Vec::new(),
            )
            .await
            .unwrap();
            let content = client.content_client().unwrap();
            assert_eq!(
                content.request(b"full").await,
                Err(super::super::WebRtcContentError::Refused)
            );
            assert_eq!(
                client.control(b"still connected").await.unwrap().payload,
                b"still connected"
            );
            assert!(client.snapshot().await.unwrap().tick > 0);
            drop(saturated);
            assert_eq!(
                content.request(b"capacity restored").await.unwrap(),
                b"capacity restored"
            );
            client.close().await.unwrap();
            stop.send(()).unwrap();
            tasks.join_next().await.unwrap().unwrap().unwrap();
            host.stop().await;
        })
        .await
        .expect("bounded content saturation probe");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ready_channel_never_admits_a_wrong_ticket_or_revoked_participant() {
        timeout(Duration::from_secs(10), async {
            for (valid_ticket, authorized) in [(false, true), (true, false)] {
                let mut client = Peer::new(Vec::new(), false, "127.0.0.1").await.unwrap();
                let client_channels = client.create_channels().await.unwrap();
                let offer = client.local_description(true).await.unwrap();
                let mut server = Peer::new(Vec::new(), false, "127.0.0.1").await.unwrap();
                server.pc.set_remote_description(offer).await.unwrap();
                let server_channels = server.create_channels().await.unwrap();
                let answer = server.local_description(false).await.unwrap();
                client.pc.set_remote_description(answer).await.unwrap();

                let runtime = Arc::new(Mutex::new(MatchRuntime::new(
                    crate::DemoSimulation::default(),
                    400,
                )));
                let (snapshots, _) = broadcast::channel(1);
                let (shutdown, _) = broadcast::channel(1);
                let state = ConnectionState {
                    runtime: Arc::clone(&runtime),
                    control: Arc::new(crate::RejectControlService),
                    control_handlers: Arc::new(Semaphore::new(
                        connection::MAX_CONCURRENT_CONTROL_HANDLERS,
                    )),
                    snapshots,
                    shutdown,
                    admission_gate: None,
                };
                let mut tasks = JoinSet::new();
                tasks.spawn(serve_peer(
                    server,
                    server_channels,
                    [42; 32],
                    state,
                    None,
                    move || authorized,
                ));
                peer::opened(&client_channels.session).await.unwrap();
                assert_eq!(
                    peer::receive(&client_channels.session, 5).await.unwrap(),
                    super::super::ADMISSION_READY
                );
                assert_eq!(runtime.lock().await.slot_count(), 0);
                peer::send(
                    &client_channels.session,
                    &[if valid_ticket { 42 } else { 43 }; 32],
                )
                .await
                .unwrap();
                let failure = tasks.join_next().await.unwrap().unwrap().unwrap_err();
                assert_eq!(
                    failure,
                    WebRtcError(if valid_ticket {
                        "game admission revoked"
                    } else {
                        "invalid admission ticket"
                    })
                );
                let runtime = runtime.lock().await;
                assert_eq!(runtime.active_count(), 0);
                assert_eq!(runtime.slot_count(), 0);
                drop(runtime);
                client.pc.close().await.unwrap();
            }
        })
        .await
        .expect("bounded native admission probe");
    }
}

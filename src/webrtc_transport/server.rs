use super::peer::{self, Channels, NEGOTIATE_TIMEOUT, Peer};
use super::{Lobby, Result, WebRtcError};
use crate::connection::{self, AdmissionRequest, ConnectionState, SnapshotPublication};
use crate::{
    ControlContext, ControlService, GameSimulation, MatchId, MatchRuntime, ReconnectToken,
    SessionLease, Welcome,
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

/// One authoritative match and one setup lobby. Games retain their runtime/storage.
pub struct WebRtcHost<S> {
    lobby: Lobby,
    match_id: MatchId,
    state: ConnectionState<S>,
    config: WebRtcHostConfig,
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
        })
    }

    pub fn lobby_id(&self) -> &str {
        self.lobby.lobby_id()
    }

    /// The game authorizes an authenticated lobby participant before any ticket or
    /// peer connection is created. An invite alone never admits a game session.
    /// Keep this future alive for the host lifetime; resolve `shutdown` to stop.
    pub async fn run(
        self,
        authorize: impl Fn(&str) -> bool,
        shutdown: impl Future<Output = ()>,
    ) -> Result<()> {
        let tick_hz = self.state.runtime.lock().await.tick_hz();
        if tick_hz == 0 {
            return Err(WebRtcError("invalid tick frequency"));
        }
        let _ticks = crate::transport::spawn_tick_loop(self.state.clone(), tick_hz);
        let mut lobby = self.lobby;
        let mut peers = JoinSet::new();
        let mut participants: BTreeMap<String, Arc<dyn webrtc::peer_connection::PeerConnection>> =
            BTreeMap::new();
        tokio::pin!(shutdown);
        let mut heartbeat = tokio::time::interval(Duration::from_secs(20));
        heartbeat.tick().await;
        let result = loop {
            tokio::select! {
                _ = &mut shutdown => break Ok(()),
                _ = heartbeat.tick() => {
                    // An authenticated ping keeps the signaling socket observable.
                    use futures_util::SinkExt;
                    if lobby.socket.send(tokio_tungstenite::tungstenite::Message::Text("{\"type\":\"ping\"}".into())).await.is_err() {
                        break Err(WebRtcError("host signaling heartbeat failed"));
                    }
                },
                event = lobby.receive() => {
                    let event = match event { Ok(event)=>event, Err(error)=>break Err(error) };
                    match event["type"].as_str() {
                        Some("signal") => {
                            let Some(from) = event["from"].as_str() else {continue};
                            let payload = &event["payload"];
                            if payload["adapter"] != "game-server-webrtc-v1" || payload["matchId"] != self.match_id.as_str()
                                || payload["kind"] != "offer" || !authorize(from) { continue; }
                            if let Some(previous) = participants.remove(from) {previous.close().await?;}
                            if participants.len() >= 16 {continue;}
                            let offer = match serde_json::from_value(payload["description"].clone()) {Ok(offer)=>offer,Err(_)=>continue};
                            let servers = match lobby.turn_servers().await {Ok(servers)=>servers,Err(error)=>break Err(error)};
                            let negotiation = async {
                                let mut peer = Peer::new(servers,self.config.relay_only,&self.config.bind_ip).await?;
                                eprintln!("WebRTC host peer created");
                                peer.pc.set_remote_description(offer).await?;
                                let channels = peer.create_channels().await?;
                                let answer = peer.local_description(false).await?;
                                eprintln!("WebRTC host ICE gathered");
                                let mut ticket = [0_u8;32];
                                SystemRandom::new().fill(&mut ticket).map_err(|_| WebRtcError("ticket randomness unavailable"))?;
                                // WSS targets the authenticated participant; this ticket is single-use,
                                // match/lobby/DTLS-offer scoped and expires with the negotiation deadline.
                                lobby.signal(from,json!({"adapter":"game-server-webrtc-v1","kind":"answer","matchId":self.match_id.as_str(),"description":answer,"ticket":ticket})).await?;
                                Ok::<_,WebRtcError>((peer,channels,ticket))
                            };
                            let (peer,channels,ticket) = match timeout(NEGOTIATE_TIMEOUT,negotiation).await {Ok(Ok(result))=>result,Ok(Err(error))=>{eprintln!("WebRTC negotiation failed: {error}");continue},Err(_)=>{eprintln!("WebRTC host negotiation timed out");continue}};
                            let state = self.state.clone();
                            let participant = from.to_owned();
                            let finished = participant.clone();
                            let connection = Arc::clone(&peer.pc);
                            let finished_connection = Arc::clone(&connection);
                            peers.spawn(async move {
                                let result = serve_peer(peer,channels,ticket,state).await;
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
                        Some("error") => break Err(WebRtcError("setup service rejected host signaling")),
                        _ => {},
                    }
                },
                completed = peers.join_next(), if !peers.is_empty() => {
                    if let Some(Ok((participant,connection,_result))) = completed
                        && participants.get(&participant).is_some_and(|current|Arc::ptr_eq(current,&connection)) {participants.remove(&participant);}
                },
            }
        };
        self.state.runtime.lock().await.begin_drain();
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
        self.state.runtime.lock().await.freeze_for_recovery();
        result
    }
}

async fn serve_peer<S: GameSimulation>(
    mut peer: Peer,
    channels: Channels,
    ticket: [u8; 32],
    state: ConnectionState<S>,
) -> Result<()> {
    let mut shutdown = state.shutdown.subscribe();
    let setup = async {
        peer::opened(&channels.session).await?;
        eprintln!("WebRTC host session open");
        let admission = peer::receive(&channels.session, 48).await?;
        eprintln!("WebRTC host admission received");
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
    let admitted = timeout(NEGOTIATE_TIMEOUT, setup).await;
    if admitted.is_err() {
        let report = peer
            .pc
            .get_stats(
                std::time::Instant::now(),
                webrtc::peer_connection::StatsSelector::None,
            )
            .await;
        for entry in report.iter() {
            if let webrtc::peer_connection::RTCStatsReportEntry::DataChannel(channel) = entry {
                eprintln!(
                    "[DEBUG-86] admission channel={} received={} sent={}",
                    channel.data_channel_identifier,
                    channel.messages_received,
                    channel.messages_sent
                );
            }
        }
    }
    let (channels, admission) = admitted.map_err(|_| WebRtcError("admission timed out"))??;
    let token = connection::generate_reconnect_token()
        .map_err(|_| WebRtcError("reconnect randomness unavailable"))?;
    let (lease, welcome) = {
        let mut runtime = state.runtime.lock().await;
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
        Ok(()) => established(&channels, &mut peer, lease, &state, &mut shutdown).await,
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
) -> Result<()> {
    let mut snapshots = state.snapshots.subscribe();
    let mut newest_tick = None;
    // Translate ordered message boundaries to the existing reliable byte-stream
    // interpreter, preserving its header/ack/watermark/rejection rules.
    let (lane_local, lane_remote) = tokio::io::duplex(8 * 1024);
    let (lane_read, lane_write) = tokio::io::split(lane_remote);
    let mut lane_tasks = JoinSet::new();
    lane_tasks.spawn(connection::run_command_lane(
        lane_read,
        lane_write,
        Arc::clone(&state.runtime),
        lease,
        Duration::from_secs(10),
    ));
    let (mut lane_recv, mut lane_send) = tokio::io::split(lane_local);
    let mut lane_header = false;
    let mut acks = [0_u8; crate::RELIABLE_COMMAND_ACK_BYTES];
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut controls = JoinSet::new();
    loop {
        tokio::select! {
            frame = peer::receive(&channels.realtime,peer::DATAGRAM_BUDGET) => {
                let command = crate::decode_command(&frame?).map_err(|_| WebRtcError("invalid command"))?;
                let outcome = state.runtime.lock().await.submit_command(lease.player_id,lease.connection_epoch,command.sequence,&command.payload);
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
                let request = crate::decode_control_request(&frame?).map_err(|_| WebRtcError("invalid reliable control"))?;
                if controls.len() >= 4 {return Err(WebRtcError("control concurrency exceeded"));}
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
                if let SnapshotPublication::CommittedPlayers(frames) = &publication && !frames.contains_key(&lease.player_id) {continue;}
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
            _ = shutdown.recv()=>return Ok(()),
            _ = peer.closed.changed()=>return Ok(()),
            _ = peer.channels.recv()=>return Err(WebRtcError("extra data channel refused")),
            _ = lane_tasks.join_next()=>return Err(WebRtcError("command lane stopped")),
        }
    }
}

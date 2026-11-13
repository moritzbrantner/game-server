use super::peer::{self, Channels, NEGOTIATE_TIMEOUT, Peer};
use super::{Lobby, Result, WebRtcError};
use crate::{
    ControlResponse, MatchId, ReconnectToken, ReliableCommandAck, SnapshotFrame,
    SnapshotReassembler, Welcome,
};
use serde_json::json;
use tokio::time::timeout;

/// Selected-path observations only; no SDP, addresses or ICE credentials.
#[derive(Clone, Debug, Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionMetrics {
    pub relayed: Option<bool>,
    pub ice_rtt_ms: Option<f64>,
    pub packets_sent: u64,
    pub packets_received: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub discarded_packets_on_send: u32,
}

/// Native guest session. Only the public lobby/match ids belong in an invite.
/// The owning application keeps this object in memory and serializes receives
/// on each channel. Session/command and realtime channels remain independent.
pub struct WebRtcClient {
    lobby: Lobby,
    peer: Peer,
    channels: Channels,
    welcome: Welcome,
    match_id: MatchId,
    bind_ip: String,
    relay_only: bool,
    reassembler: SnapshotReassembler,
}

impl WebRtcClient {
    /// Connect using a guest lobby participant. `relay_only` is an explicit
    /// diagnostic option; normal connections prefer direct ICE with TURN fallback.
    pub async fn connect(
        lobby: Lobby,
        match_id: MatchId,
        bind_ip: &str,
        relay_only: bool,
    ) -> Result<Self> {
        timeout(
            NEGOTIATE_TIMEOUT * 3,
            Self::negotiate(lobby, match_id, bind_ip.to_owned(), relay_only, None),
        )
        .await
        .map_err(|_| WebRtcError("guest connection timed out"))?
    }

    async fn negotiate(
        mut lobby: Lobby,
        match_id: MatchId,
        bind_ip: String,
        relay_only: bool,
        reconnect: Option<ReconnectToken>,
    ) -> Result<Self> {
        if lobby.participant_id() == lobby.host_participant_id() {
            return Err(WebRtcError("guest needs its own lobby participant"));
        }
        let mut peer = Peer::new(lobby.turn_servers().await?, relay_only, &bind_ip).await?;
        eprintln!("WebRTC guest peer created");
        let channels = peer.create_channels().await?;
        let offer = peer.local_description(true).await?;
        eprintln!("WebRTC guest ICE gathered");
        let host = lobby.host_participant_id().to_owned();
        lobby.signal(&host,json!({"adapter":"game-server-webrtc-v1","kind":"offer","matchId":match_id.as_str(),"description":offer})).await?;
        let ticket = loop {
            let event = lobby.receive().await?;
            if event["type"] == "error" {
                return Err(WebRtcError("setup rejected guest signaling"));
            }
            if event["type"] != "signal" || event["from"] != host {
                continue;
            }
            let payload = &event["payload"];
            if payload["adapter"] != "game-server-webrtc-v1"
                || payload["matchId"] != match_id.as_str()
                || payload["kind"] != "answer"
            {
                continue;
            }
            let ticket: [u8; 32] = serde_json::from_value(payload["ticket"].clone())?;
            peer.pc
                .set_remote_description(serde_json::from_value(payload["description"].clone())?)
                .await?;
            eprintln!("WebRTC guest answer accepted");
            break ticket;
        };
        peer::opened(&channels.session).await?;
        peer::opened(&channels.commands).await?;
        peer::opened(&channels.realtime).await?;
        eprintln!("WebRTC guest channels open");
        let mut admission = ticket.to_vec();
        if let Some(token) = reconnect {
            admission.extend_from_slice(&token.0);
        }
        peer::send(&channels.session, &admission).await?;
        let welcome =
            crate::decode_welcome(&peer::receive(&channels.session, crate::WELCOME_BYTES).await?)
                .map_err(|_| WebRtcError("invalid authoritative welcome"))?;
        peer::send(&channels.commands, &crate::encode_command_lane_header()).await?;
        Ok(Self {
            lobby,
            peer,
            channels,
            welcome,
            match_id,
            bind_ip,
            relay_only,
            reassembler: SnapshotReassembler::new(),
        })
    }

    pub fn welcome(&self) -> &Welcome {
        &self.welcome
    }

    pub async fn send_intent(&self, sequence: u32, payload: &[u8]) -> Result<()> {
        let frame =
            crate::encode_command(sequence, payload).map_err(|_| WebRtcError("invalid intent"))?;
        // Latest-wins input never waits for the reliable lane or a full send queue.
        self.channels
            .realtime
            .try_send(bytes::BytesMut::from(frame.as_slice()))
            .await?;
        Ok(())
    }

    /// Send one one-shot command and wait for the existing lane acknowledgement.
    /// Resend an unacknowledged sequence after reconnect; the runtime deduplicates it.
    pub async fn command(&mut self, sequence: u32, payload: &[u8]) -> Result<ReliableCommandAck> {
        let frame = crate::encode_command(sequence, payload)
            .map_err(|_| WebRtcError("invalid reliable command"))?;
        peer::send(&self.channels.commands, &frame).await?;
        let bytes = timeout(
            std::time::Duration::from_secs(10),
            peer::receive(&self.channels.commands, crate::RELIABLE_COMMAND_ACK_BYTES),
        )
        .await
        .map_err(|_| WebRtcError("command acknowledgement timed out"))??;
        let ack = crate::decode_reliable_command_ack(&bytes)
            .map_err(|_| WebRtcError("invalid command acknowledgement"))?;
        if ack.sequence != sequence {
            return Err(WebRtcError("unexpected command acknowledgement"));
        }
        Ok(ack)
    }

    pub async fn snapshot(&mut self) -> Result<SnapshotFrame> {
        loop {
            let frame = peer::receive(&self.channels.realtime, peer::DATAGRAM_BUDGET).await?;
            if let Some(snapshot) = self
                .reassembler
                .accept(&frame)
                .map_err(|_| WebRtcError("invalid snapshot datagram"))?
            {
                return Ok(snapshot);
            }
        }
    }

    /// One control exchange at a time on the native client; the server separately
    /// bounds exchanges and handler execution and rechecks connection fencing.
    pub async fn control(&mut self, payload: &[u8]) -> Result<ControlResponse> {
        let frame = crate::encode_control_request(payload)
            .map_err(|_| WebRtcError("invalid control request"))?;
        peer::send(&self.channels.session, &frame).await?;
        let response = timeout(
            std::time::Duration::from_secs(5),
            peer::receive(&self.channels.session, peer::RELIABLE_LIMIT),
        )
        .await
        .map_err(|_| WebRtcError("control response timed out"))??;
        crate::decode_control_response(&response)
            .map_err(|_| WebRtcError("invalid control response"))
    }

    /// Replace the transport while preserving the fenced runtime session and lane
    /// watermarks. The setup participant capability remains in this object only.
    pub async fn reconnect(self) -> Result<Self> {
        let token = ReconnectToken(self.welcome.reconnect_token);
        self.peer.pc.close().await?;
        timeout(
            NEGOTIATE_TIMEOUT * 3,
            Self::negotiate(
                self.lobby,
                self.match_id,
                self.bind_ip,
                self.relay_only,
                Some(token),
            ),
        )
        .await
        .map_err(|_| WebRtcError("reconnect timed out"))?
    }

    pub async fn close(self) -> Result<()> {
        self.peer.pc.close().await.map_err(Into::into)
    }

    pub async fn metrics(&self) -> ConnectionMetrics {
        use webrtc::peer_connection::{RTCIceCandidateType, RTCStatsReportEntry as Entry};
        let report = self
            .peer
            .pc
            .get_stats(
                std::time::Instant::now(),
                webrtc::peer_connection::StatsSelector::None,
            )
            .await;
        let mut metrics = ConnectionMetrics::default();
        for entry in report.iter() {
            if let Entry::Transport(transport) = entry
                && let Some(Entry::IceCandidatePair(pair)) =
                    report.get(&transport.selected_candidate_pair_id)
            {
                metrics.ice_rtt_ms = Some(pair.current_round_trip_time * 1000.0);
                metrics.packets_sent = pair.packets_sent;
                metrics.packets_received = pair.packets_received;
                metrics.bytes_sent = pair.bytes_sent;
                metrics.bytes_received = pair.bytes_received;
                metrics.discarded_packets_on_send = pair.packets_discarded_on_send;
                // The provider's pair entries use raw candidate ids, while its
                // candidate report entries prefix them with the endpoint role.
                let local = report.get(&pair.local_candidate_id).or_else(|| {
                    report.get(&format!("RTCLocalIceCandidate_{}", pair.local_candidate_id))
                });
                let remote = report.get(&pair.remote_candidate_id).or_else(|| {
                    report.get(&format!(
                        "RTCRemoteIceCandidate_{}",
                        pair.remote_candidate_id
                    ))
                });
                let local_relay = match local {
                    Some(Entry::LocalCandidate(candidate)) => {
                        Some(candidate.candidate_type == RTCIceCandidateType::Relay)
                    }
                    _ => None,
                };
                let remote_relay = match remote {
                    Some(Entry::RemoteCandidate(candidate)) => {
                        Some(candidate.candidate_type == RTCIceCandidateType::Relay)
                    }
                    _ => None,
                };
                metrics.relayed = if local_relay == Some(true) || remote_relay == Some(true) {
                    Some(true)
                } else if local_relay == Some(false) && remote_relay == Some(false) {
                    Some(false)
                } else {
                    None
                };
            }
        }
        metrics
    }
}

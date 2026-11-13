use super::{Result, WebRtcError};
use bytes::BytesMut;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::time::timeout;
use webrtc::data_channel::{DataChannel, DataChannelEvent, RTCDataChannelInit};
use webrtc::peer_connection::{
    PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler, RTCConfigurationBuilder,
    RTCIceGatheringState, RTCIceServer, RTCIceTransportPolicy, RTCPeerConnectionState,
    RTCSessionDescription,
};

pub(super) const NEGOTIATE_TIMEOUT: Duration = Duration::from_secs(20);
pub(super) const REALTIME: &str = "game-server.realtime.v1";
pub(super) const SESSION: &str = "game-server.session.v1";
pub(super) const COMMANDS: &str = "game-server.commands.v1";
pub(super) const DATAGRAM_BUDGET: usize = 1200;
pub(super) const RELIABLE_LIMIT: usize = 4096 + 8;

struct Handler {
    gathered: watch::Sender<bool>,
    closed: watch::Sender<bool>,
    channels: mpsc::Sender<Arc<dyn DataChannel>>,
}

#[async_trait::async_trait]
impl PeerConnectionEventHandler for Handler {
    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        if state == RTCIceGatheringState::Complete {
            self.gathered.send_replace(true);
        }
    }
    async fn on_connection_state_change(&self, state: RTCPeerConnectionState) {
        if matches!(
            state,
            RTCPeerConnectionState::Closed | RTCPeerConnectionState::Failed
        ) {
            self.closed.send_replace(true);
        }
    }
    async fn on_data_channel(&self, channel: Arc<dyn DataChannel>) {
        // Never stall the driver on a peer-controlled flood of channels.
        if self.channels.try_send(channel).is_err() {
            self.closed.send_replace(true);
        }
    }
}

pub(super) struct Peer {
    pub(super) pc: Arc<dyn PeerConnection>,
    pub(super) channels: mpsc::Receiver<Arc<dyn DataChannel>>,
    pub(super) closed: watch::Receiver<bool>,
    gathered: watch::Receiver<bool>,
}

impl Peer {
    pub(super) async fn new(
        mut servers: Vec<RTCIceServer>,
        relay_only: bool,
        bind_ip: &str,
    ) -> Result<Self> {
        // coturn's public Binding interface supplies reflexive candidates too.
        // Use only the already configured UDP TURN endpoints, never a new provider.
        let stun: std::collections::BTreeSet<_> = servers
            .iter()
            .flat_map(|server| &server.urls)
            .filter_map(|url| {
                url.strip_prefix("turn:")
                    .filter(|url| !url.ends_with("transport=tcp"))
            })
            .map(|url| format!("stun:{}", url.split('?').next().unwrap_or(url)))
            .collect();
        if !stun.is_empty() {
            servers.push(RTCIceServer {
                urls: stun.into_iter().collect(),
                ..Default::default()
            });
        }
        let bind_ip: std::net::IpAddr = bind_ip
            .parse()
            .map_err(|_| WebRtcError("invalid WebRTC bind address"))?;
        let (gathered_tx, gathered) = watch::channel(false);
        let (closed_tx, closed) = watch::channel(false);
        let (channels_tx, channels) = mpsc::channel(3);
        let pc = PeerConnectionBuilder::new()
            .with_configuration(
                RTCConfigurationBuilder::new()
                    .with_ice_servers(servers)
                    .with_ice_transport_policy(if relay_only {
                        RTCIceTransportPolicy::Relay
                    } else {
                        RTCIceTransportPolicy::All
                    })
                    .build(),
            )
            .with_runtime(
                webrtc::runtime::default_runtime()
                    .ok_or(WebRtcError("WebRTC runtime unavailable"))?,
            )
            .with_handler(Arc::new(Handler {
                gathered: gathered_tx,
                closed: closed_tx,
                channels: channels_tx,
            }))
            .with_udp_addrs(vec![std::net::SocketAddr::new(bind_ip, 0)])
            .with_data_channel_send_buffer_limit(64 * 1024)
            // This also makes dropping a canceled negotiation stop its driver.
            .with_dedicated_reactor_pool_size(1)
            .build()
            .await?;
        Ok(Self {
            pc: Arc::new(pc),
            channels,
            closed,
            gathered,
        })
    }

    pub(super) async fn local_description(&mut self, offer: bool) -> Result<RTCSessionDescription> {
        let description = if offer {
            self.pc.create_offer(None).await?
        } else {
            self.pc.create_answer(None).await?
        };
        self.pc.set_local_description(description).await?;
        timeout(NEGOTIATE_TIMEOUT, self.gathered.wait_for(|ready| *ready))
            .await
            .map_err(|_| WebRtcError("ICE gathering timed out"))?
            .map_err(|_| WebRtcError("ICE gathering stopped"))?;
        self.pc
            .local_description()
            .await
            .ok_or(WebRtcError("missing local description"))
    }

    pub(super) async fn create_content_channel(&self) -> Result<Arc<dyn DataChannel>> {
        Ok(self
            .pc
            .create_data_channel(
                "game-server.content.v1",
                Some(RTCDataChannelInit {
                    negotiated: Some(3),
                    ..Default::default()
                }),
            )
            .await?)
    }

    pub(super) async fn create_channels(&self) -> Result<Channels> {
        let realtime = self
            .pc
            .create_data_channel(
                REALTIME,
                Some(RTCDataChannelInit {
                    ordered: false,
                    max_retransmits: Some(0),
                    negotiated: Some(0),
                    ..Default::default()
                }),
            )
            .await?;
        let session = self
            .pc
            .create_data_channel(
                SESSION,
                Some(RTCDataChannelInit {
                    negotiated: Some(1),
                    ..Default::default()
                }),
            )
            .await?;
        let commands = self
            .pc
            .create_data_channel(
                COMMANDS,
                Some(RTCDataChannelInit {
                    negotiated: Some(2),
                    ..Default::default()
                }),
            )
            .await?;
        Ok(Channels {
            realtime,
            session,
            commands,
            content: None,
        })
    }
}

pub(super) struct Channels {
    pub(super) realtime: Arc<dyn DataChannel>,
    pub(super) session: Arc<dyn DataChannel>,
    pub(super) commands: Arc<dyn DataChannel>,
    pub(super) content: Option<Arc<dyn DataChannel>>,
}

pub(super) async fn opened(channel: &Arc<dyn DataChannel>) -> Result<()> {
    loop {
        // Readiness must not consume the receive queue: an immediately sending
        // peer can enqueue its admission before our event subscription opens.
        match channel.ready_state().await? {
            webrtc::data_channel::RTCDataChannelState::Open => return Ok(()),
            webrtc::data_channel::RTCDataChannelState::Closed
            | webrtc::data_channel::RTCDataChannelState::Closing => {
                return Err(WebRtcError("data channel closed before opening"));
            }
            _ => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
}

pub(super) async fn receive(channel: &Arc<dyn DataChannel>, maximum: usize) -> Result<Vec<u8>> {
    loop {
        match channel.poll().await {
            Some(DataChannelEvent::OnMessage(message))
                if !message.is_string && message.data.len() <= maximum =>
            {
                return Ok(message.data.to_vec());
            }
            Some(DataChannelEvent::OnMessage(_)) => {
                return Err(WebRtcError("invalid data channel message"));
            }
            Some(DataChannelEvent::OnClose) | None => {
                return Err(WebRtcError("data channel closed"));
            }
            _ => {}
        }
    }
}

pub(super) async fn send(channel: &Arc<dyn DataChannel>, bytes: &[u8]) -> Result<()> {
    timeout(Duration::from_secs(5), channel.send(BytesMut::from(bytes)))
        .await
        .map_err(|_| WebRtcError("data channel send timed out"))??;
    Ok(())
}

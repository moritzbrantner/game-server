use super::{Result, WebRtcError};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use url::Url;
use webrtc::peer_connection::RTCIceServer;

const SETUP_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_SIGNAL_BYTES: usize = 32 * 1024;

/// Validated endpoint and browser origin. HTTPS is required except on loopback.
#[derive(Clone, Debug)]
pub struct SetupConfig {
    endpoint: Url,
    origin: String,
}

impl SetupConfig {
    pub fn new(endpoint: &str, origin: &str) -> Result<Self> {
        let endpoint = Url::parse(endpoint).map_err(|_| WebRtcError("invalid setup endpoint"))?;
        let local = matches!(
            endpoint.host_str(),
            Some("localhost" | "127.0.0.1" | "[::1]")
        );
        if !(endpoint.scheme() == "https" || endpoint.scheme() == "http" && local)
            || endpoint.path() != "/"
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
        {
            return Err(WebRtcError("setup endpoint must be an HTTPS origin"));
        }
        crate::BrowserOriginAllowlist::new([origin])
            .map_err(|_| WebRtcError("invalid setup origin"))?;
        Ok(Self {
            endpoint,
            origin: origin.to_owned(),
        })
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Participant {
    participant_id: String,
    participant_token: String,
    host_participant_id: String,
    expires_at: u64,
    max_expires_at: u64,
}

/// An authenticated lobby participant. Capabilities never implement Debug or Serialize.
pub struct Lobby {
    config: SetupConfig,
    http: reqwest::Client,
    lobby_id: String,
    participant: Participant,
    pub(super) socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl Lobby {
    /// Register a native host through the same API used by browser lobby creators.
    pub async fn create(config: SetupConfig, max_participants: usize) -> Result<Self> {
        if !(2..=16).contains(&max_participants) {
            return Err(WebRtcError("lobby capacity must be 2..=16"));
        }
        let http = Self::http()?;
        let body = request(
            &http,
            &config,
            "/lobbies",
            json!({"maxParticipants": max_participants}),
            None,
        )
        .await?;
        let lobby_id = body["lobbyId"]
            .as_str()
            .ok_or(WebRtcError("missing lobby id"))?
            .to_owned();
        Self::connect(config, http, lobby_id, body).await
    }

    /// Claim a guest participant; the public lobby id grants no gameplay authority.
    pub async fn join(config: SetupConfig, lobby_id: &str) -> Result<Self> {
        identifier(lobby_id, 12)?;
        let http = Self::http()?;
        let body = request(
            &http,
            &config,
            &format!("/lobbies/{lobby_id}/join"),
            json!({}),
            None,
        )
        .await?;
        Self::connect(config, http, lobby_id.to_owned(), body).await
    }

    fn http() -> Result<reqwest::Client> {
        if rustls::crypto::CryptoProvider::get_default().is_none() {
            // Another caller may install a provider concurrently; preserve it.
            let _ = rustls::crypto::ring::default_provider().install_default();
        }
        reqwest::Client::builder()
            .timeout(SETUP_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| WebRtcError("setup HTTP client unavailable"))
    }

    async fn connect(
        config: SetupConfig,
        http: reqwest::Client,
        lobby_id: String,
        body: Value,
    ) -> Result<Self> {
        identifier(&lobby_id, 12)?;
        let participant: Participant = serde_json::from_value(body)?;
        identifier(&participant.participant_id, 8)?;
        identifier(&participant.host_participant_id, 8)?;
        if participant.participant_token.len() != 64
            || !participant
                .participant_token
                .bytes()
                .all(|c| c.is_ascii_hexdigit())
        {
            return Err(WebRtcError("invalid participant capability"));
        }
        let mut url = config
            .endpoint
            .join(&format!("/lobbies/{lobby_id}/connect"))
            .map_err(|_| WebRtcError("invalid lobby endpoint"))?;
        url.set_scheme(if config.endpoint.scheme() == "https" {
            "wss"
        } else {
            "ws"
        })
        .map_err(|_| WebRtcError("invalid signaling scheme"))?;
        url.query_pairs_mut()
            .append_pair("participantId", &participant.participant_id);
        let mut upgrade = url
            .as_str()
            .into_client_request()
            .map_err(|_| WebRtcError("invalid signaling request"))?;
        upgrade.headers_mut().insert(
            "origin",
            config
                .origin
                .parse()
                .map_err(|_| WebRtcError("invalid setup origin"))?,
        );
        upgrade.headers_mut().insert(
            "sec-websocket-protocol",
            format!(
                "multiplayer-setup-v1, cap.{}",
                participant.participant_token
            )
            .parse()
            .map_err(|_| WebRtcError("invalid participant capability"))?,
        );
        let ws_config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
            .max_message_size(Some(MAX_SIGNAL_BYTES + 1024))
            .max_frame_size(Some(MAX_SIGNAL_BYTES + 1024));
        let (socket, response) = timeout(
            SETUP_TIMEOUT,
            tokio_tungstenite::connect_async_with_config(upgrade, Some(ws_config), false),
        )
        .await
        .map_err(|_| WebRtcError("signaling connection timed out"))?
        .map_err(|_| WebRtcError("signaling connection refused"))?;
        if response
            .headers()
            .get("sec-websocket-protocol")
            .and_then(|v| v.to_str().ok())
            != Some("multiplayer-setup-v1")
        {
            return Err(WebRtcError("unsupported signaling protocol"));
        }
        Ok(Self {
            config,
            http,
            lobby_id,
            participant,
            socket,
        })
    }

    pub fn lobby_id(&self) -> &str {
        &self.lobby_id
    }
    pub fn participant_id(&self) -> &str {
        &self.participant.participant_id
    }
    pub fn host_participant_id(&self) -> &str {
        &self.participant.host_participant_id
    }

    /// Keep the creator's existing lobby alive within its configured lifetime cap.
    pub(super) async fn heartbeat(&mut self) -> Result<()> {
        if now_ms()?.saturating_add(60_000) >= self.participant.expires_at
            && self.participant.expires_at < self.participant.max_expires_at
        {
            self.renew().await?;
        }
        timeout(
            SETUP_TIMEOUT,
            self.socket
                .send(Message::Text("{\"type\":\"ping\"}".into())),
        )
        .await
        .map_err(|_| WebRtcError("host heartbeat timed out"))?
        .map_err(|_| WebRtcError("host signaling heartbeat failed"))
    }

    /// Obtain temporary relay credentials only from the authenticated setup interface.
    pub(super) async fn turn_servers(&self) -> Result<Vec<RTCIceServer>> {
        let body = request(
            &self.http,
            &self.config,
            &format!("/lobbies/{}/turn-credentials", self.lobby_id),
            json!({"participantId":self.participant.participant_id}),
            Some(&self.participant.participant_token),
        )
        .await?;
        let expiry = body["expiresAt"]
            .as_u64()
            .ok_or(WebRtcError("invalid TURN expiry"))?;
        if expiry <= now_ms()? {
            return Err(WebRtcError("expired TURN credentials"));
        }
        let entries = body["iceServers"]
            .as_array()
            .ok_or(WebRtcError("missing TURN servers"))?;
        if entries.is_empty() || entries.len() > 8 {
            return Err(WebRtcError("invalid TURN server count"));
        }
        entries
            .iter()
            .map(|entry| {
                let urls: Vec<String> = serde_json::from_value(entry["urls"].clone())?;
                if urls.is_empty()
                    || urls.len() > 8
                    || urls
                        .iter()
                        .any(|url| !(url.starts_with("turn:") || url.starts_with("turns:")))
                {
                    return Err(WebRtcError("invalid TURN URLs"));
                }
                Ok(RTCIceServer {
                    urls,
                    username: entry["username"]
                        .as_str()
                        .ok_or(WebRtcError("missing TURN username"))?
                        .to_owned(),
                    credential: entry["credential"]
                        .as_str()
                        .ok_or(WebRtcError("missing TURN credential"))?
                        .to_owned(),
                })
            })
            .collect()
    }

    /// Renew only while the existing setup-service lifetime cap permits it.
    pub async fn renew(&mut self) -> Result<()> {
        if self.participant.participant_id != self.participant.host_participant_id {
            return Err(WebRtcError("only the lobby creator may renew"));
        }
        let now = now_ms()?;
        if now >= self.participant.expires_at
            || self.participant.expires_at >= self.participant.max_expires_at
        {
            return Err(WebRtcError("lobby lifetime exhausted"));
        }
        let result = request(
            &self.http,
            &self.config,
            &format!("/lobbies/{}/renew", self.lobby_id),
            json!({"participantId":self.participant.participant_id}),
            Some(&self.participant.participant_token),
        )
        .await?;
        self.participant.expires_at = result["expiresAt"]
            .as_u64()
            .ok_or(WebRtcError("invalid lobby expiry"))?;
        Ok(())
    }

    pub(super) async fn signal(&mut self, to: &str, payload: Value) -> Result<()> {
        identifier(to, 8)?;
        let text = serde_json::to_string(&json!({"type":"signal", "to":to, "payload":payload}))?;
        if text.len() > MAX_SIGNAL_BYTES {
            return Err(WebRtcError("signal exceeds setup limit"));
        }
        timeout(SETUP_TIMEOUT, self.socket.send(Message::Text(text.into())))
            .await
            .map_err(|_| WebRtcError("signal send timed out"))?
            .map_err(|_| WebRtcError("signaling unavailable"))
    }

    pub(super) async fn receive(&mut self) -> Result<Value> {
        loop {
            match self.socket.next().await {
                Some(Ok(Message::Text(text))) => {
                    return serde_json::from_str(text.as_str()).map_err(Into::into);
                }
                Some(Ok(Message::Ping(bytes))) => self
                    .socket
                    .send(Message::Pong(bytes))
                    .await
                    .map_err(|_| WebRtcError("signaling unavailable"))?,
                Some(Ok(Message::Pong(_))) => {}
                _ => return Err(WebRtcError("signaling disconnected")),
            }
        }
    }
}

fn identifier(value: &str, length: usize) -> Result<()> {
    if value.len() == length
        && value
            .bytes()
            .all(|c| b"0123456789ABCDEFGHJKMNPQRSTVWXYZ".contains(&c))
    {
        Ok(())
    } else {
        Err(WebRtcError("invalid setup identifier"))
    }
}

fn now_ms() -> Result<u64> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| WebRtcError("clock before epoch"))?
            .as_millis(),
    )
    .map_err(|_| WebRtcError("clock overflow"))
}

async fn request(
    http: &reqwest::Client,
    config: &SetupConfig,
    path: &str,
    body: Value,
    token: Option<&str>,
) -> Result<Value> {
    let mut request = http
        .post(
            config
                .endpoint
                .join(path)
                .map_err(|_| WebRtcError("invalid setup path"))?,
        )
        .header("Origin", &config.origin)
        .json(&body);
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    let mut response = request
        .send()
        .await
        .map_err(|_| WebRtcError("setup request failed"))?;
    if !response.status().is_success() {
        return Err(WebRtcError("setup request refused"));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| WebRtcError("setup response failed"))?
    {
        if bytes.len() + chunk.len() > 64 * 1024 {
            return Err(WebRtcError("setup response too large"));
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(Into::into)
}

#[cfg(test)]
pub(super) mod test_support {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::task::JoinSet;

    /// Local signaling endpoints stand in for the already authenticated setup
    /// participants. The real adapter/ICE/DataChannels are used by the probe.
    pub async fn lobbies() -> (Lobby, Lobby, JoinSet<()>) {
        let mut tasks = JoinSet::new();
        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = http.local_addr().unwrap();
        let unused_turn = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let turn_address = unused_turn.local_addr().unwrap();
        drop(unused_turn);
        tasks.spawn(async move {
            loop {
                let (mut stream, _) = http.accept().await.unwrap();
                let mut bytes = [0; 4096];
                let _ = stream.read(&mut bytes).await.unwrap();
                let body = json!({"expiresAt":now_ms().unwrap()+60_000,"iceServers":[{"urls":[format!("turn:{turn_address}?transport=tcp")],"username":"test","credential":"test"}]}).to_string();
                let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",body.len(),body);
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });
        let config = SetupConfig::new(&format!("http://{address}"), "http://localhost").unwrap();
        let mut clients = Vec::new();
        let mut servers = Vec::new();
        for _ in 0..2 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let accept = async {
                let (stream, _) = listener.accept().await.unwrap();
                tokio_tungstenite::accept_async(stream).await.unwrap()
            };
            let (client, server) = tokio::join!(
                tokio_tungstenite::connect_async(format!("ws://{address}")),
                accept
            );
            clients.push(client.unwrap().0);
            servers.push(server);
        }
        let mut guest_socket = servers.pop().unwrap();
        let mut host_socket = servers.pop().unwrap();
        tasks.spawn(async move {
            loop {
                tokio::select! {
                    Some(Ok(Message::Text(text))) = host_socket.next() => {
                        let event: Value = serde_json::from_str(&text).unwrap();
                        if event["type"] == "signal" {
                            guest_socket.send(Message::Text(json!({"type":"signal","from":"00000001","payload":event["payload"]}).to_string().into())).await.unwrap();
                        }
                    },
                    Some(Ok(Message::Text(text))) = guest_socket.next() => {
                        let event: Value = serde_json::from_str(&text).unwrap();
                        if event["type"] == "signal" {
                            host_socket.send(Message::Text(json!({"type":"signal","from":"00000002","payload":event["payload"]}).to_string().into())).await.unwrap();
                        }
                    },
                    else => return,
                }
            }
        });
        let mut lobby = |id: &str| Lobby {
            config: config.clone(),
            http: Lobby::http().unwrap(),
            lobby_id: "000000000001".to_owned(),
            participant: Participant {
                participant_id: id.to_owned(),
                participant_token: "0".repeat(64),
                host_participant_id: "00000001".to_owned(),
                expires_at: u64::MAX,
                max_expires_at: u64::MAX,
            },
            socket: clients.remove(0),
        };
        (lobby("00000001"), lobby("00000002"), tasks)
    }
}

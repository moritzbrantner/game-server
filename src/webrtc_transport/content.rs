use super::{WebRtcError, peer};
use crate::{ControlContext, ControlServiceError, MatchId};
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    Arc, Weak,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use webrtc::{data_channel::DataChannel, peer_connection::PeerConnection};

/// Maximum opaque request or response payload. Games own manifests and chunk offsets.
pub const MAX_CONTENT_BYTES: usize = 16 * 1024;
pub type WebRtcContentFuture<'a> =
    Pin<Box<dyn Future<Output = std::result::Result<Vec<u8>, ControlServiceError>> + Send + 'a>>;

/// Optional game-owned content lookup. Invoked outside the authoritative runtime lock.
/// Implementations must use asynchronous I/O and make cancellation release external work;
/// do not spawn detached or unbounded blocking jobs. Recheck game-owned entitlements
/// before protected side effects, just as for delayed control operations.
pub trait WebRtcContentService: Send + Sync + 'static {
    fn handle<'a>(
        &'a self,
        participant_id: &'a str,
        match_id: &'a MatchId,
        context: ControlContext,
        request: &'a [u8],
    ) -> WebRtcContentFuture<'a>;
}

/// Game refusal is separate from a failed transport and leaves gameplay connected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WebRtcContentError {
    Refused,
    Transport(WebRtcError),
}
impl From<WebRtcError> for WebRtcContentError {
    fn from(error: WebRtcError) -> Self {
        Self::Transport(error)
    }
}
impl std::fmt::Display for WebRtcContentError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused => formatter.write_str("content request refused"),
            Self::Transport(error) => error.fmt(formatter),
        }
    }
}
impl std::error::Error for WebRtcContentError {}

/// Independent, cloneable handle to this admitted session's optional content lane.
/// Only one request may be outstanding across clones: concurrent callers fail busy.
#[derive(Clone)]
pub struct WebRtcContentClient {
    channel: Arc<dyn DataChannel>,
    peer: Weak<dyn PeerConnection>,
    usable: Arc<AtomicBool>,
}
impl WebRtcContentClient {
    pub(super) fn new(channel: Arc<dyn DataChannel>, peer: Arc<dyn PeerConnection>) -> Self {
        Self {
            channel,
            peer: Arc::downgrade(&peer),
            usable: Arc::new(AtomicBool::new(true)),
        }
    }

    /// At most 16 KiB each way, with a five-second exchange bound. A transport failure or
    /// canceled exchange disables this lane; reconnect to obtain a fresh handle.
    /// No later exchange can mistake a delayed response for its own response.
    pub async fn request(
        &self,
        payload: &[u8],
    ) -> std::result::Result<Vec<u8>, WebRtcContentError> {
        if payload.len() > MAX_CONTENT_BYTES {
            return Err(WebRtcError("content request exceeds limit").into());
        }
        let peer = self
            .peer
            .upgrade()
            .ok_or(WebRtcError("content peer closed"))?;
        if !self.usable.swap(false, Ordering::AcqRel) {
            return Err(WebRtcError("content lane busy or canceled").into());
        }
        let result = tokio::time::timeout(Duration::from_secs(5), async {
            peer::opened(&self.channel).await?;
            peer::send(&self.channel, payload).await?;
            let response = peer::receive(&self.channel, MAX_CONTENT_BYTES + 1).await?;
            if response == [0] {
                return Err(WebRtcContentError::Refused);
            }
            if response.first() != Some(&1) {
                return Err(WebRtcError("invalid content response").into());
            }
            Ok(response[1..].to_vec())
        })
        .await
        .unwrap_or(Err(WebRtcError("content response timed out").into()));
        if result.is_ok() || result == Err(WebRtcContentError::Refused) {
            self.usable.store(true, Ordering::Release);
        } else {
            peer.close().await.map_err(WebRtcError::from)?;
        }
        result
    }
}

pub(super) struct ContentBinding {
    pub service: Arc<dyn WebRtcContentService>,
    pub participant: String,
    pub match_id: MatchId,
    pub permits: Arc<tokio::sync::Semaphore>,
}

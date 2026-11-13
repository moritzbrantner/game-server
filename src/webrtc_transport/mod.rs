//! Opt-in native WebRTC transport over the existing setup-service lobby API.
//!
//! Invitations contain only public lobby/match identifiers. Participant capabilities,
//! single-use admission tickets and reconnect tokens remain in memory.

mod client;
mod content;
mod peer;
mod server;
mod setup;

pub use client::{ConnectionMetrics, WebRtcClient};
pub use content::{
    MAX_CONTENT_BYTES, WebRtcContentClient, WebRtcContentError, WebRtcContentFuture,
    WebRtcContentService,
};
pub use server::{WebRtcControlService, WebRtcHost, WebRtcHostConfig};

pub use setup::{Lobby, SetupConfig};

use std::fmt;

/// Boundary failures intentionally omit service responses, SDP and credentials.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WebRtcError(pub(crate) &'static str);

impl fmt::Display for WebRtcError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for WebRtcError {}

impl From<webrtc::error::Error> for WebRtcError {
    fn from(_: webrtc::error::Error) -> Self {
        Self("WebRTC operation failed")
    }
}

impl From<serde_json::Error> for WebRtcError {
    fn from(_: serde_json::Error) -> Self {
        Self("invalid setup message")
    }
}

type Result<T> = std::result::Result<T, WebRtcError>;

/// Server-first admission readiness marker on negotiated session channel 1.
pub const ADMISSION_READY: [u8; 5] = *b"GSAD\x01";

/// Maximum opaque game-owned proof carried with a signaling offer.
pub const MAX_AUTHORIZATION_BYTES: usize = 1024;

#[cfg(test)]
mod managed_tests;

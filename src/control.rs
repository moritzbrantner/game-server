use crate::host::MatchId;
use crate::PlayerId;
use std::error::Error;
use std::fmt;

pub const CONTROL_FORMAT_VERSION: u8 = 1;
pub const MAX_CONTROL_PAYLOAD_BYTES: usize = 4 * 1024;
pub const CONTROL_HEADER_BYTES: usize = 8;
pub const MAX_CONTROL_FRAME_BYTES: usize = CONTROL_HEADER_BYTES + MAX_CONTROL_PAYLOAD_BYTES;

const CONTROL_MAGIC: &[u8; 4] = b"GSCT";
const CONTROL_REQUEST_KIND: u8 = 1;
const CONTROL_RESPONSE_KIND: u8 = 2;
const CONTROL_REJECTED_KIND: u8 = 3;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ControlContext {
    pub player_id: PlayerId,
    pub connection_epoch: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlRequest {
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlResponse {
    pub accepted: bool,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControlWireError {
    InvalidMagic,
    UnsupportedVersion(u8),
    UnexpectedKind(u8),
    Truncated,
    TrailingBytes(usize),
    PayloadTooLarge { maximum: usize, actual: usize },
}

impl fmt::Display for ControlWireError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMagic => write!(formatter, "invalid reliable-control magic"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported reliable-control version {version}")
            }
            Self::UnexpectedKind(kind) => {
                write!(formatter, "unexpected reliable-control kind {kind}")
            }
            Self::Truncated => write!(formatter, "truncated reliable-control frame"),
            Self::TrailingBytes(count) => {
                write!(
                    formatter,
                    "reliable-control frame has {count} trailing bytes"
                )
            }
            Self::PayloadTooLarge { maximum, actual } => write!(
                formatter,
                "reliable-control payload size {actual} exceeds maximum {maximum}"
            ),
        }
    }
}

impl Error for ControlWireError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlServiceError {
    message: String,
}

impl ControlServiceError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for ControlServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for ControlServiceError {}

pub trait ControlService: Send + Sync + 'static {
    fn handle(
        &self,
        context: ControlContext,
        payload: &[u8],
    ) -> Result<Vec<u8>, ControlServiceError>;
}

pub trait MatchControlService: Send + Sync + 'static {
    fn handle(
        &self,
        match_id: &MatchId,
        context: ControlContext,
        payload: &[u8],
    ) -> Result<Vec<u8>, ControlServiceError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RejectControlService;

impl ControlService for RejectControlService {
    fn handle(
        &self,
        _context: ControlContext,
        _payload: &[u8],
    ) -> Result<Vec<u8>, ControlServiceError> {
        Err(ControlServiceError::new("reliable control is disabled"))
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RejectMatchControlService;

impl MatchControlService for RejectMatchControlService {
    fn handle(
        &self,
        _match_id: &MatchId,
        _context: ControlContext,
        _payload: &[u8],
    ) -> Result<Vec<u8>, ControlServiceError> {
        Err(ControlServiceError::new("reliable control is disabled"))
    }
}

pub fn encode_control_request(payload: &[u8]) -> Result<Vec<u8>, ControlWireError> {
    encode_frame(CONTROL_REQUEST_KIND, payload)
}

pub fn decode_control_request(bytes: &[u8]) -> Result<ControlRequest, ControlWireError> {
    let (kind, payload) = decode_frame(bytes)?;
    if kind != CONTROL_REQUEST_KIND {
        return Err(ControlWireError::UnexpectedKind(kind));
    }
    Ok(ControlRequest { payload })
}

pub fn encode_control_response(
    accepted: bool,
    payload: &[u8],
) -> Result<Vec<u8>, ControlWireError> {
    let kind = if accepted {
        CONTROL_RESPONSE_KIND
    } else {
        CONTROL_REJECTED_KIND
    };
    encode_frame(kind, payload)
}

pub fn decode_control_response(bytes: &[u8]) -> Result<ControlResponse, ControlWireError> {
    let (kind, payload) = decode_frame(bytes)?;
    let accepted = match kind {
        CONTROL_RESPONSE_KIND => true,
        CONTROL_REJECTED_KIND => false,
        _ => return Err(ControlWireError::UnexpectedKind(kind)),
    };
    Ok(ControlResponse { accepted, payload })
}

fn encode_frame(kind: u8, payload: &[u8]) -> Result<Vec<u8>, ControlWireError> {
    if payload.len() > MAX_CONTROL_PAYLOAD_BYTES {
        return Err(ControlWireError::PayloadTooLarge {
            maximum: MAX_CONTROL_PAYLOAD_BYTES,
            actual: payload.len(),
        });
    }
    let payload_len = u16::try_from(payload.len()).expect("bounded control payload fits u16");
    let mut bytes = Vec::with_capacity(CONTROL_HEADER_BYTES + payload.len());
    bytes.extend_from_slice(CONTROL_MAGIC);
    bytes.push(CONTROL_FORMAT_VERSION);
    bytes.push(kind);
    bytes.extend_from_slice(&payload_len.to_be_bytes());
    bytes.extend_from_slice(payload);
    Ok(bytes)
}

fn decode_frame(bytes: &[u8]) -> Result<(u8, Vec<u8>), ControlWireError> {
    if bytes.len() < CONTROL_HEADER_BYTES {
        return Err(ControlWireError::Truncated);
    }
    if &bytes[..CONTROL_MAGIC.len()] != CONTROL_MAGIC {
        return Err(ControlWireError::InvalidMagic);
    }
    let version = bytes[4];
    if version != CONTROL_FORMAT_VERSION {
        return Err(ControlWireError::UnsupportedVersion(version));
    }
    let kind = bytes[5];
    let payload_len = usize::from(u16::from_be_bytes([bytes[6], bytes[7]]));
    if payload_len > MAX_CONTROL_PAYLOAD_BYTES {
        return Err(ControlWireError::PayloadTooLarge {
            maximum: MAX_CONTROL_PAYLOAD_BYTES,
            actual: payload_len,
        });
    }
    let expected_len = CONTROL_HEADER_BYTES + payload_len;
    if bytes.len() < expected_len {
        return Err(ControlWireError::Truncated);
    }
    if bytes.len() > expected_len {
        return Err(ControlWireError::TrailingBytes(bytes.len() - expected_len));
    }
    Ok((kind, bytes[CONTROL_HEADER_BYTES..].to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_context_preserves_connection_fencing_identity() {
        let context = ControlContext {
            player_id: 7,
            connection_epoch: 11,
        };
        assert_eq!(context.player_id, 7);
        assert_eq!(context.connection_epoch, 11);
    }

    #[test]
    fn request_and_response_round_trip_exactly() {
        let request = encode_control_request(b"ready").unwrap();
        assert_eq!(
            decode_control_request(&request).unwrap(),
            ControlRequest {
                payload: b"ready".to_vec()
            }
        );

        let response = encode_control_response(true, b"accepted").unwrap();
        assert_eq!(
            decode_control_response(&response).unwrap(),
            ControlResponse {
                accepted: true,
                payload: b"accepted".to_vec()
            }
        );

        let rejected = encode_control_response(false, b"").unwrap();
        assert_eq!(
            decode_control_response(&rejected).unwrap(),
            ControlResponse {
                accepted: false,
                payload: Vec::new()
            }
        );
    }

    #[test]
    fn malformed_and_oversized_frames_fail_closed() {
        let oversized = vec![0_u8; MAX_CONTROL_PAYLOAD_BYTES + 1];
        assert!(matches!(
            encode_control_request(&oversized),
            Err(ControlWireError::PayloadTooLarge { .. })
        ));

        let mut truncated = encode_control_request(b"ready").unwrap();
        truncated.pop();
        assert_eq!(
            decode_control_request(&truncated),
            Err(ControlWireError::Truncated)
        );

        let mut trailing = encode_control_request(b"ready").unwrap();
        trailing.push(0);
        assert_eq!(
            decode_control_request(&trailing),
            Err(ControlWireError::TrailingBytes(1))
        );
    }

    #[test]
    fn request_and_response_kinds_cannot_be_confused() {
        let request = encode_control_request(b"ready").unwrap();
        assert!(matches!(
            decode_control_response(&request),
            Err(ControlWireError::UnexpectedKind(_))
        ));

        let response = encode_control_response(true, b"accepted").unwrap();
        assert!(matches!(
            decode_control_request(&response),
            Err(ControlWireError::UnexpectedKind(_))
        ));
    }
}

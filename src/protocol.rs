use std::fmt;

pub const PROTOCOL_VERSION: u8 = 3;
pub const COMMAND_HEADER_BYTES: usize = 8;
pub const SNAPSHOT_HEADER_BYTES: usize = 20;
pub const RECONNECT_TOKEN_BYTES: usize = 16;
pub const WELCOME_BYTES: usize = 46;
pub const MAX_COMMAND_PAYLOAD_BYTES: usize = 1024;
pub const MAX_SNAPSHOT_PAYLOAD_BYTES: usize = u16::MAX as usize;

const COMMAND_KIND: u8 = 1;
const SNAPSHOT_KIND: u8 = 2;
const WELCOME_KIND: u8 = 3;
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

pub type PlayerId = u32;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandFrame {
    pub sequence: u32,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotFrame {
    pub tick: u64,
    pub state_hash: u64,
    pub payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Welcome {
    pub player_id: PlayerId,
    pub tick_hz: u16,
    pub max_players: u16,
    pub current_tick: u64,
    pub connection_epoch: u32,
    pub reconnect_token: [u8; RECONNECT_TOKEN_BYTES],
    pub reconnect_grace_ticks: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProtocolError {
    IncorrectLength { expected: usize, actual: usize },
    UnsupportedVersion(u8),
    UnexpectedKind(u8),
    InvalidSequence,
    PayloadTooLarge { maximum: usize, actual: usize },
    InvalidStateHash { expected: u64, actual: u64 },
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IncorrectLength { expected, actual } => {
                write!(formatter, "expected {expected} bytes, received {actual}")
            }
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported protocol version {version}")
            }
            Self::UnexpectedKind(kind) => write!(formatter, "unexpected frame kind {kind}"),
            Self::InvalidSequence => write!(formatter, "command sequence must be non-zero"),
            Self::PayloadTooLarge { maximum, actual } => {
                write!(formatter, "payload size {actual} exceeds maximum {maximum}")
            }
            Self::InvalidStateHash { expected, actual } => write!(
                formatter,
                "snapshot hash mismatch: expected {expected:#018x}, got {actual:#018x}"
            ),
        }
    }
}

impl std::error::Error for ProtocolError {}

pub fn encode_command(sequence: u32, payload: &[u8]) -> Result<Vec<u8>, ProtocolError> {
    if sequence == 0 {
        return Err(ProtocolError::InvalidSequence);
    }
    if payload.len() > MAX_COMMAND_PAYLOAD_BYTES {
        return Err(ProtocolError::PayloadTooLarge {
            maximum: MAX_COMMAND_PAYLOAD_BYTES,
            actual: payload.len(),
        });
    }
    let payload_len = u16::try_from(payload.len()).expect("bounded command payload length");
    let mut bytes = Vec::with_capacity(COMMAND_HEADER_BYTES + payload.len());
    bytes.push(PROTOCOL_VERSION);
    bytes.push(COMMAND_KIND);
    bytes.extend_from_slice(&sequence.to_be_bytes());
    bytes.extend_from_slice(&payload_len.to_be_bytes());
    bytes.extend_from_slice(payload);
    Ok(bytes)
}

pub fn decode_command(bytes: &[u8]) -> Result<CommandFrame, ProtocolError> {
    if bytes.len() < COMMAND_HEADER_BYTES {
        return Err(ProtocolError::IncorrectLength {
            expected: COMMAND_HEADER_BYTES,
            actual: bytes.len(),
        });
    }
    require_header(bytes, COMMAND_KIND)?;
    let sequence = u32::from_be_bytes(bytes[2..6].try_into().expect("checked command header"));
    if sequence == 0 {
        return Err(ProtocolError::InvalidSequence);
    }
    let payload_len = usize::from(u16::from_be_bytes(
        bytes[6..8].try_into().expect("checked command header"),
    ));
    if payload_len > MAX_COMMAND_PAYLOAD_BYTES {
        return Err(ProtocolError::PayloadTooLarge {
            maximum: MAX_COMMAND_PAYLOAD_BYTES,
            actual: payload_len,
        });
    }
    require_length(bytes, COMMAND_HEADER_BYTES + payload_len)?;
    Ok(CommandFrame {
        sequence,
        payload: bytes[COMMAND_HEADER_BYTES..].to_vec(),
    })
}

pub fn encode_snapshot(snapshot: &SnapshotFrame) -> Result<Vec<u8>, ProtocolError> {
    if snapshot.payload.len() > MAX_SNAPSHOT_PAYLOAD_BYTES {
        return Err(ProtocolError::PayloadTooLarge {
            maximum: MAX_SNAPSHOT_PAYLOAD_BYTES,
            actual: snapshot.payload.len(),
        });
    }
    let expected_hash = snapshot_hash(snapshot.tick, &snapshot.payload);
    if snapshot.state_hash != expected_hash {
        return Err(ProtocolError::InvalidStateHash {
            expected: expected_hash,
            actual: snapshot.state_hash,
        });
    }
    let payload_len =
        u16::try_from(snapshot.payload.len()).expect("bounded snapshot payload length");
    let mut bytes = Vec::with_capacity(SNAPSHOT_HEADER_BYTES + snapshot.payload.len());
    bytes.push(PROTOCOL_VERSION);
    bytes.push(SNAPSHOT_KIND);
    bytes.extend_from_slice(&snapshot.tick.to_be_bytes());
    bytes.extend_from_slice(&snapshot.state_hash.to_be_bytes());
    bytes.extend_from_slice(&payload_len.to_be_bytes());
    bytes.extend_from_slice(&snapshot.payload);
    Ok(bytes)
}

pub fn decode_snapshot(bytes: &[u8]) -> Result<SnapshotFrame, ProtocolError> {
    if bytes.len() < SNAPSHOT_HEADER_BYTES {
        return Err(ProtocolError::IncorrectLength {
            expected: SNAPSHOT_HEADER_BYTES,
            actual: bytes.len(),
        });
    }
    require_header(bytes, SNAPSHOT_KIND)?;
    let tick = u64::from_be_bytes(bytes[2..10].try_into().expect("checked snapshot header"));
    let state_hash = u64::from_be_bytes(bytes[10..18].try_into().expect("checked snapshot header"));
    let payload_len = usize::from(u16::from_be_bytes(
        bytes[18..20].try_into().expect("checked snapshot header"),
    ));
    require_length(bytes, SNAPSHOT_HEADER_BYTES + payload_len)?;
    let payload = bytes[SNAPSHOT_HEADER_BYTES..].to_vec();
    let expected_hash = snapshot_hash(tick, &payload);
    if expected_hash != state_hash {
        return Err(ProtocolError::InvalidStateHash {
            expected: expected_hash,
            actual: state_hash,
        });
    }
    Ok(SnapshotFrame {
        tick,
        state_hash,
        payload,
    })
}

pub fn encode_welcome(welcome: Welcome) -> [u8; WELCOME_BYTES] {
    let mut bytes = [0_u8; WELCOME_BYTES];
    bytes[0] = PROTOCOL_VERSION;
    bytes[1] = WELCOME_KIND;
    bytes[2..6].copy_from_slice(&welcome.player_id.to_be_bytes());
    bytes[6..8].copy_from_slice(&welcome.tick_hz.to_be_bytes());
    bytes[8..10].copy_from_slice(&welcome.max_players.to_be_bytes());
    bytes[10..18].copy_from_slice(&welcome.current_tick.to_be_bytes());
    bytes[18..22].copy_from_slice(&welcome.connection_epoch.to_be_bytes());
    bytes[22..38].copy_from_slice(&welcome.reconnect_token);
    bytes[38..46].copy_from_slice(&welcome.reconnect_grace_ticks.to_be_bytes());
    bytes
}

pub fn decode_welcome(bytes: &[u8]) -> Result<Welcome, ProtocolError> {
    require_length(bytes, WELCOME_BYTES)?;
    require_header(bytes, WELCOME_KIND)?;
    Ok(Welcome {
        player_id: u32::from_be_bytes(bytes[2..6].try_into().expect("checked welcome length")),
        tick_hz: u16::from_be_bytes(bytes[6..8].try_into().expect("checked welcome length")),
        max_players: u16::from_be_bytes(bytes[8..10].try_into().expect("checked welcome length")),
        current_tick: u64::from_be_bytes(bytes[10..18].try_into().expect("checked welcome length")),
        connection_epoch: u32::from_be_bytes(
            bytes[18..22].try_into().expect("checked welcome length"),
        ),
        reconnect_token: bytes[22..38].try_into().expect("checked welcome length"),
        reconnect_grace_ticks: u64::from_be_bytes(
            bytes[38..46].try_into().expect("checked welcome length"),
        ),
    })
}

pub fn snapshot_hash(tick: u64, payload: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET_BASIS;
    hash = fnv_update(hash, &tick.to_be_bytes());
    hash = fnv_update(hash, &(payload.len() as u64).to_be_bytes());
    fnv_update(hash, payload)
}

fn require_header(bytes: &[u8], expected_kind: u8) -> Result<(), ProtocolError> {
    if bytes[0] != PROTOCOL_VERSION {
        return Err(ProtocolError::UnsupportedVersion(bytes[0]));
    }
    if bytes[1] != expected_kind {
        return Err(ProtocolError::UnexpectedKind(bytes[1]));
    }
    Ok(())
}

fn require_length(bytes: &[u8], expected: usize) -> Result<(), ProtocolError> {
    if bytes.len() != expected {
        return Err(ProtocolError::IncorrectLength {
            expected,
            actual: bytes.len(),
        });
    }
    Ok(())
}

fn fnv_update(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_round_trip_is_exact() {
        let encoded = encode_command(7, b"game command").unwrap();
        assert_eq!(
            decode_command(&encoded).unwrap(),
            CommandFrame {
                sequence: 7,
                payload: b"game command".to_vec()
            }
        );
    }

    #[test]
    fn malformed_command_is_rejected() {
        assert!(decode_command(&[PROTOCOL_VERSION, COMMAND_KIND]).is_err());
        assert!(encode_command(0, b"invalid").is_err());
    }

    #[test]
    fn snapshot_round_trip_verifies_hash() {
        let payload = b"opaque game state".to_vec();
        let snapshot = SnapshotFrame {
            tick: 9,
            state_hash: snapshot_hash(9, &payload),
            payload,
        };
        assert_eq!(
            decode_snapshot(&encode_snapshot(&snapshot).unwrap()).unwrap(),
            snapshot
        );
    }

    #[test]
    fn welcome_carries_reconnect_identity() {
        let welcome = Welcome {
            player_id: 7,
            tick_hz: 20,
            max_players: 16,
            current_tick: 99,
            connection_epoch: 3,
            reconnect_token: [0xab; RECONNECT_TOKEN_BYTES],
            reconnect_grace_ticks: 600,
        };
        assert_eq!(decode_welcome(&encode_welcome(welcome)).unwrap(), welcome);
    }
}

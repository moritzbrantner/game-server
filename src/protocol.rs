use std::fmt;

pub const PROTOCOL_VERSION: u8 = 1;
pub const INPUT_BYTES: usize = 8;
pub const SNAPSHOT_HEADER_BYTES: usize = 19;
pub const SNAPSHOT_PLAYER_BYTES: usize = 12;
pub const WELCOME_BYTES: usize = 18;
pub const MAX_PLAYERS: usize = 16;

const INPUT_KIND: u8 = 1;
const SNAPSHOT_KIND: u8 = 2;
const WELCOME_KIND: u8 = 3;
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

pub type PlayerId = u32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InputCommand {
    pub sequence: u32,
    pub horizontal: i8,
    pub vertical: i8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotPlayer {
    pub player_id: PlayerId,
    pub x: i16,
    pub y: i16,
    pub last_applied_sequence: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub tick: u64,
    pub state_hash: u64,
    pub players: Vec<SnapshotPlayer>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Welcome {
    pub player_id: PlayerId,
    pub tick_hz: u16,
    pub max_players: u8,
    pub current_tick: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProtocolError {
    IncorrectLength { expected: usize, actual: usize },
    UnsupportedVersion(u8),
    UnexpectedKind(u8),
    InvalidSequence,
    InvalidAxis { horizontal: i8, vertical: i8 },
    InvalidPlayerCount(usize),
    InvalidStateHash { expected: u64, actual: u64 },
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IncorrectLength { expected, actual } => {
                write!(formatter, "expected {expected} bytes, received {actual}")
            }
            Self::UnsupportedVersion(version) => write!(formatter, "unsupported protocol version {version}"),
            Self::UnexpectedKind(kind) => write!(formatter, "unexpected frame kind {kind}"),
            Self::InvalidSequence => write!(formatter, "input sequence must be non-zero"),
            Self::InvalidAxis { horizontal, vertical } => write!(
                formatter,
                "input axes must each be between -1 and 1, got ({horizontal}, {vertical})"
            ),
            Self::InvalidPlayerCount(count) => write!(formatter, "invalid player count {count}"),
            Self::InvalidStateHash { expected, actual } => write!(
                formatter,
                "snapshot hash mismatch: expected {expected:#018x}, got {actual:#018x}"
            ),
        }
    }
}

impl std::error::Error for ProtocolError {}

pub fn validate_input(input: InputCommand) -> Result<(), ProtocolError> {
    if input.sequence == 0 {
        return Err(ProtocolError::InvalidSequence);
    }
    if !(-1..=1).contains(&input.horizontal) || !(-1..=1).contains(&input.vertical) {
        return Err(ProtocolError::InvalidAxis {
            horizontal: input.horizontal,
            vertical: input.vertical,
        });
    }
    Ok(())
}

pub fn encode_input(input: InputCommand) -> Result<[u8; INPUT_BYTES], ProtocolError> {
    validate_input(input)?;
    let mut bytes = [0_u8; INPUT_BYTES];
    bytes[0] = PROTOCOL_VERSION;
    bytes[1] = INPUT_KIND;
    bytes[2..6].copy_from_slice(&input.sequence.to_be_bytes());
    bytes[6] = input.horizontal as u8;
    bytes[7] = input.vertical as u8;
    Ok(bytes)
}

pub fn decode_input(bytes: &[u8]) -> Result<InputCommand, ProtocolError> {
    require_length(bytes, INPUT_BYTES)?;
    require_header(bytes, INPUT_KIND)?;
    let input = InputCommand {
        sequence: u32::from_be_bytes(bytes[2..6].try_into().expect("checked input length")),
        horizontal: bytes[6] as i8,
        vertical: bytes[7] as i8,
    };
    validate_input(input)?;
    Ok(input)
}

pub fn encode_snapshot(snapshot: &Snapshot) -> Result<Vec<u8>, ProtocolError> {
    if snapshot.players.len() > MAX_PLAYERS {
        return Err(ProtocolError::InvalidPlayerCount(snapshot.players.len()));
    }
    let expected_hash = snapshot_hash(snapshot.tick, &snapshot.players);
    if snapshot.state_hash != expected_hash {
        return Err(ProtocolError::InvalidStateHash {
            expected: expected_hash,
            actual: snapshot.state_hash,
        });
    }

    let mut bytes = Vec::with_capacity(
        SNAPSHOT_HEADER_BYTES + snapshot.players.len() * SNAPSHOT_PLAYER_BYTES,
    );
    bytes.push(PROTOCOL_VERSION);
    bytes.push(SNAPSHOT_KIND);
    bytes.extend_from_slice(&snapshot.tick.to_be_bytes());
    bytes.extend_from_slice(&snapshot.state_hash.to_be_bytes());
    bytes.push(snapshot.players.len() as u8);
    for player in &snapshot.players {
        bytes.extend_from_slice(&player.player_id.to_be_bytes());
        bytes.extend_from_slice(&player.x.to_be_bytes());
        bytes.extend_from_slice(&player.y.to_be_bytes());
        bytes.extend_from_slice(&player.last_applied_sequence.to_be_bytes());
    }
    Ok(bytes)
}

pub fn decode_snapshot(bytes: &[u8]) -> Result<Snapshot, ProtocolError> {
    if bytes.len() < SNAPSHOT_HEADER_BYTES {
        return Err(ProtocolError::IncorrectLength {
            expected: SNAPSHOT_HEADER_BYTES,
            actual: bytes.len(),
        });
    }
    require_header(bytes, SNAPSHOT_KIND)?;
    let player_count = usize::from(bytes[18]);
    if player_count > MAX_PLAYERS {
        return Err(ProtocolError::InvalidPlayerCount(player_count));
    }
    require_length(
        bytes,
        SNAPSHOT_HEADER_BYTES + player_count * SNAPSHOT_PLAYER_BYTES,
    )?;

    let tick = u64::from_be_bytes(bytes[2..10].try_into().expect("checked header length"));
    let state_hash = u64::from_be_bytes(bytes[10..18].try_into().expect("checked header length"));
    let mut players = Vec::with_capacity(player_count);
    for index in 0..player_count {
        let offset = SNAPSHOT_HEADER_BYTES + index * SNAPSHOT_PLAYER_BYTES;
        players.push(SnapshotPlayer {
            player_id: u32::from_be_bytes(
                bytes[offset..offset + 4]
                    .try_into()
                    .expect("checked player length"),
            ),
            x: i16::from_be_bytes(
                bytes[offset + 4..offset + 6]
                    .try_into()
                    .expect("checked player length"),
            ),
            y: i16::from_be_bytes(
                bytes[offset + 6..offset + 8]
                    .try_into()
                    .expect("checked player length"),
            ),
            last_applied_sequence: u32::from_be_bytes(
                bytes[offset + 8..offset + 12]
                    .try_into()
                    .expect("checked player length"),
            ),
        });
    }
    let expected_hash = snapshot_hash(tick, &players);
    if expected_hash != state_hash {
        return Err(ProtocolError::InvalidStateHash {
            expected: expected_hash,
            actual: state_hash,
        });
    }
    Ok(Snapshot {
        tick,
        state_hash,
        players,
    })
}

pub fn encode_welcome(welcome: Welcome) -> [u8; WELCOME_BYTES] {
    let mut bytes = [0_u8; WELCOME_BYTES];
    bytes[0] = PROTOCOL_VERSION;
    bytes[1] = WELCOME_KIND;
    bytes[2..6].copy_from_slice(&welcome.player_id.to_be_bytes());
    bytes[6..8].copy_from_slice(&welcome.tick_hz.to_be_bytes());
    bytes[8] = welcome.max_players;
    bytes[10..18].copy_from_slice(&welcome.current_tick.to_be_bytes());
    bytes
}

pub fn decode_welcome(bytes: &[u8]) -> Result<Welcome, ProtocolError> {
    require_length(bytes, WELCOME_BYTES)?;
    require_header(bytes, WELCOME_KIND)?;
    Ok(Welcome {
        player_id: u32::from_be_bytes(bytes[2..6].try_into().expect("checked welcome length")),
        tick_hz: u16::from_be_bytes(bytes[6..8].try_into().expect("checked welcome length")),
        max_players: bytes[8],
        current_tick: u64::from_be_bytes(bytes[10..18].try_into().expect("checked welcome length")),
    })
}

pub fn snapshot_hash(tick: u64, players: &[SnapshotPlayer]) -> u64 {
    let mut hash = FNV_OFFSET_BASIS;
    hash = fnv_update(hash, &tick.to_be_bytes());
    hash = fnv_update(hash, &[players.len() as u8]);
    for player in players {
        hash = fnv_update(hash, &player.player_id.to_be_bytes());
        hash = fnv_update(hash, &player.x.to_be_bytes());
        hash = fnv_update(hash, &player.y.to_be_bytes());
        hash = fnv_update(hash, &player.last_applied_sequence.to_be_bytes());
    }
    hash
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
    fn input_round_trip_is_exact() {
        let input = InputCommand {
            sequence: 7,
            horizontal: -1,
            vertical: 1,
        };
        assert_eq!(decode_input(&encode_input(input).unwrap()).unwrap(), input);
    }

    #[test]
    fn malformed_input_is_rejected() {
        assert!(decode_input(&[PROTOCOL_VERSION, INPUT_KIND]).is_err());
        assert!(encode_input(InputCommand { sequence: 1, horizontal: 2, vertical: 0 }).is_err());
    }

    #[test]
    fn snapshot_round_trip_verifies_hash() {
        let players = vec![SnapshotPlayer {
            player_id: 1,
            x: 10,
            y: -10,
            last_applied_sequence: 4,
        }];
        let snapshot = Snapshot {
            tick: 9,
            state_hash: snapshot_hash(9, &players),
            players,
        };
        assert_eq!(decode_snapshot(&encode_snapshot(&snapshot).unwrap()).unwrap(), snapshot);
    }
}

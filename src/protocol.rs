use std::fmt;

pub const PROTOCOL_VERSION: u8 = 3;
pub const COMMAND_HEADER_BYTES: usize = 8;
pub const SNAPSHOT_HEADER_BYTES: usize = 20;
pub const RECONNECT_TOKEN_BYTES: usize = 16;
pub const WELCOME_BYTES: usize = 46;
pub const MAX_COMMAND_PAYLOAD_BYTES: usize = 1024;
pub const MAX_SNAPSHOT_PAYLOAD_BYTES: usize = u16::MAX as usize;
/// Largest encoded snapshot frame: the snapshot header plus the maximum payload.
pub const MAX_SNAPSHOT_FRAME_BYTES: usize = SNAPSHOT_HEADER_BYTES + MAX_SNAPSHOT_PAYLOAD_BYTES;
pub const SNAPSHOT_FRAGMENT_HEADER_BYTES: usize = 14;
/// Hard upper bound on the fragments that carry one snapshot frame.
///
/// Any datagram budget of at least [`MIN_FRAGMENTED_DATAGRAM_BYTES`] can carry
/// the largest legal snapshot frame within this bound.
pub const MAX_SNAPSHOT_FRAGMENTS: usize = 64;
/// Smallest datagram budget that can carry every legal snapshot frame.
pub const MIN_FRAGMENTED_DATAGRAM_BYTES: usize =
    SNAPSHOT_FRAGMENT_HEADER_BYTES + MAX_SNAPSHOT_FRAME_BYTES.div_ceil(MAX_SNAPSHOT_FRAGMENTS);

const COMMAND_KIND: u8 = 1;
const SNAPSHOT_KIND: u8 = 2;
const WELCOME_KIND: u8 = 3;
pub(crate) const SNAPSHOT_FRAGMENT_KIND: u8 = 4;
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

/// One piece of an encoded snapshot frame that exceeded the datagram budget.
///
/// Concatenating the chunks of fragments `0..count` for one tick yields the
/// exact bytes of a normal encoded snapshot frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotFragment<'a> {
    pub tick: u64,
    pub index: u8,
    pub count: u8,
    pub chunk: &'a [u8],
}

/// A server-to-client realtime datagram: a whole snapshot or one fragment of one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnapshotDatagram<'a> {
    Snapshot(SnapshotFrame),
    Fragment(SnapshotFragment<'a>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProtocolError {
    IncorrectLength { expected: usize, actual: usize },
    UnsupportedVersion(u8),
    UnexpectedKind(u8),
    InvalidSequence,
    PayloadTooLarge { maximum: usize, actual: usize },
    InvalidStateHash { expected: u64, actual: u64 },
    DatagramBudgetTooSmall { minimum: usize, actual: usize },
    TooManyFragments { maximum: usize, required: usize },
    InvalidFragmentCount(u8),
    InvalidFragmentIndex { index: u8, count: u8 },
    EmptyFragment,
    InconsistentFragment { tick: u64 },
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
            Self::DatagramBudgetTooSmall { minimum, actual } => write!(
                formatter,
                "datagram budget {actual} is below the fragment minimum {minimum}"
            ),
            Self::TooManyFragments { maximum, required } => write!(
                formatter,
                "snapshot requires {required} fragments, exceeding maximum {maximum}"
            ),
            Self::InvalidFragmentCount(count) => write!(
                formatter,
                "snapshot fragment count {count} is outside 1..={MAX_SNAPSHOT_FRAGMENTS}"
            ),
            Self::InvalidFragmentIndex { index, count } => write!(
                formatter,
                "snapshot fragment index {index} is outside fragment count {count}"
            ),
            Self::EmptyFragment => write!(formatter, "snapshot fragment carries no bytes"),
            Self::InconsistentFragment { tick } => write!(
                formatter,
                "snapshot fragments for tick {tick} are inconsistent"
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
    let (tick, state_hash) = verify_snapshot_frame(bytes)?;
    Ok(SnapshotFrame {
        tick,
        state_hash,
        payload: bytes[SNAPSHOT_HEADER_BYTES..].to_vec(),
    })
}

/// Decodes an owned snapshot frame, reusing its allocation for the payload.
pub(crate) fn decode_snapshot_owned(mut bytes: Vec<u8>) -> Result<SnapshotFrame, ProtocolError> {
    let (tick, state_hash) = verify_snapshot_frame(&bytes)?;
    bytes.drain(..SNAPSHOT_HEADER_BYTES);
    Ok(SnapshotFrame {
        tick,
        state_hash,
        payload: bytes,
    })
}

fn verify_snapshot_frame(bytes: &[u8]) -> Result<(u64, u64), ProtocolError> {
    let tick = snapshot_frame_tick(bytes)?;
    let state_hash = u64::from_be_bytes(bytes[10..18].try_into().expect("checked snapshot header"));
    let expected_hash = snapshot_hash(tick, &bytes[SNAPSHOT_HEADER_BYTES..]);
    if expected_hash != state_hash {
        return Err(ProtocolError::InvalidStateHash {
            expected: expected_hash,
            actual: state_hash,
        });
    }
    Ok((tick, state_hash))
}

/// Checks the snapshot frame header and exact length, without the hash, and returns its tick.
pub(crate) fn snapshot_frame_tick(bytes: &[u8]) -> Result<u64, ProtocolError> {
    if bytes.len() < SNAPSHOT_HEADER_BYTES {
        return Err(ProtocolError::IncorrectLength {
            expected: SNAPSHOT_HEADER_BYTES,
            actual: bytes.len(),
        });
    }
    require_header(bytes, SNAPSHOT_KIND)?;
    let tick = u64::from_be_bytes(bytes[2..10].try_into().expect("checked snapshot header"));
    let payload_len = usize::from(u16::from_be_bytes(
        bytes[18..20].try_into().expect("checked snapshot header"),
    ));
    require_length(bytes, SNAPSHOT_HEADER_BYTES + payload_len)?;
    Ok(tick)
}

/// Splits one encoded snapshot frame into fragment datagrams of at most
/// `max_datagram_size` bytes each.
///
/// Callers send a frame that already fits the budget unchanged; this function
/// is for frames that do not. Every fragment except the last carries a full
/// chunk. The snapshot hash is not recomputed here: receivers verify it after
/// reassembly through [`decode_snapshot`].
pub fn encode_snapshot_fragments(
    frame: &[u8],
    max_datagram_size: usize,
) -> Result<Vec<Vec<u8>>, ProtocolError> {
    let tick = snapshot_frame_tick(frame)?;
    let chunk_capacity = max_datagram_size
        .checked_sub(SNAPSHOT_FRAGMENT_HEADER_BYTES)
        .filter(|capacity| *capacity > 0)
        .ok_or(ProtocolError::DatagramBudgetTooSmall {
            minimum: SNAPSHOT_FRAGMENT_HEADER_BYTES + 1,
            actual: max_datagram_size,
        })?
        .min(usize::from(u16::MAX));
    let required = frame.len().div_ceil(chunk_capacity);
    if required > MAX_SNAPSHOT_FRAGMENTS {
        return Err(ProtocolError::TooManyFragments {
            maximum: MAX_SNAPSHOT_FRAGMENTS,
            required,
        });
    }
    let count = u8::try_from(required).expect("bounded snapshot fragment count");
    Ok(frame
        .chunks(chunk_capacity)
        .enumerate()
        .map(|(index, chunk)| {
            let index = u8::try_from(index).expect("bounded snapshot fragment index");
            let chunk_len = u16::try_from(chunk.len()).expect("bounded snapshot fragment chunk");
            let mut bytes = Vec::with_capacity(SNAPSHOT_FRAGMENT_HEADER_BYTES + chunk.len());
            bytes.push(PROTOCOL_VERSION);
            bytes.push(SNAPSHOT_FRAGMENT_KIND);
            bytes.extend_from_slice(&tick.to_be_bytes());
            bytes.push(index);
            bytes.push(count);
            bytes.extend_from_slice(&chunk_len.to_be_bytes());
            bytes.extend_from_slice(chunk);
            bytes
        })
        .collect())
}

/// Decodes the header of one snapshot fragment datagram and borrows its chunk.
pub fn decode_snapshot_fragment(bytes: &[u8]) -> Result<SnapshotFragment<'_>, ProtocolError> {
    if bytes.len() < SNAPSHOT_FRAGMENT_HEADER_BYTES {
        return Err(ProtocolError::IncorrectLength {
            expected: SNAPSHOT_FRAGMENT_HEADER_BYTES,
            actual: bytes.len(),
        });
    }
    require_header(bytes, SNAPSHOT_FRAGMENT_KIND)?;
    let tick = u64::from_be_bytes(bytes[2..10].try_into().expect("checked fragment header"));
    let index = bytes[10];
    let count = bytes[11];
    if count == 0 || usize::from(count) > MAX_SNAPSHOT_FRAGMENTS {
        return Err(ProtocolError::InvalidFragmentCount(count));
    }
    if index >= count {
        return Err(ProtocolError::InvalidFragmentIndex { index, count });
    }
    let chunk_len = usize::from(u16::from_be_bytes(
        bytes[12..14].try_into().expect("checked fragment header"),
    ));
    if chunk_len == 0 {
        return Err(ProtocolError::EmptyFragment);
    }
    require_length(bytes, SNAPSHOT_FRAGMENT_HEADER_BYTES + chunk_len)?;
    Ok(SnapshotFragment {
        tick,
        index,
        count,
        chunk: &bytes[SNAPSHOT_FRAGMENT_HEADER_BYTES..],
    })
}

/// Classifies and decodes one server-to-client realtime datagram.
///
/// Whole snapshots are fully verified, including their state hash. Fragments
/// are header-checked only; use [`crate::SnapshotReassembler`] to combine and
/// verify them.
pub fn decode_snapshot_datagram(bytes: &[u8]) -> Result<SnapshotDatagram<'_>, ProtocolError> {
    if bytes.get(1) == Some(&SNAPSHOT_FRAGMENT_KIND) {
        decode_snapshot_fragment(bytes).map(SnapshotDatagram::Fragment)
    } else {
        decode_snapshot(bytes).map(SnapshotDatagram::Snapshot)
    }
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

    fn encoded_snapshot(tick: u64, payload_len: usize) -> Vec<u8> {
        let payload: Vec<u8> = (0..payload_len).map(|index| (index % 251) as u8).collect();
        encode_snapshot(&SnapshotFrame {
            tick,
            state_hash: snapshot_hash(tick, &payload),
            payload,
        })
        .unwrap()
    }

    fn raw_fragment(tick: u64, index: u8, count: u8, chunk: &[u8]) -> Vec<u8> {
        let mut bytes = vec![PROTOCOL_VERSION, SNAPSHOT_FRAGMENT_KIND];
        bytes.extend_from_slice(&tick.to_be_bytes());
        bytes.push(index);
        bytes.push(count);
        bytes.extend_from_slice(&u16::try_from(chunk.len()).unwrap().to_be_bytes());
        bytes.extend_from_slice(chunk);
        bytes
    }

    #[test]
    fn snapshot_fragment_wire_layout_is_exact() {
        let frame = encoded_snapshot(0x0102_0304_0506_0708, 4);
        let fragments =
            encode_snapshot_fragments(&frame, SNAPSHOT_FRAGMENT_HEADER_BYTES + 16).unwrap();

        assert_eq!(fragments.len(), 2);
        assert_eq!(
            fragments[0][..SNAPSHOT_FRAGMENT_HEADER_BYTES],
            [3, 4, 1, 2, 3, 4, 5, 6, 7, 8, 0, 2, 0, 16]
        );
        assert_eq!(fragments[0][SNAPSHOT_FRAGMENT_HEADER_BYTES..], frame[..16]);
        assert_eq!(
            fragments[1][..SNAPSHOT_FRAGMENT_HEADER_BYTES],
            [3, 4, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 0, 8]
        );
        assert_eq!(fragments[1][SNAPSHOT_FRAGMENT_HEADER_BYTES..], frame[16..]);
    }

    #[test]
    fn snapshot_fragments_fit_the_budget_and_concatenate_to_the_frame() {
        let frame = encoded_snapshot(42, 5_000);
        let budget = 1_200;
        let fragments = encode_snapshot_fragments(&frame, budget).unwrap();

        assert_eq!(
            fragments.len(),
            frame
                .len()
                .div_ceil(budget - SNAPSHOT_FRAGMENT_HEADER_BYTES)
        );
        let mut reassembled = Vec::new();
        for (index, datagram) in fragments.iter().enumerate() {
            assert!(datagram.len() <= budget);
            let fragment = decode_snapshot_fragment(datagram).unwrap();
            assert_eq!(fragment.tick, 42);
            assert_eq!(usize::from(fragment.index), index);
            assert_eq!(usize::from(fragment.count), fragments.len());
            if index + 1 < fragments.len() {
                assert_eq!(
                    datagram.len(),
                    budget,
                    "only the last fragment may be short"
                );
            }
            reassembled.extend_from_slice(fragment.chunk);
        }
        assert_eq!(reassembled, frame);
        assert_eq!(
            decode_snapshot(&reassembled).unwrap(),
            decode_snapshot(&frame).unwrap()
        );
    }

    #[test]
    fn snapshot_fragment_count_follows_exact_chunk_boundaries() {
        let budget = 100;
        let capacity = budget - SNAPSHOT_FRAGMENT_HEADER_BYTES;
        for (frame_len, expected) in [
            (SNAPSHOT_HEADER_BYTES, 1),
            (capacity, 1),
            (capacity + 1, 2),
            (2 * capacity, 2),
            (2 * capacity + 1, 3),
            (MAX_SNAPSHOT_FRAGMENTS * capacity, MAX_SNAPSHOT_FRAGMENTS),
        ] {
            let frame = encoded_snapshot(1, frame_len - SNAPSHOT_HEADER_BYTES);
            let fragments = encode_snapshot_fragments(&frame, budget).unwrap();
            assert_eq!(fragments.len(), expected, "frame of {frame_len} bytes");
            assert_eq!(
                fragments.concat().len(),
                frame_len + expected * SNAPSHOT_FRAGMENT_HEADER_BYTES
            );
        }

        let frame = encoded_snapshot(
            1,
            MAX_SNAPSHOT_FRAGMENTS * capacity + 1 - SNAPSHOT_HEADER_BYTES,
        );
        assert_eq!(
            encode_snapshot_fragments(&frame, budget),
            Err(ProtocolError::TooManyFragments {
                maximum: MAX_SNAPSHOT_FRAGMENTS,
                required: MAX_SNAPSHOT_FRAGMENTS + 1,
            })
        );
    }

    #[test]
    fn minimum_fragmented_budget_carries_the_largest_snapshot() {
        let frame = encoded_snapshot(1, MAX_SNAPSHOT_PAYLOAD_BYTES);
        assert_eq!(frame.len(), MAX_SNAPSHOT_FRAME_BYTES);

        let fragments = encode_snapshot_fragments(&frame, MIN_FRAGMENTED_DATAGRAM_BYTES).unwrap();
        assert_eq!(fragments.len(), MAX_SNAPSHOT_FRAGMENTS);
        assert!(
            fragments
                .iter()
                .all(|fragment| fragment.len() <= MIN_FRAGMENTED_DATAGRAM_BYTES)
        );
        assert!(matches!(
            encode_snapshot_fragments(&frame, MIN_FRAGMENTED_DATAGRAM_BYTES - 1),
            Err(ProtocolError::TooManyFragments { .. })
        ));
    }

    #[test]
    fn fragmenting_rejects_unusable_budgets_and_non_snapshot_frames() {
        let frame = encoded_snapshot(1, 10);
        for budget in [0, SNAPSHOT_FRAGMENT_HEADER_BYTES] {
            assert_eq!(
                encode_snapshot_fragments(&frame, budget),
                Err(ProtocolError::DatagramBudgetTooSmall {
                    minimum: SNAPSHOT_FRAGMENT_HEADER_BYTES + 1,
                    actual: budget,
                })
            );
        }
        assert_eq!(
            encode_snapshot_fragments(&encode_command(1, b"not a snapshot").unwrap(), 100),
            Err(ProtocolError::UnexpectedKind(COMMAND_KIND))
        );
        assert!(matches!(
            encode_snapshot_fragments(&frame[..frame.len() - 1], 100),
            Err(ProtocolError::IncorrectLength { .. })
        ));
    }

    #[test]
    fn malformed_snapshot_fragments_are_rejected() {
        let valid = raw_fragment(9, 1, 2, b"chunk");
        assert_eq!(
            decode_snapshot_fragment(&valid).unwrap(),
            SnapshotFragment {
                tick: 9,
                index: 1,
                count: 2,
                chunk: b"chunk",
            }
        );

        let mut wrong_version = valid.clone();
        wrong_version[0] = PROTOCOL_VERSION + 1;
        let mut trailing = valid.clone();
        trailing.push(0);
        let cases = [
            (
                valid[..SNAPSHOT_FRAGMENT_HEADER_BYTES - 1].to_vec(),
                ProtocolError::IncorrectLength {
                    expected: SNAPSHOT_FRAGMENT_HEADER_BYTES,
                    actual: SNAPSHOT_FRAGMENT_HEADER_BYTES - 1,
                },
            ),
            (
                wrong_version,
                ProtocolError::UnsupportedVersion(PROTOCOL_VERSION + 1),
            ),
            (
                raw_fragment(9, 0, 0, b"chunk"),
                ProtocolError::InvalidFragmentCount(0),
            ),
            (
                raw_fragment(9, 0, 65, b"chunk"),
                ProtocolError::InvalidFragmentCount(65),
            ),
            (
                raw_fragment(9, 2, 2, b"chunk"),
                ProtocolError::InvalidFragmentIndex { index: 2, count: 2 },
            ),
            (raw_fragment(9, 0, 2, b""), ProtocolError::EmptyFragment),
            (
                valid[..valid.len() - 1].to_vec(),
                ProtocolError::IncorrectLength {
                    expected: valid.len(),
                    actual: valid.len() - 1,
                },
            ),
            (
                trailing,
                ProtocolError::IncorrectLength {
                    expected: valid.len(),
                    actual: valid.len() + 1,
                },
            ),
        ];
        for (bytes, expected) in cases {
            assert_eq!(decode_snapshot_fragment(&bytes), Err(expected.clone()));
            assert_eq!(decode_snapshot_datagram(&bytes), Err(expected));
        }
        assert_eq!(
            decode_snapshot_fragment(&encoded_snapshot(1, 4)),
            Err(ProtocolError::UnexpectedKind(SNAPSHOT_KIND))
        );
    }

    #[test]
    fn snapshot_datagrams_are_classified_as_whole_or_fragment() {
        let frame = encoded_snapshot(3, 8);
        assert_eq!(
            decode_snapshot_datagram(&frame).unwrap(),
            SnapshotDatagram::Snapshot(decode_snapshot(&frame).unwrap())
        );
        let fragment = raw_fragment(3, 0, 2, &frame[..10]);
        assert_eq!(
            decode_snapshot_datagram(&fragment).unwrap(),
            SnapshotDatagram::Fragment(SnapshotFragment {
                tick: 3,
                index: 0,
                count: 2,
                chunk: &frame[..10],
            })
        );
        for invalid in [
            Vec::new(),
            vec![PROTOCOL_VERSION],
            encode_command(1, b"command").unwrap(),
            encode_welcome(Welcome {
                player_id: 1,
                tick_hz: 20,
                max_players: 2,
                current_tick: 0,
                connection_epoch: 1,
                reconnect_token: [0; RECONNECT_TOKEN_BYTES],
                reconnect_grace_ticks: 1,
            })
            .to_vec(),
        ] {
            assert!(decode_snapshot_datagram(&invalid).is_err());
        }
    }

    #[test]
    fn owned_snapshot_decode_matches_borrowed_decode() {
        let frame = encoded_snapshot(11, 300);
        assert_eq!(
            decode_snapshot_owned(frame.clone()).unwrap(),
            decode_snapshot(&frame).unwrap()
        );
        let mut corrupted = frame;
        let last = corrupted.len() - 1;
        corrupted[last] ^= 0xff;
        assert!(matches!(
            decode_snapshot_owned(corrupted),
            Err(ProtocolError::InvalidStateHash { .. })
        ));
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

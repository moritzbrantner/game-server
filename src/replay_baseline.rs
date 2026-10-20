//! Runtime identity watermarks accompanying a simulation-owned canonical checkpoint.
use crate::protocol::{MAX_SNAPSHOT_PAYLOAD_BYTES, PlayerId, snapshot_hash};
use crate::replay::{LaneSequences, ReplayError};
use crate::simulation::SimulationSnapshot;
use std::collections::BTreeMap;

const PLAYER_BYTES: usize = 12;
const FIXED_BYTES: usize = 18;
pub(crate) const MAX_BASELINE_BODY_BYTES: usize =
    FIXED_BYTES + MAX_SNAPSHOT_PAYLOAD_BYTES + u16::MAX as usize * PLAYER_BYTES;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayPlayerWatermarks {
    pub player_id: PlayerId,
    pub datagram_sequence: u32,
    pub reliable_sequence: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayBaseline {
    pub snapshot: SimulationSnapshot,
    /// Highest identity ever admitted, including players already retired.
    pub highest_player_id: PlayerId,
    /// Strictly increasing current player identities; reconnect credentials stay in RecoveryImage.
    pub players: Vec<ReplayPlayerWatermarks>,
}

impl ReplayBaseline {
    pub(crate) fn validate(&self) -> Result<(), ReplayError> {
        if self.players.len() > u16::MAX as usize {
            return Err(ReplayError::InvalidBaseline("too many players"));
        }
        let mut previous = 0;
        for player in &self.players {
            if player.player_id <= previous || player.player_id > self.highest_player_id {
                return Err(ReplayError::InvalidBaseline(
                    "invalid player identity order or ceiling",
                ));
            }
            previous = player.player_id;
        }
        let snapshot = &self.snapshot;
        if snapshot.payload.len() > MAX_SNAPSHOT_PAYLOAD_BYTES {
            return Err(ReplayError::PayloadTooLarge {
                maximum: MAX_SNAPSHOT_PAYLOAD_BYTES,
                actual: snapshot.payload.len(),
            });
        }
        let expected = snapshot_hash(snapshot.tick, &snapshot.payload);
        if snapshot.state_hash != expected {
            return Err(ReplayError::InvalidCheckpointHash {
                tick: snapshot.tick,
                expected,
                actual: snapshot.state_hash,
            });
        }
        Ok(())
    }

    pub(crate) fn watermarks(&self) -> BTreeMap<PlayerId, LaneSequences> {
        self.players
            .iter()
            .map(|player| {
                (
                    player.player_id,
                    LaneSequences::new(player.datagram_sequence, player.reliable_sequence),
                )
            })
            .collect()
    }

    pub(crate) fn body_len(&self) -> Result<usize, ReplayError> {
        self.validate()?;
        Ok(FIXED_BYTES + self.players.len() * PLAYER_BYTES + self.snapshot.payload.len())
    }

    pub(crate) fn encode_body(&self, output: &mut Vec<u8>) {
        output.extend_from_slice(&self.highest_player_id.to_be_bytes());
        output.extend_from_slice(&(self.players.len() as u16).to_be_bytes());
        for player in &self.players {
            output.extend_from_slice(&player.player_id.to_be_bytes());
            output.extend_from_slice(&player.datagram_sequence.to_be_bytes());
            output.extend_from_slice(&player.reliable_sequence.to_be_bytes());
        }
        output.extend_from_slice(&self.snapshot.state_hash.to_be_bytes());
        output.extend_from_slice(&(self.snapshot.payload.len() as u32).to_be_bytes());
        output.extend_from_slice(&self.snapshot.payload);
    }

    pub(crate) fn decode(tick: u64, mut bytes: &[u8]) -> Result<Self, ReplayError> {
        fn take<const N: usize>(bytes: &mut &[u8]) -> Result<[u8; N], ReplayError> {
            let (value, rest) = bytes.split_at_checked(N).ok_or(ReplayError::Truncated)?;
            *bytes = rest;
            value.try_into().map_err(|_| ReplayError::Truncated)
        }
        let highest_player_id = u32::from_be_bytes(take(&mut bytes)?);
        let count = usize::from(u16::from_be_bytes(take(&mut bytes)?));
        // Validate the entire bounded frame before allocating from its count.
        if bytes.len() < count * PLAYER_BYTES + 12 {
            return Err(ReplayError::Truncated);
        }
        let mut players = Vec::with_capacity(count);
        for _ in 0..count {
            players.push(ReplayPlayerWatermarks {
                player_id: u32::from_be_bytes(take(&mut bytes)?),
                datagram_sequence: u32::from_be_bytes(take(&mut bytes)?),
                reliable_sequence: u32::from_be_bytes(take(&mut bytes)?),
            });
        }
        let state_hash = u64::from_be_bytes(take(&mut bytes)?);
        let length = u32::from_be_bytes(take(&mut bytes)?) as usize;
        if bytes.len() != length {
            return Err(ReplayError::InvalidBaseline(
                "checkpoint payload length mismatch",
            ));
        }
        let baseline = Self {
            snapshot: SimulationSnapshot {
                tick,
                state_hash,
                payload: bytes.to_vec(),
            },
            highest_player_id,
            players,
        };
        baseline.validate()?;
        Ok(baseline)
    }
}

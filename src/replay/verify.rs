//! The interpreter shared by replay verification and recovery.
use super::{ReplayError, ReplayLog, ReplayRecord};
use crate::protocol::{CommandLane, MAX_COMMAND_PAYLOAD_BYTES};
use crate::simulation::{GameSimulation, SimulationSnapshot};
use std::collections::BTreeMap;

pub(super) const MAX_VERIFIER_TICK_GAP: u64 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayVerification {
    pub records_verified: usize,
    pub checkpoints_verified: usize,
    pub final_snapshot: SimulationSnapshot,
}

/// Per-player sequence watermarks, one per command lane.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct LaneSequences {
    datagram: u32,
    reliable: u32,
}

impl LaneSequences {
    pub(crate) fn new(datagram: u32, reliable: u32) -> Self {
        Self { datagram, reliable }
    }
    pub(crate) fn get(&self, lane: CommandLane) -> u32 {
        match lane {
            CommandLane::Datagram => self.datagram,
            CommandLane::Reliable => self.reliable,
        }
    }

    pub(crate) fn set(&mut self, lane: CommandLane, sequence: u32) {
        match lane {
            CommandLane::Datagram => self.datagram = sequence,
            CommandLane::Reliable => self.reliable = sequence,
        }
    }
}

pub fn verify_replay<S: GameSimulation>(
    simulation: S,
    log: &ReplayLog,
) -> Result<ReplayVerification, ReplayError> {
    let (simulation, checkpoints_verified) = replay_into(simulation, log)?;
    Ok(ReplayVerification {
        records_verified: log.records().len(),
        checkpoints_verified,
        final_snapshot: simulation.snapshot()?,
    })
}

// Verification and recovery must interpret the same authoritative history.
// Runtime-owned identity and sequence rules cannot be delegated to game logic.
pub(crate) fn replay_into<S: GameSimulation>(
    mut simulation: S,
    log: &ReplayLog,
) -> Result<(S, usize), ReplayError> {
    let mut last_admitted_player_id = 0;
    let mut sequences = BTreeMap::new();
    let mut previous_tick = None;
    let mut checkpoints_verified = 0_usize;

    for (index, record) in log.records().iter().enumerate() {
        if let ReplayRecord::Baseline { baseline } = record {
            if index != 0 {
                return Err(ReplayError::InvalidBaseline("baseline must be first"));
            }
            baseline.validate()?;
            simulation.restore_checkpoint(&baseline.snapshot)?;
            if simulation.snapshot()? != baseline.snapshot {
                return Err(ReplayError::InvalidBaseline(
                    "restored simulation differs from checkpoint",
                ));
            }
            last_admitted_player_id = baseline.highest_player_id;
            sequences = baseline.watermarks();
            previous_tick = Some(baseline.snapshot.tick);
            checkpoints_verified += 1;
            continue;
        }
        let record_tick = record.tick();
        if let Some(previous) = previous_tick.filter(|previous| record_tick < *previous) {
            return Err(ReplayError::NonMonotonicTick {
                previous,
                actual: record_tick,
            });
        }
        let simulation_tick = simulation.current_tick();
        if record_tick < simulation_tick {
            return Err(ReplayError::ReplayStartsBeforeSimulation {
                simulation_tick,
                record_tick,
            });
        }
        let tick_gap = record_tick - simulation_tick;
        if tick_gap > MAX_VERIFIER_TICK_GAP {
            return Err(ReplayError::TickGapTooLarge {
                from_tick: simulation_tick,
                to_tick: record_tick,
                maximum: MAX_VERIFIER_TICK_GAP,
            });
        }
        if tick_gap == 1 {
            simulation.advance_tick()?;
        }

        match record {
            ReplayRecord::Baseline { .. } => {
                return Err(ReplayError::InvalidBaseline("baseline must be first"));
            }
            ReplayRecord::PlayerAdmitted { player_id, .. } => {
                if *player_id <= last_admitted_player_id {
                    return Err(ReplayError::NonIncreasingPlayerId {
                        previous: last_admitted_player_id,
                        actual: *player_id,
                    });
                }
                simulation.add_player(*player_id)?;
                sequences.insert(*player_id, LaneSequences::default());
                last_admitted_player_id = *player_id;
            }
            ReplayRecord::CommandApplied {
                player_id,
                lane,
                sequence,
                payload,
                ..
            } => {
                let watermarks = sequences
                    .get_mut(player_id)
                    .ok_or(ReplayError::UnknownCommandPlayer(*player_id))?;
                if *sequence == 0 {
                    return Err(ReplayError::InvalidSequence);
                }
                let previous = watermarks.get(*lane);
                if *sequence <= previous {
                    return Err(ReplayError::NonIncreasingSequence {
                        player_id: *player_id,
                        lane: *lane,
                        previous,
                        actual: *sequence,
                    });
                }
                simulation.apply_lane_command(*player_id, *lane, *sequence, payload)?;
                watermarks.set(*lane, *sequence);
            }
            ReplayRecord::HostInputApplied {
                player_id, payload, ..
            } => {
                if !sequences.contains_key(player_id) {
                    return Err(ReplayError::UnknownCommandPlayer(*player_id));
                }
                if payload.len() > MAX_COMMAND_PAYLOAD_BYTES {
                    return Err(ReplayError::PayloadTooLarge {
                        maximum: MAX_COMMAND_PAYLOAD_BYTES,
                        actual: payload.len(),
                    });
                }
                simulation.apply_host_input(*player_id, payload)?;
            }
            ReplayRecord::PlayerRemoved { player_id, .. } => {
                if sequences.remove(player_id).is_none()
                    || !simulation.try_remove_player(*player_id)?
                {
                    return Err(ReplayError::MissingPlayerOnRemoval(*player_id));
                }
            }
            ReplayRecord::DigestCheckpoint { tick, digest } => {
                let actual = simulation.snapshot()?;
                if canonical_digest(&actual) != *digest {
                    return Err(ReplayError::CheckpointDigestMismatch(*tick));
                }
                checkpoints_verified += 1;
            }
            ReplayRecord::Checkpoint { snapshot } => {
                let actual = simulation.snapshot()?;
                if actual.state_hash != snapshot.state_hash {
                    return Err(ReplayError::CheckpointMismatch {
                        tick: snapshot.tick,
                        expected_hash: snapshot.state_hash,
                        actual_hash: actual.state_hash,
                    });
                }
                if actual.payload != snapshot.payload {
                    return Err(ReplayError::CheckpointPayloadMismatch(snapshot.tick));
                }
                checkpoints_verified += 1;
            }
        }
        previous_tick = Some(record_tick);
    }

    Ok((simulation, checkpoints_verified))
}

pub(crate) fn canonical_digest(snapshot: &SimulationSnapshot) -> [u8; 32] {
    let mut context = ring::digest::Context::new(&ring::digest::SHA256);
    context.update(b"game-server/replay-checkpoint/v2\0");
    context.update(&snapshot.tick.to_be_bytes());
    context.update(&snapshot.state_hash.to_be_bytes());
    context.update(&(snapshot.payload.len() as u64).to_be_bytes());
    context.update(&snapshot.payload);
    let mut digest = [0_u8; 32];
    // SHA-256 always produces 32 bytes.
    digest.copy_from_slice(context.finish().as_ref());
    digest
}

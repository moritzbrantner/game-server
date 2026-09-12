use crate::{GameSimulation, MatchRuntime};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

pub const MAX_MATCH_ID_BYTES: usize = 64;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct MatchId(String);

impl MatchId {
    pub fn new(value: impl Into<String>) -> Result<Self, MatchIdError> {
        let value = value.into();
        if value.is_empty() {
            return Err(MatchIdError::Empty);
        }
        if value.len() > MAX_MATCH_ID_BYTES {
            return Err(MatchIdError::TooLong {
                maximum: MAX_MATCH_ID_BYTES,
                actual: value.len(),
            });
        }
        if let Some(character) = value.chars().find(|character| {
            !(character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
        }) {
            return Err(MatchIdError::InvalidCharacter(character));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MatchId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl TryFrom<&str> for MatchId {
    type Error = MatchIdError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MatchIdError {
    Empty,
    TooLong { maximum: usize, actual: usize },
    InvalidCharacter(char),
}

impl fmt::Display for MatchIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(formatter, "match id must not be empty"),
            Self::TooLong { maximum, actual } => {
                write!(
                    formatter,
                    "match id length {actual} exceeds maximum {maximum}"
                )
            }
            Self::InvalidCharacter(character) => write!(
                formatter,
                "match id contains unsupported character {character:?}; use ASCII letters, digits, '-' or '_'"
            ),
        }
    }
}

impl Error for MatchIdError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HostError {
    ZeroCapacity,
    Draining,
    DuplicateMatch(MatchId),
    AtCapacity {
        maximum: usize,
    },
    UnknownMatch(MatchId),
    MatchNotDraining(MatchId),
    MatchNotIdle {
        id: MatchId,
        active_players: usize,
        occupied_slots: usize,
    },
}

impl fmt::Display for HostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroCapacity => write!(formatter, "match host capacity must be non-zero"),
            Self::Draining => write!(formatter, "match host is draining and rejects placement"),
            Self::DuplicateMatch(id) => write!(formatter, "match {id} is already hosted"),
            Self::AtCapacity { maximum } => {
                write!(formatter, "match host has reached capacity {maximum}")
            }
            Self::UnknownMatch(id) => write!(formatter, "match {id} is not hosted"),
            Self::MatchNotDraining(id) => {
                write!(formatter, "match {id} must be draining before removal")
            }
            Self::MatchNotIdle {
                id,
                active_players,
                occupied_slots,
            } => write!(
                formatter,
                "match {id} still owns {occupied_slots} player slot(s), including {active_players} active connection(s)"
            ),
        }
    }
}

impl Error for HostError {}

pub struct PlacementFailure<S: GameSimulation> {
    error: HostError,
    id: MatchId,
    runtime: MatchRuntime<S>,
}

impl<S: GameSimulation> PlacementFailure<S> {
    fn new(error: HostError, id: MatchId, runtime: MatchRuntime<S>) -> Self {
        Self { error, id, runtime }
    }

    pub fn error(&self) -> &HostError {
        &self.error
    }

    pub fn id(&self) -> &MatchId {
        &self.id
    }

    pub fn into_parts(self) -> (HostError, MatchId, MatchRuntime<S>) {
        (self.error, self.id, self.runtime)
    }
}

impl<S: GameSimulation> fmt::Debug for PlacementFailure<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PlacementFailure")
            .field("error", &self.error)
            .field("id", &self.id)
            .field("runtime", &"<retained>")
            .finish()
    }
}

impl<S: GameSimulation> fmt::Display for PlacementFailure<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(formatter)
    }
}

impl<S: GameSimulation> Error for PlacementFailure<S> {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HostStatus {
    pub draining: bool,
    pub hosted_matches: usize,
    pub max_matches: usize,
    pub active_players: usize,
    pub occupied_player_slots: usize,
    pub player_capacity: usize,
}

impl HostStatus {
    pub fn remaining_match_capacity(self) -> usize {
        self.max_matches.saturating_sub(self.hosted_matches)
    }

    pub fn ready_for_new_match(self) -> bool {
        !self.draining && self.hosted_matches < self.max_matches
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MatchStatus {
    pub id: MatchId,
    pub draining: bool,
    pub frozen: bool,
    pub current_tick: u64,
    pub active_players: usize,
    pub occupied_player_slots: usize,
    pub max_players: usize,
}

#[derive(Debug)]
pub struct MatchHost<S> {
    matches: BTreeMap<MatchId, MatchRuntime<S>>,
    max_matches: usize,
    draining: bool,
}

impl<S: GameSimulation> MatchHost<S> {
    pub fn new(max_matches: usize) -> Result<Self, HostError> {
        if max_matches == 0 {
            return Err(HostError::ZeroCapacity);
        }
        Ok(Self {
            matches: BTreeMap::new(),
            max_matches,
            draining: false,
        })
    }

    pub fn max_matches(&self) -> usize {
        self.max_matches
    }

    pub fn len(&self) -> usize {
        self.matches.len()
    }

    pub fn is_empty(&self) -> bool {
        self.matches.is_empty()
    }

    pub fn is_draining(&self) -> bool {
        self.draining
    }

    pub fn status(&self) -> HostStatus {
        let (active_players, occupied_player_slots, player_capacity) = self.matches.values().fold(
            (0_usize, 0_usize, 0_usize),
            |(active, occupied, capacity), runtime| {
                (
                    active.saturating_add(runtime.active_count()),
                    occupied.saturating_add(runtime.slot_count()),
                    capacity.saturating_add(runtime.max_players()),
                )
            },
        );
        HostStatus {
            draining: self.draining,
            hosted_matches: self.matches.len(),
            max_matches: self.max_matches,
            active_players,
            occupied_player_slots,
            player_capacity,
        }
    }

    pub fn statuses(&self) -> Vec<MatchStatus> {
        self.matches
            .iter()
            .map(|(id, runtime)| MatchStatus {
                id: id.clone(),
                draining: runtime.is_draining(),
                frozen: runtime.is_frozen(),
                current_tick: runtime.current_tick(),
                active_players: runtime.active_count(),
                occupied_player_slots: runtime.slot_count(),
                max_players: runtime.max_players(),
            })
            .collect()
    }

    pub fn match_status(&self, id: &MatchId) -> Option<MatchStatus> {
        let runtime = self.matches.get(id)?;
        Some(MatchStatus {
            id: id.clone(),
            draining: runtime.is_draining(),
            frozen: runtime.is_frozen(),
            current_tick: runtime.current_tick(),
            active_players: runtime.active_count(),
            occupied_player_slots: runtime.slot_count(),
            max_players: runtime.max_players(),
        })
    }

    pub fn insert(
        &mut self,
        id: MatchId,
        runtime: MatchRuntime<S>,
    ) -> Result<(), PlacementFailure<S>> {
        if self.draining {
            return Err(PlacementFailure::new(HostError::Draining, id, runtime));
        }
        if self.matches.contains_key(&id) {
            return Err(PlacementFailure::new(
                HostError::DuplicateMatch(id.clone()),
                id,
                runtime,
            ));
        }
        if self.matches.len() >= self.max_matches {
            return Err(PlacementFailure::new(
                HostError::AtCapacity {
                    maximum: self.max_matches,
                },
                id,
                runtime,
            ));
        }
        self.matches.insert(id, runtime);
        Ok(())
    }

    pub fn runtime(&self, id: &MatchId) -> Option<&MatchRuntime<S>> {
        self.matches.get(id)
    }

    pub fn with_runtime_mut<R>(
        &mut self,
        id: &MatchId,
        operation: impl FnOnce(&mut MatchRuntime<S>) -> R,
    ) -> Option<R> {
        let host_draining = self.draining;
        let runtime = self.matches.get_mut(id)?;
        let preserve_drain = host_draining || runtime.is_draining();
        let result = operation(runtime);
        if preserve_drain {
            runtime.begin_drain();
        }
        Some(result)
    }

    pub fn begin_match_drain(&mut self, id: &MatchId) -> Result<(), HostError> {
        let runtime = self
            .matches
            .get_mut(id)
            .ok_or_else(|| HostError::UnknownMatch(id.clone()))?;
        runtime.begin_drain();
        Ok(())
    }

    pub fn begin_drain(&mut self) {
        self.draining = true;
        for runtime in self.matches.values_mut() {
            runtime.begin_drain();
        }
    }

    pub fn remove_drained(&mut self, id: &MatchId) -> Result<MatchRuntime<S>, HostError> {
        let runtime = self
            .matches
            .get(id)
            .ok_or_else(|| HostError::UnknownMatch(id.clone()))?;
        if !runtime.is_draining() {
            return Err(HostError::MatchNotDraining(id.clone()));
        }
        if runtime.active_count() != 0 || runtime.slot_count() != 0 {
            return Err(HostError::MatchNotIdle {
                id: id.clone(),
                active_players: runtime.active_count(),
                occupied_slots: runtime.slot_count(),
            });
        }
        Ok(self
            .matches
            .remove(id)
            .expect("match existence checked before removal"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DemoSimulation, RECONNECT_TOKEN_BYTES, ReconnectToken, RuntimeError};

    fn id(value: &str) -> MatchId {
        MatchId::new(value).unwrap()
    }

    fn runtime(grace_ticks: u64) -> MatchRuntime<DemoSimulation> {
        MatchRuntime::new(DemoSimulation::new(), grace_ticks)
    }

    fn token(value: u8) -> ReconnectToken {
        ReconnectToken([value; RECONNECT_TOKEN_BYTES])
    }

    #[test]
    fn match_ids_are_bounded_and_route_safe() {
        assert_eq!(MatchId::new(""), Err(MatchIdError::Empty));
        assert_eq!(
            MatchId::new("a".repeat(MAX_MATCH_ID_BYTES + 1)),
            Err(MatchIdError::TooLong {
                maximum: MAX_MATCH_ID_BYTES,
                actual: MAX_MATCH_ID_BYTES + 1,
            })
        );
        assert_eq!(
            MatchId::new("match/one"),
            Err(MatchIdError::InvalidCharacter('/'))
        );
        assert_eq!(id("match_01-west").as_str(), "match_01-west");
    }

    #[test]
    fn host_enforces_nonzero_bounded_unique_lossless_placement() {
        assert!(matches!(
            MatchHost::<DemoSimulation>::new(0),
            Err(HostError::ZeroCapacity)
        ));
        let mut host = MatchHost::new(1).unwrap();
        host.insert(id("one"), runtime(10)).unwrap();

        let duplicate = host.insert(id("one"), runtime(10)).unwrap_err();
        assert_eq!(duplicate.error(), &HostError::DuplicateMatch(id("one")));
        assert_eq!(duplicate.id(), &id("one"));

        let mut retained = runtime(10);
        retained.admit(token(9)).unwrap();
        let capacity = host.insert(id("two"), retained).unwrap_err();
        assert_eq!(capacity.error(), &HostError::AtCapacity { maximum: 1 });
        let (error, returned_id, returned_runtime) = capacity.into_parts();
        assert_eq!(error, HostError::AtCapacity { maximum: 1 });
        assert_eq!(returned_id, id("two"));
        assert_eq!(returned_runtime.active_count(), 1);
        assert_eq!(returned_runtime.slot_count(), 1);
    }

    #[test]
    fn per_match_drain_is_isolated_and_process_drain_is_global() {
        let mut host = MatchHost::new(3).unwrap();
        host.insert(id("one"), runtime(10)).unwrap();
        host.insert(id("two"), runtime(10)).unwrap();

        host.begin_match_drain(&id("one")).unwrap();
        assert!(host.runtime(&id("one")).unwrap().is_draining());
        assert!(!host.runtime(&id("two")).unwrap().is_draining());
        assert!(host.status().ready_for_new_match());

        host.with_runtime_mut(&id("one"), |runtime| {
            runtime.resume_after_failed_recovery();
        })
        .unwrap();
        assert!(host.runtime(&id("one")).unwrap().is_draining());

        host.begin_drain();
        assert!(host.is_draining());
        assert!(host.runtime(&id("two")).unwrap().is_draining());
        assert!(!host.status().ready_for_new_match());
        host.with_runtime_mut(&id("two"), |runtime| {
            runtime.resume_after_failed_recovery();
        })
        .unwrap();
        assert!(host.runtime(&id("two")).unwrap().is_draining());
        let rejected = host.insert(id("three"), runtime(10)).unwrap_err();
        assert_eq!(rejected.error(), &HostError::Draining);
        assert_eq!(rejected.id(), &id("three"));
        let (_, _, returned_runtime) = rejected.into_parts();
        assert_eq!(returned_runtime.slot_count(), 0);
    }

    #[test]
    fn status_reports_capacity_and_session_facts_without_stored_ready_state() {
        let mut first = runtime(10);
        first.admit(token(1)).unwrap();
        let mut host = MatchHost::new(2).unwrap();
        host.insert(id("b"), runtime(10)).unwrap();
        host.insert(id("a"), first).unwrap();

        let status = host.status();
        assert_eq!(status.hosted_matches, 2);
        assert_eq!(status.max_matches, 2);
        assert_eq!(status.active_players, 1);
        assert_eq!(status.occupied_player_slots, 1);
        assert_eq!(
            status.player_capacity,
            DemoSimulation::new().max_players() * 2
        );
        assert_eq!(status.remaining_match_capacity(), 0);
        assert!(!status.ready_for_new_match());

        let statuses = host.statuses();
        assert_eq!(statuses[0].id, id("a"));
        assert_eq!(statuses[1].id, id("b"));
    }

    #[test]
    fn drained_match_cannot_be_removed_while_reconnectable_slots_remain() {
        let mut live = runtime(0);
        let lease = live.admit(token(1)).unwrap();
        let mut host = MatchHost::new(1).unwrap();
        host.insert(id("one"), live).unwrap();

        assert!(matches!(
            host.remove_drained(&id("one")),
            Err(HostError::MatchNotDraining(_))
        ));
        host.begin_match_drain(&id("one")).unwrap();
        assert!(matches!(
            host.remove_drained(&id("one")),
            Err(HostError::MatchNotIdle {
                active_players: 1,
                occupied_slots: 1,
                ..
            })
        ));

        host.with_runtime_mut(&id("one"), |runtime| {
            assert!(runtime.disconnect(lease.player_id, lease.connection_epoch));
            assert!(matches!(
                runtime.admit(token(2)),
                Err(RuntimeError::Draining)
            ));
            runtime.advance_tick().unwrap();
            assert_eq!(runtime.slot_count(), 1);
            runtime.advance_tick().unwrap();
            assert_eq!(runtime.slot_count(), 0);
        })
        .unwrap();

        let removed = host.remove_drained(&id("one")).unwrap();
        assert_eq!(removed.slot_count(), 0);
        assert!(host.is_empty());
        assert!(host.status().ready_for_new_match());
    }
}

use crate::protocol::{MAX_PLAYERS, PlayerId, RECONNECT_TOKEN_BYTES};
use std::collections::BTreeMap;
use std::fmt;

pub const DEFAULT_RECONNECT_GRACE_TICKS: u64 = 20 * 30;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ReconnectToken(pub [u8; RECONNECT_TOKEN_BYTES]);

impl ReconnectToken {
    pub fn encode_hex(self) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut output = String::with_capacity(RECONNECT_TOKEN_BYTES * 2);
        for byte in self.0 {
            output.push(HEX[usize::from(byte >> 4)] as char);
            output.push(HEX[usize::from(byte & 0x0f)] as char);
        }
        output
    }

    pub fn decode_hex(value: &str) -> Option<Self> {
        if value.len() != RECONNECT_TOKEN_BYTES * 2 {
            return None;
        }
        let bytes = value.as_bytes();
        let mut token = [0_u8; RECONNECT_TOKEN_BYTES];
        for (index, output) in token.iter_mut().enumerate() {
            let high = decode_hex_nibble(bytes[index * 2])?;
            let low = decode_hex_nibble(bytes[index * 2 + 1])?;
            *output = (high << 4) | low;
        }
        Some(Self(token))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SessionLease {
    pub player_id: PlayerId,
    pub connection_epoch: u32,
    pub reconnect_token: ReconnectToken,
    pub reconnect_grace_ticks: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionError {
    PlayerCapacity,
    TokenCollision,
    UnknownToken,
    AlreadyConnected,
    ReconnectExpired,
    ConnectionEpochExhausted,
}

impl fmt::Display for SessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PlayerCapacity => write!(formatter, "match has reached player capacity"),
            Self::TokenCollision => write!(formatter, "reconnect token collision"),
            Self::UnknownToken => write!(formatter, "unknown reconnect token"),
            Self::AlreadyConnected => {
                write!(formatter, "player slot already has an active connection")
            }
            Self::ReconnectExpired => write!(formatter, "reconnect grace period has expired"),
            Self::ConnectionEpochExhausted => write!(formatter, "connection epoch exhausted"),
        }
    }
}

impl std::error::Error for SessionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PlayerSession {
    token: ReconnectToken,
    connection_epoch: u32,
    connected: bool,
    expires_at_tick: u64,
}

#[derive(Clone, Debug)]
pub struct SessionRegistry {
    reconnect_grace_ticks: u64,
    next_player_id: PlayerId,
    players: BTreeMap<PlayerId, PlayerSession>,
    tokens: BTreeMap<ReconnectToken, PlayerId>,
}

impl Default for SessionRegistry {
    fn default() -> Self {
        Self::new(DEFAULT_RECONNECT_GRACE_TICKS)
    }
}

impl SessionRegistry {
    pub fn new(reconnect_grace_ticks: u64) -> Self {
        Self {
            reconnect_grace_ticks,
            next_player_id: 1,
            players: BTreeMap::new(),
            tokens: BTreeMap::new(),
        }
    }

    pub fn slot_count(&self) -> usize {
        self.players.len()
    }

    pub fn active_count(&self) -> usize {
        self.players
            .values()
            .filter(|session| session.connected)
            .count()
    }

    pub fn admit(&mut self, reconnect_token: ReconnectToken) -> Result<SessionLease, SessionError> {
        if self.players.len() >= MAX_PLAYERS {
            return Err(SessionError::PlayerCapacity);
        }
        if self.tokens.contains_key(&reconnect_token) {
            return Err(SessionError::TokenCollision);
        }

        let player_id = self.next_player_id;
        self.next_player_id = self.next_player_id.checked_add(1).unwrap_or(0);
        if player_id == 0 || self.next_player_id == 0 {
            return Err(SessionError::PlayerCapacity);
        }
        let session = PlayerSession {
            token: reconnect_token,
            connection_epoch: 1,
            connected: true,
            expires_at_tick: u64::MAX,
        };
        self.players.insert(player_id, session);
        self.tokens.insert(reconnect_token, player_id);
        Ok(SessionLease {
            player_id,
            connection_epoch: session.connection_epoch,
            reconnect_token: session.token,
            reconnect_grace_ticks: self.reconnect_grace_ticks,
        })
    }

    pub fn reconnect(
        &mut self,
        previous_token: ReconnectToken,
        replacement_token: ReconnectToken,
        current_tick: u64,
    ) -> Result<SessionLease, SessionError> {
        if previous_token == replacement_token || self.tokens.contains_key(&replacement_token) {
            return Err(SessionError::TokenCollision);
        }
        let player_id = *self
            .tokens
            .get(&previous_token)
            .ok_or(SessionError::UnknownToken)?;
        let session = self
            .players
            .get_mut(&player_id)
            .ok_or(SessionError::UnknownToken)?;
        if session.connected {
            return Err(SessionError::AlreadyConnected);
        }
        if current_tick > session.expires_at_tick {
            return Err(SessionError::ReconnectExpired);
        }
        session.connection_epoch = session
            .connection_epoch
            .checked_add(1)
            .ok_or(SessionError::ConnectionEpochExhausted)?;
        session.connected = true;
        session.expires_at_tick = u64::MAX;
        session.token = replacement_token;
        let lease = SessionLease {
            player_id,
            connection_epoch: session.connection_epoch,
            reconnect_token: session.token,
            reconnect_grace_ticks: self.reconnect_grace_ticks,
        };
        self.tokens.remove(&previous_token);
        self.tokens.insert(replacement_token, player_id);
        Ok(lease)
    }

    pub fn disconnect(
        &mut self,
        player_id: PlayerId,
        connection_epoch: u32,
        current_tick: u64,
    ) -> bool {
        let Some(session) = self.players.get_mut(&player_id) else {
            return false;
        };
        if !session.connected || session.connection_epoch != connection_epoch {
            return false;
        }
        session.connected = false;
        session.expires_at_tick = current_tick.saturating_add(self.reconnect_grace_ticks);
        true
    }

    pub fn remove_slot(&mut self, player_id: PlayerId) -> bool {
        let Some(session) = self.players.remove(&player_id) else {
            return false;
        };
        self.tokens.remove(&session.token);
        true
    }

    pub fn expire(&mut self, current_tick: u64) -> Vec<PlayerId> {
        let expired = self
            .players
            .iter()
            .filter_map(|(&player_id, session)| {
                (!session.connected && current_tick > session.expires_at_tick).then_some(player_id)
            })
            .collect::<Vec<_>>();
        for player_id in &expired {
            self.remove_slot(*player_id);
        }
        expired
    }
}

fn decode_hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(value: u8) -> ReconnectToken {
        ReconnectToken([value; RECONNECT_TOKEN_BYTES])
    }

    #[test]
    fn reconnect_rotates_token_and_epoch() {
        let mut sessions = SessionRegistry::new(10);
        let admitted = sessions.admit(token(1)).unwrap();
        assert!(sessions.disconnect(admitted.player_id, admitted.connection_epoch, 5));
        let reconnected = sessions.reconnect(token(1), token(2), 12).unwrap();
        assert_eq!(reconnected.player_id, admitted.player_id);
        assert_eq!(reconnected.connection_epoch, 2);
        assert_eq!(reconnected.reconnect_token, token(2));
        assert_eq!(
            sessions.reconnect(token(1), token(3), 12),
            Err(SessionError::UnknownToken)
        );
    }

    #[test]
    fn stale_disconnect_cannot_evict_newer_connection() {
        let mut sessions = SessionRegistry::new(10);
        let first = sessions.admit(token(1)).unwrap();
        assert!(sessions.disconnect(first.player_id, first.connection_epoch, 1));
        let second = sessions.reconnect(token(1), token(2), 2).unwrap();
        assert!(!sessions.disconnect(first.player_id, first.connection_epoch, 3));
        assert_eq!(sessions.active_count(), 1);
        assert!(sessions.disconnect(second.player_id, second.connection_epoch, 3));
    }

    #[test]
    fn expired_disconnected_slots_are_removed() {
        let mut sessions = SessionRegistry::new(5);
        let lease = sessions.admit(token(1)).unwrap();
        assert!(sessions.disconnect(lease.player_id, lease.connection_epoch, 10));
        assert!(sessions.expire(15).is_empty());
        assert_eq!(sessions.expire(16), vec![lease.player_id]);
        assert_eq!(sessions.slot_count(), 0);
    }

    #[test]
    fn active_token_cannot_be_replayed() {
        let mut sessions = SessionRegistry::new(10);
        sessions.admit(token(1)).unwrap();
        assert_eq!(
            sessions.reconnect(token(1), token(2), 1),
            Err(SessionError::AlreadyConnected)
        );
    }

    #[test]
    fn token_hex_round_trip_is_exact() {
        let value = ReconnectToken([0xab; RECONNECT_TOKEN_BYTES]);
        assert_eq!(ReconnectToken::decode_hex(&value.encode_hex()), Some(value));
        assert_eq!(ReconnectToken::decode_hex("not-a-token"), None);
    }
}

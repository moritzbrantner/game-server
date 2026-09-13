use crate::control::{CONTROL_FORMAT_VERSION, MAX_CONTROL_PAYLOAD_BYTES};
use crate::host::{MatchId, MatchIdError};
use crate::protocol::{
    MAX_COMMAND_PAYLOAD_BYTES, MAX_SNAPSHOT_PAYLOAD_BYTES, PROTOCOL_VERSION, RECONNECT_TOKEN_BYTES,
};
use crate::session::ReconnectToken;
use std::error::Error;
use std::fmt;

pub const BROWSER_ROUTE_VERSION: u8 = 1;
pub const BROWSER_MATCH_SEGMENT: &str = "matches";
pub const BROWSER_RECONNECT_SEGMENT: &str = "reconnect";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BrowserProtocolContract {
    pub route_version: u8,
    pub game_protocol_version: u8,
    pub control_format_version: u8,
    pub reconnect_token_bytes: usize,
    pub max_command_payload_bytes: usize,
    pub max_snapshot_payload_bytes: usize,
    pub max_control_payload_bytes: usize,
}

pub const BROWSER_PROTOCOL_CONTRACT: BrowserProtocolContract = BrowserProtocolContract {
    route_version: BROWSER_ROUTE_VERSION,
    game_protocol_version: PROTOCOL_VERSION,
    control_format_version: CONTROL_FORMAT_VERSION,
    reconnect_token_bytes: RECONNECT_TOKEN_BYTES,
    max_command_payload_bytes: MAX_COMMAND_PAYLOAD_BYTES,
    max_snapshot_payload_bytes: MAX_SNAPSHOT_PAYLOAD_BYTES,
    max_control_payload_bytes: MAX_CONTROL_PAYLOAD_BYTES,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BrowserAdmission {
    New,
    Reconnect(ReconnectToken),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserSessionRoute {
    pub match_id: MatchId,
    pub admission: BrowserAdmission,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserRoutePrefix(String);

impl BrowserRoutePrefix {
    pub fn new(value: impl Into<String>) -> Result<Self, BrowserRouteError> {
        let value = value.into();
        validate_base_path(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn match_path(&self, match_id: &MatchId) -> String {
        format!(
            "{}/{}/{}",
            self.as_str(),
            BROWSER_MATCH_SEGMENT,
            match_id.as_str()
        )
    }

    pub fn reconnect_path(&self, match_id: &MatchId, token: ReconnectToken) -> String {
        format!(
            "{}/{}/{}",
            self.match_path(match_id),
            BROWSER_RECONNECT_SEGMENT,
            token.encode_hex()
        )
    }

    pub fn parse(&self, path: &str) -> Result<Option<BrowserSessionRoute>, BrowserRouteError> {
        let prefix = format!("{}/{}/", self.as_str(), BROWSER_MATCH_SEGMENT);
        let Some(remainder) = path.strip_prefix(&prefix) else {
            return Ok(None);
        };
        let mut segments = remainder.split('/');
        let match_id_segment = segments.next().unwrap_or_default();
        let match_id =
            MatchId::new(match_id_segment.to_owned()).map_err(BrowserRouteError::InvalidMatchId)?;

        match (segments.next(), segments.next(), segments.next()) {
            (None, None, None) => Ok(Some(BrowserSessionRoute {
                match_id,
                admission: BrowserAdmission::New,
            })),
            (Some(BROWSER_RECONNECT_SEGMENT), Some(token), None) => {
                let reconnect_token = ReconnectToken::decode_hex(token)
                    .ok_or(BrowserRouteError::InvalidReconnectToken)?;
                Ok(Some(BrowserSessionRoute {
                    match_id,
                    admission: BrowserAdmission::Reconnect(reconnect_token),
                }))
            }
            _ => Err(BrowserRouteError::InvalidRouteShape),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BrowserRouteError {
    InvalidBasePath,
    InvalidMatchId(MatchIdError),
    InvalidReconnectToken,
    InvalidRouteShape,
}

impl fmt::Display for BrowserRouteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBasePath => write!(
                formatter,
                "browser route base path must use canonical absolute ASCII segments containing only letters, digits, '-' or '_'"
            ),
            Self::InvalidMatchId(error) => write!(formatter, "invalid browser match id: {error}"),
            Self::InvalidReconnectToken => {
                write!(formatter, "invalid browser reconnect token")
            }
            Self::InvalidRouteShape => write!(formatter, "invalid browser session route shape"),
        }
    }
}

impl Error for BrowserRouteError {}

fn validate_base_path(value: &str) -> Result<(), BrowserRouteError> {
    if value.len() < 2 || !value.starts_with('/') || value.ends_with('/') {
        return Err(BrowserRouteError::InvalidBasePath);
    }

    if value[1..].split('/').any(|segment| {
        segment.is_empty()
            || segment
                .chars()
                .any(|character| !(character.is_ascii_alphanumeric() || matches!(character, '-' | '_')))
    }) {
        return Err(BrowserRouteError::InvalidBasePath);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn match_id(value: &str) -> MatchId {
        MatchId::new(value).unwrap()
    }

    #[test]
    fn browser_protocol_contract_tracks_wire_limits() {
        assert_eq!(BROWSER_PROTOCOL_CONTRACT.route_version, 1);
        assert_eq!(
            BROWSER_PROTOCOL_CONTRACT.game_protocol_version,
            PROTOCOL_VERSION
        );
        assert_eq!(
            BROWSER_PROTOCOL_CONTRACT.control_format_version,
            CONTROL_FORMAT_VERSION
        );
        assert_eq!(
            BROWSER_PROTOCOL_CONTRACT.reconnect_token_bytes,
            RECONNECT_TOKEN_BYTES
        );
    }

    #[test]
    fn builds_and_parses_match_and_reconnect_routes() {
        let prefix = BrowserRoutePrefix::new("/game").unwrap();
        let nested_prefix = BrowserRoutePrefix::new("/api/game_v1").unwrap();
        let id = match_id("uno_01");
        let token = ReconnectToken([0xab; RECONNECT_TOKEN_BYTES]);

        assert_eq!(prefix.match_path(&id), "/game/matches/uno_01");
        assert_eq!(
            nested_prefix.match_path(&id),
            "/api/game_v1/matches/uno_01"
        );
        assert_eq!(
            prefix.reconnect_path(&id, token),
            format!("/game/matches/uno_01/reconnect/{}", token.encode_hex())
        );
        assert_eq!(
            prefix.parse("/game/matches/uno_01").unwrap(),
            Some(BrowserSessionRoute {
                match_id: id.clone(),
                admission: BrowserAdmission::New,
            })
        );
        assert_eq!(
            prefix.parse(&prefix.reconnect_path(&id, token)).unwrap(),
            Some(BrowserSessionRoute {
                match_id: id,
                admission: BrowserAdmission::Reconnect(token),
            })
        );
    }

    #[test]
    fn unrelated_paths_are_ignored_and_owned_malformed_routes_fail_closed() {
        let prefix = BrowserRoutePrefix::new("/game").unwrap();

        assert_eq!(prefix.parse("/health").unwrap(), None);
        assert!(matches!(
            prefix.parse("/game/matches/bad/id"),
            Err(BrowserRouteError::InvalidRouteShape)
        ));
        assert!(matches!(
            prefix.parse("/game/matches/one/reconnect/not-valid"),
            Err(BrowserRouteError::InvalidReconnectToken)
        ));
        assert!(matches!(
            prefix.parse("/game/matches/%2F"),
            Err(BrowserRouteError::InvalidMatchId(_))
        ));
    }

    #[test]
    fn rejects_ambiguous_or_browser_normalized_base_paths() {
        for value in [
            "",
            "/",
            "game",
            "/game/",
            "/game//sessions",
            "/game?x=1",
            "/game/.",
            "/game/..",
            "/game\\sessions",
            "/game sessions",
            "/gáme",
            "/game/%2e%2e",
        ] {
            assert_eq!(
                BrowserRoutePrefix::new(value),
                Err(BrowserRouteError::InvalidBasePath)
            );
        }
    }
}

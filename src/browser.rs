use crate::control::{CONTROL_FORMAT_VERSION, MAX_CONTROL_PAYLOAD_BYTES};
use crate::host::{MatchId, MatchIdError};
use crate::protocol::{
    MAX_COMMAND_PAYLOAD_BYTES, MAX_COMMAND_REJECTION_PAYLOAD_BYTES, MAX_SNAPSHOT_FRAGMENTS,
    MAX_SNAPSHOT_PAYLOAD_BYTES, PROTOCOL_VERSION, RECONNECT_TOKEN_BYTES,
};
use crate::session::ReconnectToken;
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;

pub const MAX_BROWSER_ORIGINS: usize = 16;
pub const MAX_BROWSER_ORIGIN_BYTES: usize = 2048;

/// Exact serialized HTTP(S) origins. This browser boundary is not client authentication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserOriginAllowlist(BTreeSet<String>);

impl BrowserOriginAllowlist {
    pub fn new(
        origins: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, BrowserOriginError> {
        let mut allowed = BTreeSet::new();
        for (index, origin) in origins.into_iter().enumerate() {
            let origin = origin.into();
            if index >= MAX_BROWSER_ORIGINS || origin.len() > MAX_BROWSER_ORIGIN_BYTES {
                return Err(BrowserOriginError);
            }
            let parsed = url::Url::parse(&origin).map_err(|_| BrowserOriginError)?;
            if !matches!(parsed.scheme(), "http" | "https")
                || parsed.origin().ascii_serialization() != origin
            {
                return Err(BrowserOriginError);
            }
            allowed.insert(origin);
        }
        if allowed.is_empty() {
            return Err(BrowserOriginError);
        }
        Ok(Self(allowed))
    }

    pub fn allows(&self, origin: Option<&str>) -> bool {
        origin.is_some_and(|origin| self.0.contains(origin))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BrowserOriginError;

impl fmt::Display for BrowserOriginError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(
            "browser allowlist requires 1–16 canonical HTTP(S) origins up to 2048 bytes each",
        )
    }
}
impl Error for BrowserOriginError {}

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
    pub max_command_rejection_payload_bytes: usize,
    pub max_snapshot_payload_bytes: usize,
    pub max_snapshot_fragments: usize,
    pub max_control_payload_bytes: usize,
}

pub const BROWSER_PROTOCOL_CONTRACT: BrowserProtocolContract = BrowserProtocolContract {
    route_version: BROWSER_ROUTE_VERSION,
    game_protocol_version: PROTOCOL_VERSION,
    control_format_version: CONTROL_FORMAT_VERSION,
    reconnect_token_bytes: RECONNECT_TOKEN_BYTES,
    max_command_payload_bytes: MAX_COMMAND_PAYLOAD_BYTES,
    max_command_rejection_payload_bytes: MAX_COMMAND_REJECTION_PAYLOAD_BYTES,
    max_snapshot_payload_bytes: MAX_SNAPSHOT_PAYLOAD_BYTES,
    max_snapshot_fragments: MAX_SNAPSHOT_FRAGMENTS,
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
            || segment.chars().any(|character| {
                !(character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
            })
    }) {
        return Err(BrowserRouteError::InvalidBasePath);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origin_allowlists_are_canonical_bounded_exact_and_never_match_missing_origins() {
        let allowed = BrowserOriginAllowlist::new([
            "https://board.example",
            "http://127.0.0.1:5173",
            "https://board.example",
        ])
        .unwrap();
        assert!(allowed.allows(Some("https://board.example")));
        assert!(allowed.allows(Some("http://127.0.0.1:5173")));
        for origin in [
            None,
            Some("null"),
            Some("https://board.example.attacker.test"),
            Some("https://board.example/"),
            Some("http://board.example"),
        ] {
            assert!(!allowed.allows(origin));
        }
        assert!(BrowserOriginAllowlist::new(std::iter::empty::<String>()).is_err());
        for origin in [
            "*",
            "null",
            "https://board.example/",
            "https://board.example:443",
            "https://user:secret@board.example",
            "https://board.example/path",
            "https://board.example?query",
            "https://board.example#fragment",
        ] {
            assert!(BrowserOriginAllowlist::new([origin]).is_err());
        }
        assert!(
            BrowserOriginAllowlist::new([format!(
                "https://{}.example",
                "a".repeat(MAX_BROWSER_ORIGIN_BYTES)
            )])
            .is_err()
        );
        assert!(
            BrowserOriginAllowlist::new(
                (0..=MAX_BROWSER_ORIGINS).map(|i| format!("https://board-{i}.example"))
            )
            .is_err()
        );
        assert!(
            !BrowserOriginAllowlist::new(["https://user:secret@board.example"])
                .unwrap_err()
                .to_string()
                .contains("secret")
        );
    }

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
        assert_eq!(
            BROWSER_PROTOCOL_CONTRACT.max_snapshot_fragments,
            MAX_SNAPSHOT_FRAGMENTS
        );
    }

    #[test]
    fn builds_and_parses_match_and_reconnect_routes() {
        let prefix = BrowserRoutePrefix::new("/game").unwrap();
        let nested_prefix = BrowserRoutePrefix::new("/api/game_v1").unwrap();
        let id = match_id("uno_01");
        let token = ReconnectToken([0xab; RECONNECT_TOKEN_BYTES]);

        assert_eq!(prefix.match_path(&id), "/game/matches/uno_01");
        assert_eq!(nested_prefix.match_path(&id), "/api/game_v1/matches/uno_01");
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

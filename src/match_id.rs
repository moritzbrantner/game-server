//! Match identifiers shared by hosts, control frames and browser routes.

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

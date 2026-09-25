use crate::protocol::{MAX_COMMAND_PAYLOAD_BYTES, MAX_SNAPSHOT_PAYLOAD_BYTES, PlayerId, snapshot_hash};
use crate::simulation::{GameSimulation, SimulationError, SimulationSnapshot, SnapshotScope};
use std::fmt;

pub const EXTERNAL_SIMULATION_PROTOCOL_VERSION: u8 = 1;
pub const MAX_EXTERNAL_SIMULATION_ERROR_BYTES: usize = 1024;

const REQUEST_HEADER_BYTES: usize = 2;
const RESPONSE_HEADER_BYTES: usize = 3;
const COMMAND_REQUEST_FIXED_BYTES: usize = REQUEST_HEADER_BYTES + 4 + 4 + 2;
const SNAPSHOT_RESPONSE_FIXED_BYTES: usize = RESPONSE_HEADER_BYTES + 8 + 8 + 2;
const STATUS_OK: u8 = 0;
const STATUS_ERROR: u8 = 1;

pub const MAX_EXTERNAL_SIMULATION_FRAME_BYTES: usize =
    SNAPSHOT_RESPONSE_FIXED_BYTES + MAX_SNAPSHOT_PAYLOAD_BYTES;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExternalSimulationProtocolContract {
    pub version: u8,
    pub command_payload_bytes: usize,
    pub snapshot_payload_bytes: usize,
    pub error_payload_bytes: usize,
    pub maximum_frame_bytes: usize,
}

pub const EXTERNAL_SIMULATION_PROTOCOL_CONTRACT: ExternalSimulationProtocolContract =
    ExternalSimulationProtocolContract {
        version: EXTERNAL_SIMULATION_PROTOCOL_VERSION,
        command_payload_bytes: MAX_COMMAND_PAYLOAD_BYTES,
        snapshot_payload_bytes: MAX_SNAPSHOT_PAYLOAD_BYTES,
        error_payload_bytes: MAX_EXTERNAL_SIMULATION_ERROR_BYTES,
        maximum_frame_bytes: MAX_EXTERNAL_SIMULATION_FRAME_BYTES,
    };

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExternalSimulationOperation {
    Describe,
    AddPlayer,
    RemovePlayer,
    ApplyCommand,
    AdvanceTick,
    Snapshot,
    SnapshotFor,
}

impl ExternalSimulationOperation {
    fn code(self) -> u8 {
        match self {
            Self::Describe => 1,
            Self::AddPlayer => 2,
            Self::RemovePlayer => 3,
            Self::ApplyCommand => 4,
            Self::AdvanceTick => 5,
            Self::Snapshot => 6,
            Self::SnapshotFor => 7,
        }
    }

    fn from_code(code: u8) -> Result<Self, ExternalSimulationError> {
        match code {
            1 => Ok(Self::Describe),
            2 => Ok(Self::AddPlayer),
            3 => Ok(Self::RemovePlayer),
            4 => Ok(Self::ApplyCommand),
            5 => Ok(Self::AdvanceTick),
            6 => Ok(Self::Snapshot),
            7 => Ok(Self::SnapshotFor),
            _ => Err(ExternalSimulationError::UnknownOperation(code)),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExternalSimulationDescriptor {
    pub tick_hz: u16,
    pub max_players: u16,
    pub current_tick: u64,
    pub snapshot_scope: SnapshotScope,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExternalSimulationRequest {
    Describe,
    AddPlayer(PlayerId),
    RemovePlayer(PlayerId),
    ApplyCommand {
        player_id: PlayerId,
        sequence: u32,
        payload: Vec<u8>,
    },
    AdvanceTick,
    Snapshot,
    SnapshotFor(PlayerId),
}

impl ExternalSimulationRequest {
    pub fn operation(&self) -> ExternalSimulationOperation {
        match self {
            Self::Describe => ExternalSimulationOperation::Describe,
            Self::AddPlayer(_) => ExternalSimulationOperation::AddPlayer,
            Self::RemovePlayer(_) => ExternalSimulationOperation::RemovePlayer,
            Self::ApplyCommand { .. } => ExternalSimulationOperation::ApplyCommand,
            Self::AdvanceTick => ExternalSimulationOperation::AdvanceTick,
            Self::Snapshot => ExternalSimulationOperation::Snapshot,
            Self::SnapshotFor(_) => ExternalSimulationOperation::SnapshotFor,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExternalSimulationResponse {
    Descriptor(ExternalSimulationDescriptor),
    Acknowledged,
    PlayerRemoved(bool),
    TickAdvanced(u64),
    Snapshot(SimulationSnapshot),
    Rejected(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExternalSimulationError {
    Bridge(String),
    UnsupportedVersion(u8),
    UnknownOperation(u8),
    UnexpectedStatus(u8),
    UnexpectedOperation {
        expected: ExternalSimulationOperation,
        actual: ExternalSimulationOperation,
    },
    UnexpectedResponse(ExternalSimulationOperation),
    IncorrectLength {
        expected: usize,
        actual: usize,
    },
    PayloadTooLarge {
        maximum: usize,
        actual: usize,
    },
    InvalidSequence,
    InvalidSnapshotScope(u8),
    InvalidBoolean(u8),
    InvalidUtf8,
    InvalidStateHash {
        expected: u64,
        actual: u64,
    },
    InvalidDescriptor(&'static str),
    TickMismatch {
        expected: u64,
        actual: u64,
    },
    Remote(String),
}

impl ExternalSimulationError {
    pub fn bridge(message: impl Into<String>) -> Self {
        Self::Bridge(message.into())
    }

    fn into_simulation_error(self) -> SimulationError {
        SimulationError::new(self.to_string())
    }
}

impl fmt::Display for ExternalSimulationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bridge(message) => write!(formatter, "external simulation bridge failed: {message}"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported external simulation protocol version {version}")
            }
            Self::UnknownOperation(operation) => {
                write!(formatter, "unknown external simulation operation {operation}")
            }
            Self::UnexpectedStatus(status) => {
                write!(formatter, "unexpected external simulation response status {status}")
            }
            Self::UnexpectedOperation { expected, actual } => write!(
                formatter,
                "external simulation response operation {actual:?} does not match request {expected:?}"
            ),
            Self::UnexpectedResponse(operation) => write!(
                formatter,
                "external simulation response shape does not match operation {operation:?}"
            ),
            Self::IncorrectLength { expected, actual } => write!(
                formatter,
                "external simulation frame expected {expected} bytes, received {actual}"
            ),
            Self::PayloadTooLarge { maximum, actual } => write!(
                formatter,
                "external simulation payload size {actual} exceeds maximum {maximum}"
            ),
            Self::InvalidSequence => {
                write!(formatter, "external simulation command sequence must be non-zero")
            }
            Self::InvalidSnapshotScope(scope) => {
                write!(formatter, "invalid external simulation snapshot scope {scope}")
            }
            Self::InvalidBoolean(value) => {
                write!(formatter, "invalid external simulation boolean {value}")
            }
            Self::InvalidUtf8 => write!(formatter, "external simulation error payload is not UTF-8"),
            Self::InvalidStateHash { expected, actual } => write!(
                formatter,
                "external simulation snapshot hash mismatch: expected {expected:#018x}, got {actual:#018x}"
            ),
            Self::InvalidDescriptor(message) => {
                write!(formatter, "invalid external simulation descriptor: {message}")
            }
            Self::TickMismatch { expected, actual } => write!(
                formatter,
                "external simulation tick mismatch: expected {expected}, got {actual}"
            ),
            Self::Remote(message) => write!(formatter, "external simulation rejected operation: {message}"),
        }
    }
}

impl std::error::Error for ExternalSimulationError {}

pub trait ExternalSimulationBridge: Send + 'static {
    fn exchange(&self, request: &[u8]) -> Result<Vec<u8>, ExternalSimulationError>;
}

#[derive(Debug)]
pub struct ExternalSimulationAdapter<B> {
    bridge: B,
    descriptor: ExternalSimulationDescriptor,
    current_tick: u64,
}

impl<B: ExternalSimulationBridge> ExternalSimulationAdapter<B> {
    pub fn connect(bridge: B) -> Result<Self, ExternalSimulationError> {
        let request = encode_external_simulation_request(&ExternalSimulationRequest::Describe)?;
        let response = bridge.exchange(&request)?;
        let (operation, response) = decode_external_simulation_response(&response)?;
        if operation != ExternalSimulationOperation::Describe {
            return Err(ExternalSimulationError::UnexpectedOperation {
                expected: ExternalSimulationOperation::Describe,
                actual: operation,
            });
        }
        let descriptor = match response {
            ExternalSimulationResponse::Descriptor(descriptor) => descriptor,
            ExternalSimulationResponse::Rejected(message) => {
                return Err(ExternalSimulationError::Remote(message));
            }
            _ => return Err(ExternalSimulationError::UnexpectedResponse(operation)),
        };
        validate_descriptor(descriptor)?;
        Ok(Self {
            bridge,
            descriptor,
            current_tick: descriptor.current_tick,
        })
    }

    pub fn descriptor(&self) -> ExternalSimulationDescriptor {
        self.descriptor
    }

    fn exchange(
        &self,
        request: ExternalSimulationRequest,
    ) -> Result<ExternalSimulationResponse, ExternalSimulationError> {
        let expected = request.operation();
        let encoded = encode_external_simulation_request(&request)?;
        let response = self.bridge.exchange(&encoded)?;
        let (actual, response) = decode_external_simulation_response(&response)?;
        if actual != expected {
            return Err(ExternalSimulationError::UnexpectedOperation { expected, actual });
        }
        match response {
            ExternalSimulationResponse::Rejected(message) => {
                Err(ExternalSimulationError::Remote(message))
            }
            response => Ok(response),
        }
    }

    fn snapshot_request(
        &self,
        request: ExternalSimulationRequest,
    ) -> Result<SimulationSnapshot, SimulationError> {
        let operation = request.operation();
        let response = self
            .exchange(request)
            .map_err(ExternalSimulationError::into_simulation_error)?;
        let ExternalSimulationResponse::Snapshot(snapshot) = response else {
            return Err(
                ExternalSimulationError::UnexpectedResponse(operation).into_simulation_error()
            );
        };
        if snapshot.tick != self.current_tick {
            return Err(ExternalSimulationError::TickMismatch {
                expected: self.current_tick,
                actual: snapshot.tick,
            }
            .into_simulation_error());
        }
        Ok(snapshot)
    }
}

impl<B: ExternalSimulationBridge> GameSimulation for ExternalSimulationAdapter<B> {
    fn tick_hz(&self) -> u16 {
        self.descriptor.tick_hz
    }

    fn max_players(&self) -> usize {
        usize::from(self.descriptor.max_players)
    }

    fn current_tick(&self) -> u64 {
        self.current_tick
    }

    fn add_player(&mut self, player_id: PlayerId) -> Result<(), SimulationError> {
        let response = self
            .exchange(ExternalSimulationRequest::AddPlayer(player_id))
            .map_err(ExternalSimulationError::into_simulation_error)?;
        if response == ExternalSimulationResponse::Acknowledged {
            Ok(())
        } else {
            Err(
                ExternalSimulationError::UnexpectedResponse(ExternalSimulationOperation::AddPlayer)
                    .into_simulation_error(),
            )
        }
    }

    fn remove_player(&mut self, player_id: PlayerId) -> bool {
        self.try_remove_player(player_id).unwrap_or(false)
    }

    fn try_remove_player(&mut self, player_id: PlayerId) -> Result<bool, SimulationError> {
        let response = self
            .exchange(ExternalSimulationRequest::RemovePlayer(player_id))
            .map_err(ExternalSimulationError::into_simulation_error)?;
        let ExternalSimulationResponse::PlayerRemoved(removed) = response else {
            return Err(
                ExternalSimulationError::UnexpectedResponse(
                    ExternalSimulationOperation::RemovePlayer,
                )
                .into_simulation_error(),
            );
        };
        Ok(removed)
    }

    fn apply_command(
        &mut self,
        player_id: PlayerId,
        sequence: u32,
        payload: &[u8],
    ) -> Result<(), SimulationError> {
        let response = self
            .exchange(ExternalSimulationRequest::ApplyCommand {
                player_id,
                sequence,
                payload: payload.to_vec(),
            })
            .map_err(ExternalSimulationError::into_simulation_error)?;
        if response == ExternalSimulationResponse::Acknowledged {
            Ok(())
        } else {
            Err(
                ExternalSimulationError::UnexpectedResponse(
                    ExternalSimulationOperation::ApplyCommand,
                )
                .into_simulation_error(),
            )
        }
    }

    fn advance_tick(&mut self) -> Result<(), SimulationError> {
        let expected = self
            .current_tick
            .checked_add(1)
            .ok_or_else(|| SimulationError::new("external simulation tick exhausted u64"))?;
        let response = self
            .exchange(ExternalSimulationRequest::AdvanceTick)
            .map_err(ExternalSimulationError::into_simulation_error)?;
        let ExternalSimulationResponse::TickAdvanced(actual) = response else {
            return Err(
                ExternalSimulationError::UnexpectedResponse(
                    ExternalSimulationOperation::AdvanceTick,
                )
                .into_simulation_error(),
            );
        };
        if actual != expected {
            return Err(
                ExternalSimulationError::TickMismatch { expected, actual }.into_simulation_error()
            );
        }
        self.current_tick = actual;
        Ok(())
    }

    fn snapshot_scope(&self) -> SnapshotScope {
        self.descriptor.snapshot_scope
    }

    fn snapshot(&self) -> Result<SimulationSnapshot, SimulationError> {
        self.snapshot_request(ExternalSimulationRequest::Snapshot)
    }

    fn snapshot_for(&self, player_id: PlayerId) -> Result<SimulationSnapshot, SimulationError> {
        if self.descriptor.snapshot_scope != SnapshotScope::PlayerScoped {
            return Err(SimulationError::new(
                "external simulation exposes shared snapshots only",
            ));
        }
        self.snapshot_request(ExternalSimulationRequest::SnapshotFor(player_id))
    }
}

pub fn encode_external_simulation_request(
    request: &ExternalSimulationRequest,
) -> Result<Vec<u8>, ExternalSimulationError> {
    let mut output = Vec::with_capacity(COMMAND_REQUEST_FIXED_BYTES);
    output.push(EXTERNAL_SIMULATION_PROTOCOL_VERSION);
    output.push(request.operation().code());
    match request {
        ExternalSimulationRequest::Describe
        | ExternalSimulationRequest::AdvanceTick
        | ExternalSimulationRequest::Snapshot => {}
        ExternalSimulationRequest::AddPlayer(player_id)
        | ExternalSimulationRequest::RemovePlayer(player_id)
        | ExternalSimulationRequest::SnapshotFor(player_id) => {
            output.extend_from_slice(&player_id.to_be_bytes());
        }
        ExternalSimulationRequest::ApplyCommand {
            player_id,
            sequence,
            payload,
        } => {
            if *sequence == 0 {
                return Err(ExternalSimulationError::InvalidSequence);
            }
            if payload.len() > MAX_COMMAND_PAYLOAD_BYTES {
                return Err(ExternalSimulationError::PayloadTooLarge {
                    maximum: MAX_COMMAND_PAYLOAD_BYTES,
                    actual: payload.len(),
                });
            }
            let payload_len = u16::try_from(payload.len()).map_err(|_| {
                ExternalSimulationError::PayloadTooLarge {
                    maximum: MAX_COMMAND_PAYLOAD_BYTES,
                    actual: payload.len(),
                }
            })?;
            output.extend_from_slice(&player_id.to_be_bytes());
            output.extend_from_slice(&sequence.to_be_bytes());
            output.extend_from_slice(&payload_len.to_be_bytes());
            output.extend_from_slice(payload);
        }
    }
    Ok(output)
}

pub fn decode_external_simulation_request(
    bytes: &[u8],
) -> Result<ExternalSimulationRequest, ExternalSimulationError> {
    require_maximum_length(bytes)?;
    if bytes.len() < REQUEST_HEADER_BYTES {
        return Err(ExternalSimulationError::IncorrectLength {
            expected: REQUEST_HEADER_BYTES,
            actual: bytes.len(),
        });
    }
    require_version(bytes[0])?;
    let operation = ExternalSimulationOperation::from_code(bytes[1])?;
    match operation {
        ExternalSimulationOperation::Describe => {
            require_length(bytes, REQUEST_HEADER_BYTES)?;
            Ok(ExternalSimulationRequest::Describe)
        }
        ExternalSimulationOperation::AddPlayer
        | ExternalSimulationOperation::RemovePlayer
        | ExternalSimulationOperation::SnapshotFor => {
            require_length(bytes, REQUEST_HEADER_BYTES + 4)?;
            let player_id = u32::from_be_bytes(
                bytes[REQUEST_HEADER_BYTES..REQUEST_HEADER_BYTES + 4]
                    .try_into()
                    .map_err(|_| ExternalSimulationError::IncorrectLength {
                        expected: REQUEST_HEADER_BYTES + 4,
                        actual: bytes.len(),
                    })?,
            );
            Ok(match operation {
                ExternalSimulationOperation::AddPlayer => {
                    ExternalSimulationRequest::AddPlayer(player_id)
                }
                ExternalSimulationOperation::RemovePlayer => {
                    ExternalSimulationRequest::RemovePlayer(player_id)
                }
                ExternalSimulationOperation::SnapshotFor => {
                    ExternalSimulationRequest::SnapshotFor(player_id)
                }
                _ => return Err(ExternalSimulationError::UnexpectedResponse(operation)),
            })
        }
        ExternalSimulationOperation::ApplyCommand => {
            if bytes.len() < COMMAND_REQUEST_FIXED_BYTES {
                return Err(ExternalSimulationError::IncorrectLength {
                    expected: COMMAND_REQUEST_FIXED_BYTES,
                    actual: bytes.len(),
                });
            }
            let player_id = u32::from_be_bytes(
                bytes[2..6]
                    .try_into()
                    .map_err(|_| ExternalSimulationError::IncorrectLength {
                        expected: COMMAND_REQUEST_FIXED_BYTES,
                        actual: bytes.len(),
                    })?,
            );
            let sequence = u32::from_be_bytes(
                bytes[6..10]
                    .try_into()
                    .map_err(|_| ExternalSimulationError::IncorrectLength {
                        expected: COMMAND_REQUEST_FIXED_BYTES,
                        actual: bytes.len(),
                    })?,
            );
            if sequence == 0 {
                return Err(ExternalSimulationError::InvalidSequence);
            }
            let payload_len = usize::from(u16::from_be_bytes(
                bytes[10..12]
                    .try_into()
                    .map_err(|_| ExternalSimulationError::IncorrectLength {
                        expected: COMMAND_REQUEST_FIXED_BYTES,
                        actual: bytes.len(),
                    })?,
            ));
            if payload_len > MAX_COMMAND_PAYLOAD_BYTES {
                return Err(ExternalSimulationError::PayloadTooLarge {
                    maximum: MAX_COMMAND_PAYLOAD_BYTES,
                    actual: payload_len,
                });
            }
            require_length(bytes, COMMAND_REQUEST_FIXED_BYTES + payload_len)?;
            Ok(ExternalSimulationRequest::ApplyCommand {
                player_id,
                sequence,
                payload: bytes[COMMAND_REQUEST_FIXED_BYTES..].to_vec(),
            })
        }
        ExternalSimulationOperation::AdvanceTick => {
            require_length(bytes, REQUEST_HEADER_BYTES)?;
            Ok(ExternalSimulationRequest::AdvanceTick)
        }
        ExternalSimulationOperation::Snapshot => {
            require_length(bytes, REQUEST_HEADER_BYTES)?;
            Ok(ExternalSimulationRequest::Snapshot)
        }
    }
}

pub fn encode_external_simulation_response(
    operation: ExternalSimulationOperation,
    response: &ExternalSimulationResponse,
) -> Result<Vec<u8>, ExternalSimulationError> {
    let mut output = Vec::with_capacity(RESPONSE_HEADER_BYTES + 16);
    output.push(EXTERNAL_SIMULATION_PROTOCOL_VERSION);
    output.push(operation.code());

    if let ExternalSimulationResponse::Rejected(message) = response {
        if message.len() > MAX_EXTERNAL_SIMULATION_ERROR_BYTES {
            return Err(ExternalSimulationError::PayloadTooLarge {
                maximum: MAX_EXTERNAL_SIMULATION_ERROR_BYTES,
                actual: message.len(),
            });
        }
        output.push(STATUS_ERROR);
        output.extend_from_slice(message.as_bytes());
        return Ok(output);
    }

    output.push(STATUS_OK);
    match (operation, response) {
        (ExternalSimulationOperation::Describe, ExternalSimulationResponse::Descriptor(descriptor)) => {
            validate_descriptor(*descriptor)?;
            output.extend_from_slice(&descriptor.tick_hz.to_be_bytes());
            output.extend_from_slice(&descriptor.max_players.to_be_bytes());
            output.extend_from_slice(&descriptor.current_tick.to_be_bytes());
            output.push(encode_snapshot_scope(descriptor.snapshot_scope));
        }
        (
            ExternalSimulationOperation::AddPlayer | ExternalSimulationOperation::ApplyCommand,
            ExternalSimulationResponse::Acknowledged,
        ) => {}
        (
            ExternalSimulationOperation::RemovePlayer,
            ExternalSimulationResponse::PlayerRemoved(removed),
        ) => output.push(u8::from(*removed)),
        (
            ExternalSimulationOperation::AdvanceTick,
            ExternalSimulationResponse::TickAdvanced(tick),
        ) => output.extend_from_slice(&tick.to_be_bytes()),
        (
            ExternalSimulationOperation::Snapshot | ExternalSimulationOperation::SnapshotFor,
            ExternalSimulationResponse::Snapshot(snapshot),
        ) => encode_snapshot_body(snapshot, &mut output)?,
        _ => return Err(ExternalSimulationError::UnexpectedResponse(operation)),
    }
    Ok(output)
}

pub fn decode_external_simulation_response(
    bytes: &[u8],
) -> Result<(ExternalSimulationOperation, ExternalSimulationResponse), ExternalSimulationError> {
    require_maximum_length(bytes)?;
    if bytes.len() < RESPONSE_HEADER_BYTES {
        return Err(ExternalSimulationError::IncorrectLength {
            expected: RESPONSE_HEADER_BYTES,
            actual: bytes.len(),
        });
    }
    require_version(bytes[0])?;
    let operation = ExternalSimulationOperation::from_code(bytes[1])?;
    match bytes[2] {
        STATUS_ERROR => {
            let message = &bytes[RESPONSE_HEADER_BYTES..];
            if message.len() > MAX_EXTERNAL_SIMULATION_ERROR_BYTES {
                return Err(ExternalSimulationError::PayloadTooLarge {
                    maximum: MAX_EXTERNAL_SIMULATION_ERROR_BYTES,
                    actual: message.len(),
                });
            }
            let message = std::str::from_utf8(message)
                .map_err(|_| ExternalSimulationError::InvalidUtf8)?
                .to_owned();
            Ok((operation, ExternalSimulationResponse::Rejected(message)))
        }
        STATUS_OK => decode_success_response(operation, bytes),
        status => Err(ExternalSimulationError::UnexpectedStatus(status)),
    }
}

fn decode_success_response(
    operation: ExternalSimulationOperation,
    bytes: &[u8],
) -> Result<(ExternalSimulationOperation, ExternalSimulationResponse), ExternalSimulationError> {
    let response = match operation {
        ExternalSimulationOperation::Describe => {
            const LENGTH: usize = RESPONSE_HEADER_BYTES + 2 + 2 + 8 + 1;
            require_length(bytes, LENGTH)?;
            let tick_hz = u16::from_be_bytes(
                bytes[3..5]
                    .try_into()
                    .map_err(|_| ExternalSimulationError::IncorrectLength {
                        expected: LENGTH,
                        actual: bytes.len(),
                    })?,
            );
            let max_players = u16::from_be_bytes(
                bytes[5..7]
                    .try_into()
                    .map_err(|_| ExternalSimulationError::IncorrectLength {
                        expected: LENGTH,
                        actual: bytes.len(),
                    })?,
            );
            let current_tick = u64::from_be_bytes(
                bytes[7..15]
                    .try_into()
                    .map_err(|_| ExternalSimulationError::IncorrectLength {
                        expected: LENGTH,
                        actual: bytes.len(),
                    })?,
            );
            let descriptor = ExternalSimulationDescriptor {
                tick_hz,
                max_players,
                current_tick,
                snapshot_scope: decode_snapshot_scope(bytes[15])?,
            };
            validate_descriptor(descriptor)?;
            ExternalSimulationResponse::Descriptor(descriptor)
        }
        ExternalSimulationOperation::AddPlayer | ExternalSimulationOperation::ApplyCommand => {
            require_length(bytes, RESPONSE_HEADER_BYTES)?;
            ExternalSimulationResponse::Acknowledged
        }
        ExternalSimulationOperation::RemovePlayer => {
            require_length(bytes, RESPONSE_HEADER_BYTES + 1)?;
            let removed = match bytes[3] {
                0 => false,
                1 => true,
                value => return Err(ExternalSimulationError::InvalidBoolean(value)),
            };
            ExternalSimulationResponse::PlayerRemoved(removed)
        }
        ExternalSimulationOperation::AdvanceTick => {
            const LENGTH: usize = RESPONSE_HEADER_BYTES + 8;
            require_length(bytes, LENGTH)?;
            let tick = u64::from_be_bytes(
                bytes[RESPONSE_HEADER_BYTES..LENGTH]
                    .try_into()
                    .map_err(|_| ExternalSimulationError::IncorrectLength {
                        expected: LENGTH,
                        actual: bytes.len(),
                    })?,
            );
            ExternalSimulationResponse::TickAdvanced(tick)
        }
        ExternalSimulationOperation::Snapshot | ExternalSimulationOperation::SnapshotFor => {
            ExternalSimulationResponse::Snapshot(decode_snapshot_body(bytes)?)
        }
    };
    Ok((operation, response))
}

fn encode_snapshot_body(
    snapshot: &SimulationSnapshot,
    output: &mut Vec<u8>,
) -> Result<(), ExternalSimulationError> {
    if snapshot.payload.len() > MAX_SNAPSHOT_PAYLOAD_BYTES {
        return Err(ExternalSimulationError::PayloadTooLarge {
            maximum: MAX_SNAPSHOT_PAYLOAD_BYTES,
            actual: snapshot.payload.len(),
        });
    }
    let expected = snapshot_hash(snapshot.tick, &snapshot.payload);
    if snapshot.state_hash != expected {
        return Err(ExternalSimulationError::InvalidStateHash {
            expected,
            actual: snapshot.state_hash,
        });
    }
    let payload_len = u16::try_from(snapshot.payload.len()).map_err(|_| {
        ExternalSimulationError::PayloadTooLarge {
            maximum: MAX_SNAPSHOT_PAYLOAD_BYTES,
            actual: snapshot.payload.len(),
        }
    })?;
    output.extend_from_slice(&snapshot.tick.to_be_bytes());
    output.extend_from_slice(&snapshot.state_hash.to_be_bytes());
    output.extend_from_slice(&payload_len.to_be_bytes());
    output.extend_from_slice(&snapshot.payload);
    Ok(())
}

fn decode_snapshot_body(bytes: &[u8]) -> Result<SimulationSnapshot, ExternalSimulationError> {
    if bytes.len() < SNAPSHOT_RESPONSE_FIXED_BYTES {
        return Err(ExternalSimulationError::IncorrectLength {
            expected: SNAPSHOT_RESPONSE_FIXED_BYTES,
            actual: bytes.len(),
        });
    }
    let tick = u64::from_be_bytes(
        bytes[3..11]
            .try_into()
            .map_err(|_| ExternalSimulationError::IncorrectLength {
                expected: SNAPSHOT_RESPONSE_FIXED_BYTES,
                actual: bytes.len(),
            })?,
    );
    let state_hash = u64::from_be_bytes(
        bytes[11..19]
            .try_into()
            .map_err(|_| ExternalSimulationError::IncorrectLength {
                expected: SNAPSHOT_RESPONSE_FIXED_BYTES,
                actual: bytes.len(),
            })?,
    );
    let payload_len = usize::from(u16::from_be_bytes(
        bytes[19..21]
            .try_into()
            .map_err(|_| ExternalSimulationError::IncorrectLength {
                expected: SNAPSHOT_RESPONSE_FIXED_BYTES,
                actual: bytes.len(),
            })?,
    ));
    if payload_len > MAX_SNAPSHOT_PAYLOAD_BYTES {
        return Err(ExternalSimulationError::PayloadTooLarge {
            maximum: MAX_SNAPSHOT_PAYLOAD_BYTES,
            actual: payload_len,
        });
    }
    require_length(bytes, SNAPSHOT_RESPONSE_FIXED_BYTES + payload_len)?;
    let payload = bytes[SNAPSHOT_RESPONSE_FIXED_BYTES..].to_vec();
    let expected = snapshot_hash(tick, &payload);
    if state_hash != expected {
        return Err(ExternalSimulationError::InvalidStateHash {
            expected,
            actual: state_hash,
        });
    }
    Ok(SimulationSnapshot {
        tick,
        state_hash,
        payload,
    })
}

fn encode_snapshot_scope(scope: SnapshotScope) -> u8 {
    match scope {
        SnapshotScope::Shared => 0,
        SnapshotScope::PlayerScoped => 1,
    }
}

fn decode_snapshot_scope(scope: u8) -> Result<SnapshotScope, ExternalSimulationError> {
    match scope {
        0 => Ok(SnapshotScope::Shared),
        1 => Ok(SnapshotScope::PlayerScoped),
        value => Err(ExternalSimulationError::InvalidSnapshotScope(value)),
    }
}

fn validate_descriptor(
    descriptor: ExternalSimulationDescriptor,
) -> Result<(), ExternalSimulationError> {
    if descriptor.tick_hz == 0 {
        return Err(ExternalSimulationError::InvalidDescriptor(
            "tick rate must be non-zero",
        ));
    }
    if descriptor.max_players == 0 {
        return Err(ExternalSimulationError::InvalidDescriptor(
            "player capacity must be non-zero",
        ));
    }
    Ok(())
}

fn require_version(version: u8) -> Result<(), ExternalSimulationError> {
    if version == EXTERNAL_SIMULATION_PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(ExternalSimulationError::UnsupportedVersion(version))
    }
}

fn require_length(bytes: &[u8], expected: usize) -> Result<(), ExternalSimulationError> {
    if bytes.len() == expected {
        Ok(())
    } else {
        Err(ExternalSimulationError::IncorrectLength {
            expected,
            actual: bytes.len(),
        })
    }
}

fn require_maximum_length(bytes: &[u8]) -> Result<(), ExternalSimulationError> {
    if bytes.len() <= MAX_EXTERNAL_SIMULATION_FRAME_BYTES {
        Ok(())
    } else {
        Err(ExternalSimulationError::PayloadTooLarge {
            maximum: MAX_EXTERNAL_SIMULATION_FRAME_BYTES,
            actual: bytes.len(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CommandOutcome, MatchRuntime, RECONNECT_TOKEN_BYTES, ReconnectToken, verify_replay,
    };
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    #[derive(Debug, Default)]
    struct ForeignGame {
        tick: u64,
        players: BTreeMap<PlayerId, u8>,
        command_calls: usize,
        reject_removal: bool,
    }

    #[derive(Clone, Debug)]
    struct LoopbackBridge {
        state: Arc<Mutex<ForeignGame>>,
        snapshot_scope: SnapshotScope,
    }

    impl LoopbackBridge {
        fn shared() -> Self {
            Self {
                state: Arc::new(Mutex::new(ForeignGame::default())),
                snapshot_scope: SnapshotScope::Shared,
            }
        }

        fn player_scoped() -> Self {
            Self {
                state: Arc::new(Mutex::new(ForeignGame::default())),
                snapshot_scope: SnapshotScope::PlayerScoped,
            }
        }

        fn with_rejected_removal(self) -> Self {
            self.state
                .lock()
                .expect("test bridge mutex")
                .reject_removal = true;
            self
        }

        fn command_calls(&self) -> usize {
            self.state.lock().expect("test bridge mutex").command_calls
        }
    }

    impl ExternalSimulationBridge for LoopbackBridge {
        fn exchange(&self, request: &[u8]) -> Result<Vec<u8>, ExternalSimulationError> {
            let request = decode_external_simulation_request(request)?;
            let operation = request.operation();
            let mut game = self
                .state
                .lock()
                .map_err(|_| ExternalSimulationError::bridge("test bridge mutex poisoned"))?;
            let response = match request {
                ExternalSimulationRequest::Describe => {
                    ExternalSimulationResponse::Descriptor(ExternalSimulationDescriptor {
                        tick_hz: 20,
                        max_players: 2,
                        current_tick: game.tick,
                        snapshot_scope: self.snapshot_scope,
                    })
                }
                ExternalSimulationRequest::AddPlayer(player_id) => {
                    if game.players.insert(player_id, 0).is_some() {
                        ExternalSimulationResponse::Rejected("player already exists".to_owned())
                    } else {
                        ExternalSimulationResponse::Acknowledged
                    }
                }
                ExternalSimulationRequest::RemovePlayer(player_id) => {
                    if game.reject_removal {
                        ExternalSimulationResponse::Rejected("removal unavailable".to_owned())
                    } else {
                        ExternalSimulationResponse::PlayerRemoved(
                            game.players.remove(&player_id).is_some(),
                        )
                    }
                }
                ExternalSimulationRequest::ApplyCommand {
                    player_id,
                    sequence: _,
                    payload,
                } => {
                    game.command_calls += 1;
                    match (game.players.get_mut(&player_id), payload.as_slice()) {
                        (Some(value), [next]) => {
                            *value = *next;
                            ExternalSimulationResponse::Acknowledged
                        }
                        (None, _) => {
                            ExternalSimulationResponse::Rejected("unknown player".to_owned())
                        }
                        (_, _) => ExternalSimulationResponse::Rejected(
                            "command must contain exactly one byte".to_owned(),
                        ),
                    }
                }
                ExternalSimulationRequest::AdvanceTick => {
                    let Some(next) = game.tick.checked_add(1) else {
                        return encode_external_simulation_response(
                            operation,
                            &ExternalSimulationResponse::Rejected("tick exhausted".to_owned()),
                        );
                    };
                    game.tick = next;
                    ExternalSimulationResponse::TickAdvanced(next)
                }
                ExternalSimulationRequest::Snapshot => {
                    ExternalSimulationResponse::Snapshot(canonical_snapshot(&game))
                }
                ExternalSimulationRequest::SnapshotFor(player_id) => {
                    if self.snapshot_scope != SnapshotScope::PlayerScoped {
                        ExternalSimulationResponse::Rejected(
                            "player projection unavailable".to_owned(),
                        )
                    } else if let Some(value) = game.players.get(&player_id) {
                        ExternalSimulationResponse::Snapshot(SimulationSnapshot::new(
                            game.tick,
                            vec![*value],
                        ))
                    } else {
                        ExternalSimulationResponse::Rejected("unknown player".to_owned())
                    }
                }
            };
            encode_external_simulation_response(operation, &response)
        }
    }

    fn canonical_snapshot(game: &ForeignGame) -> SimulationSnapshot {
        let mut payload = Vec::with_capacity(game.players.len() * 5);
        for (player_id, value) in &game.players {
            payload.extend_from_slice(&player_id.to_be_bytes());
            payload.push(*value);
        }
        SimulationSnapshot::new(game.tick, payload)
    }

    #[test]
    fn external_wire_fixture_is_stable() {
        assert_eq!(
            encode_external_simulation_request(&ExternalSimulationRequest::ApplyCommand {
                player_id: 1,
                sequence: 2,
                payload: vec![0xaa],
            })
            .unwrap(),
            vec![1, 4, 0, 0, 0, 1, 0, 0, 0, 2, 0, 1, 0xaa]
        );

        let descriptor = ExternalSimulationResponse::Descriptor(ExternalSimulationDescriptor {
            tick_hz: 20,
            max_players: 2,
            current_tick: 7,
            snapshot_scope: SnapshotScope::PlayerScoped,
        });
        assert_eq!(
            encode_external_simulation_response(ExternalSimulationOperation::Describe, &descriptor)
                .unwrap(),
            vec![1, 1, 0, 0, 20, 0, 2, 0, 0, 0, 0, 0, 0, 0, 7, 1]
        );
    }

    #[test]
    fn runtime_keeps_identity_and_sequence_authority_outside_external_game_logic() {
        let bridge = LoopbackBridge::shared();
        let observer = bridge.clone();
        let simulation = ExternalSimulationAdapter::connect(bridge).unwrap();
        let mut runtime = MatchRuntime::new_with_replay_capture(simulation, 5);
        let lease = runtime
            .admit(ReconnectToken([1; RECONNECT_TOKEN_BYTES]))
            .unwrap();

        assert_eq!(
            runtime
                .submit_command(lease.player_id, lease.connection_epoch, 1, &[7])
                .unwrap(),
            CommandOutcome::Applied
        );
        assert_eq!(
            runtime
                .submit_command(lease.player_id, lease.connection_epoch, 1, &[9])
                .unwrap(),
            CommandOutcome::IgnoredStale
        );
        let snapshot = runtime.advance_tick().unwrap();
        assert_eq!(observer.command_calls(), 1);

        let replay = runtime.replay_log().unwrap().clone();
        let verification = verify_replay(
            ExternalSimulationAdapter::connect(LoopbackBridge::shared()).unwrap(),
            &replay,
        )
        .unwrap();
        assert_eq!(verification.final_snapshot, snapshot);
    }

    #[test]
    fn player_scoped_projection_stays_separate_from_canonical_snapshot() {
        let mut simulation =
            ExternalSimulationAdapter::connect(LoopbackBridge::player_scoped()).unwrap();
        simulation.add_player(1).unwrap();
        simulation.add_player(2).unwrap();
        simulation.apply_command(1, 1, &[7]).unwrap();
        simulation.apply_command(2, 1, &[9]).unwrap();

        let canonical = simulation.snapshot().unwrap();
        let first = simulation.snapshot_for(1).unwrap();
        let second = simulation.snapshot_for(2).unwrap();

        assert_eq!(canonical.payload.len(), 10);
        assert_eq!(first.payload, vec![7]);
        assert_eq!(second.payload, vec![9]);
    }

    #[test]
    fn failed_external_removal_does_not_discard_runtime_session_state() {
        let simulation =
            ExternalSimulationAdapter::connect(LoopbackBridge::shared().with_rejected_removal())
                .unwrap();
        let mut runtime = MatchRuntime::new_with_replay_capture(simulation, 0);
        let lease = runtime
            .admit(ReconnectToken([1; RECONNECT_TOKEN_BYTES]))
            .unwrap();
        assert!(runtime.disconnect(lease.player_id, lease.connection_epoch));

        runtime.advance_tick().unwrap();
        let error = runtime.advance_tick().unwrap_err();

        assert!(error.to_string().contains("removal unavailable"));
        assert_eq!(runtime.slot_count(), 1);
        assert!(
            !runtime
                .replay_log()
                .unwrap()
                .records()
                .iter()
                .any(|record| matches!(record, crate::ReplayRecord::PlayerRemoved { .. }))
        );
    }

    #[test]
    fn mismatched_response_operation_fails_closed() {
        #[derive(Debug)]
        struct WrongBridge;

        impl ExternalSimulationBridge for WrongBridge {
            fn exchange(&self, _request: &[u8]) -> Result<Vec<u8>, ExternalSimulationError> {
                encode_external_simulation_response(
                    ExternalSimulationOperation::AddPlayer,
                    &ExternalSimulationResponse::Acknowledged,
                )
            }
        }

        assert_eq!(
            ExternalSimulationAdapter::connect(WrongBridge).unwrap_err(),
            ExternalSimulationError::UnexpectedOperation {
                expected: ExternalSimulationOperation::Describe,
                actual: ExternalSimulationOperation::AddPlayer,
            }
        );
    }
}

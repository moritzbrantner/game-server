//! Server-authoritative match hosting over WebTransport.
//!
//! The default `transport` feature is the native host. Without it
//! (`default-features = false`) the crate is the client wire layer — frame
//! codecs, snapshot reassembly, browser routes and control frames — and builds
//! for `wasm32-unknown-unknown`.

// Client wire layer: std + url only, builds for wasm32-unknown-unknown.
pub mod browser;
pub mod control;
pub mod external_simulation;
pub mod match_id;
pub mod protocol;
pub mod reassembly;
pub mod session;
pub mod simulation;
pub mod world;

// Native host: tokio, wtransport and ring.
#[cfg(feature = "transport")]
mod connection;
#[cfg(feature = "transport")]
pub mod host;
#[cfg(feature = "transport")]
pub mod host_recovery;
#[cfg(feature = "transport")]
pub mod host_status;
#[cfg(feature = "transport")]
pub mod host_transport;
#[cfg(feature = "transport")]
pub mod live_host;
#[cfg(feature = "transport")]
pub mod persistence;
#[cfg(feature = "transport")]
pub mod recovery;
#[cfg(feature = "transport")]
pub mod replay;
#[cfg(feature = "transport")]
mod replay_baseline;
#[cfg(feature = "transport")]
pub mod runtime;
#[cfg(feature = "transport")]
pub mod transport;

pub use browser::{
    BROWSER_MATCH_SEGMENT, BROWSER_PROTOCOL_CONTRACT, BROWSER_RECONNECT_SEGMENT,
    BROWSER_ROUTE_VERSION, BrowserAdmission, BrowserOriginAllowlist, BrowserOriginError,
    BrowserProtocolContract, BrowserRouteError, BrowserRoutePrefix, BrowserSessionRoute,
    MAX_BROWSER_ORIGIN_BYTES, MAX_BROWSER_ORIGINS,
};
pub use control::{
    CONTROL_FORMAT_VERSION, CONTROL_HEADER_BYTES, ControlContext, ControlRequest, ControlResponse,
    ControlService, ControlServiceError, ControlWireError, MAX_CONTROL_FRAME_BYTES,
    MAX_CONTROL_PAYLOAD_BYTES, MatchControlService, RejectControlService,
    RejectMatchControlService, decode_control_request, decode_control_response,
    encode_control_request, encode_control_response,
};
pub use external_simulation::{
    EXTERNAL_SIMULATION_PROTOCOL_CONTRACT, EXTERNAL_SIMULATION_PROTOCOL_VERSION,
    ExternalSimulationAdapter, ExternalSimulationBridge, ExternalSimulationDescriptor,
    ExternalSimulationError, ExternalSimulationOperation, ExternalSimulationProtocolContract,
    ExternalSimulationRequest, ExternalSimulationResponse, MAX_EXTERNAL_SIMULATION_ERROR_BYTES,
    MAX_EXTERNAL_SIMULATION_FRAME_BYTES, decode_external_simulation_request,
    decode_external_simulation_response, encode_external_simulation_request,
    encode_external_simulation_response,
};
pub use match_id::{MAX_MATCH_ID_BYTES, MatchId, MatchIdError};
pub use protocol::{
    COMMAND_HEADER_BYTES, COMMAND_LANE_FORMAT_VERSION, COMMAND_LANE_HEADER_BYTES,
    COMMAND_REJECTION_HEADER_BYTES, CommandFrame, CommandLane, CommandRejectionFrame,
    MAX_COMMAND_LANES_PER_CONNECTION, MAX_COMMAND_PAYLOAD_BYTES,
    MAX_COMMAND_REJECTION_PAYLOAD_BYTES, MAX_SNAPSHOT_FRAGMENTS, MAX_SNAPSHOT_FRAME_BYTES,
    MAX_SNAPSHOT_PAYLOAD_BYTES, MIN_FRAGMENTED_DATAGRAM_BYTES, PROTOCOL_VERSION, PlayerId,
    ProtocolError, RECONNECT_TOKEN_BYTES, RELIABLE_COMMAND_ACK_BYTES, ReliableCommandAck,
    ReliableCommandOutcome, SNAPSHOT_FRAGMENT_HEADER_BYTES, SNAPSHOT_HEADER_BYTES,
    SnapshotDatagram, SnapshotFragment, SnapshotFrame, WELCOME_BYTES, Welcome, decode_command,
    decode_command_lane_header, decode_command_rejection, decode_reliable_command_ack,
    decode_snapshot, decode_snapshot_datagram, decode_snapshot_fragment, decode_welcome,
    encode_command, encode_command_lane_header, encode_command_rejection,
    encode_reliable_command_ack, encode_snapshot, encode_snapshot_fragments, encode_welcome,
    snapshot_hash,
};
pub use reassembly::{
    SNAPSHOT_REASSEMBLY_MAX_BUFFERED_BYTES, SNAPSHOT_REASSEMBLY_MAX_IDLE_DATAGRAMS,
    SNAPSHOT_REASSEMBLY_MAX_PENDING, SnapshotReassembler, SnapshotReassemblyStats,
};
pub use session::{
    DEFAULT_MAX_PLAYERS, DEFAULT_RECONNECT_GRACE_TICKS, ReconnectToken, RecoverableSession,
    SessionError, SessionLease, SessionRecoveryError, SessionRecoverySnapshot, SessionRegistry,
};
pub use simulation::{
    GameSimulation, PlayerSnapshotContext, SimulationError, SimulationSnapshot, SnapshotScope,
};
pub use world::{
    DEMO_MAX_PLAYERS, DEMO_TICK_HZ, DemoSimulation, DemoSnapshotPlayer, WorldError,
    decode_demo_snapshot, encode_demo_command,
};

#[cfg(feature = "transport")]
pub use host::{HostError, HostStatus, MatchHost, MatchStatus, PlacementFailure};
#[cfg(feature = "transport")]
pub use host_recovery::{
    HOST_RECOVERY_BUNDLE_VERSION, MatchHostRecoveryConfig, MatchHostRecoveryError,
    PreparedLiveMatchHost, PreparedMatchHost, prepare_live_match_host_for_recovery,
    prepare_match_host_for_recovery,
};
#[cfg(feature = "transport")]
pub use host_status::{
    DEFAULT_HOST_STATUS_PORT, HOST_STATUS_CONTRACT_VERSION, HostStatusServerError,
    MatchHostStatusConfig, serve_live_match_host_with_status_and_control_and_shutdown,
    serve_match_host_with_status_and_control_and_shutdown,
    serve_prepared_live_match_host_with_status_and_control_and_shutdown,
    serve_prepared_match_host_with_status_and_control_and_shutdown,
};
#[cfg(feature = "transport")]
pub use host_transport::{
    MatchHostTransportError, MatchHostWebTransportConfig,
    serve_live_match_host_with_control_and_shutdown, serve_match_host,
    serve_match_host_with_control, serve_match_host_with_control_and_shutdown,
    serve_match_host_with_shutdown,
};
#[cfg(feature = "transport")]
pub use live_host::{ColdMatchResolver, LiveHostError, LiveMatchHost, LivePlacementFailure};
#[cfg(feature = "transport")]
pub use persistence::{
    PersistenceError, SnapshotCommitBatch, SnapshotCommitFuture, SnapshotCommitter,
    SnapshotPersistence,
};
#[cfg(feature = "transport")]
pub use recovery::{
    MAX_RECOVERY_IMAGE_BYTES, RECOVERY_FORMAT_VERSION, RecoveryError, RecoveryImage,
};
#[cfg(feature = "transport")]
pub use replay::{
    REPLAY_FORMAT_VERSION, ReplayError, ReplayLog, ReplayRecord, ReplayVerification, verify_replay,
};
#[cfg(feature = "transport")]
pub use replay_baseline::{ReplayBaseline, ReplayPlayerWatermarks};
#[cfg(feature = "transport")]
pub use runtime::{CommandOutcome, MatchRuntime, RuntimeError, RuntimeRecoveryError};
#[cfg(feature = "transport")]
pub use transport::{
    TransportError, WebTransportConfig, serve, serve_with_control, serve_with_control_and_shutdown,
    serve_with_shutdown,
};

#[cfg(all(test, feature = "transport"))]
mod benchmarks;

#[cfg(all(test, feature = "transport"))]
mod test_support;

#[cfg(all(test, feature = "transport"))]
mod command_lane_acceptance;

#[cfg(all(test, feature = "transport"))]
mod welcome_handshake_acceptance;

#[cfg(all(test, feature = "transport"))]
mod host_input_tests;

#[cfg(all(test, feature = "transport"))]
mod checkpoint_tests;

#[cfg(all(test, feature = "transport"))]
mod persistence_tests;

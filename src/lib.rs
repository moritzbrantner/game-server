pub mod browser;
pub mod control;
pub mod host;
#[cfg(feature = "physics")]
pub mod physics;
pub mod protocol;
pub mod recovery;
pub mod replay;
pub mod runtime;
pub mod session;
pub mod simulation;
pub mod transport;
pub mod world;

pub use browser::{
    BROWSER_MATCH_SEGMENT, BROWSER_PROTOCOL_CONTRACT, BROWSER_RECONNECT_SEGMENT,
    BROWSER_ROUTE_VERSION, BrowserAdmission, BrowserProtocolContract, BrowserRouteError,
    BrowserRoutePrefix, BrowserSessionRoute,
};
pub use control::{
    CONTROL_FORMAT_VERSION, CONTROL_HEADER_BYTES, ControlContext, ControlRequest, ControlResponse,
    ControlService, ControlServiceError, ControlWireError, MAX_CONTROL_FRAME_BYTES,
    MAX_CONTROL_PAYLOAD_BYTES, RejectControlService, decode_control_request,
    decode_control_response, encode_control_request, encode_control_response,
};
pub use host::{
    HostError, HostStatus, MAX_MATCH_ID_BYTES, MatchHost, MatchId, MatchIdError, MatchStatus,
    PlacementFailure,
};
#[cfg(feature = "physics")]
pub use physics::{PINNED_PHYSICS_ENGINE_REVISION, PhysicsWorldAdapter};
pub use protocol::{
    COMMAND_HEADER_BYTES, CommandFrame, MAX_COMMAND_PAYLOAD_BYTES, MAX_SNAPSHOT_PAYLOAD_BYTES,
    PROTOCOL_VERSION, PlayerId, ProtocolError, RECONNECT_TOKEN_BYTES, SNAPSHOT_HEADER_BYTES,
    SnapshotFrame, WELCOME_BYTES, Welcome, decode_command, decode_snapshot, decode_welcome,
    encode_command, encode_snapshot, encode_welcome, snapshot_hash,
};
pub use recovery::{
    MAX_RECOVERY_IMAGE_BYTES, RECOVERY_FORMAT_VERSION, RecoveryError, RecoveryImage,
};
pub use replay::{
    REPLAY_FORMAT_VERSION, ReplayError, ReplayLog, ReplayRecord, ReplayVerification, verify_replay,
};
pub use runtime::{CommandOutcome, MatchRuntime, RuntimeError, RuntimeRecoveryError};
pub use session::{
    DEFAULT_MAX_PLAYERS, DEFAULT_RECONNECT_GRACE_TICKS, ReconnectToken, RecoverableSession,
    SessionError, SessionLease, SessionRecoveryError, SessionRecoverySnapshot, SessionRegistry,
};
pub use simulation::{GameSimulation, SimulationError, SimulationSnapshot};
pub use transport::{
    TransportError, WebTransportConfig, serve, serve_browser_match,
    serve_browser_match_with_control, serve_browser_match_with_control_and_shutdown,
    serve_browser_match_with_shutdown, serve_with_control, serve_with_control_and_shutdown,
    serve_with_shutdown,
};
pub use world::{
    DEMO_MAX_PLAYERS, DEMO_TICK_HZ, DemoSimulation, DemoSnapshotPlayer, WorldError,
    decode_demo_snapshot, encode_demo_command,
};

#[cfg(feature = "physics")]
pub mod physics;
pub mod protocol;
pub mod runtime;
pub mod session;
pub mod simulation;
pub mod transport;
pub mod world;

#[cfg(feature = "physics")]
pub use physics::{PINNED_PHYSICS_ENGINE_REVISION, PhysicsWorldAdapter};
pub use protocol::{
    CommandFrame, MAX_COMMAND_PAYLOAD_BYTES, MAX_SNAPSHOT_PAYLOAD_BYTES, PlayerId, ProtocolError,
    RECONNECT_TOKEN_BYTES, SnapshotFrame, Welcome, decode_command, decode_snapshot, decode_welcome,
    encode_command, encode_snapshot, encode_welcome, snapshot_hash,
};
pub use runtime::{CommandOutcome, MatchRuntime, RuntimeError};
pub use session::{
    DEFAULT_MAX_PLAYERS, DEFAULT_RECONNECT_GRACE_TICKS, ReconnectToken, SessionError, SessionLease,
    SessionRegistry,
};
pub use simulation::{GameSimulation, SimulationError, SimulationSnapshot};
pub use transport::{TransportError, WebTransportConfig, serve};
pub use world::{
    DEMO_MAX_PLAYERS, DEMO_TICK_HZ, DemoSimulation, DemoSnapshotPlayer, WorldError,
    decode_demo_snapshot, encode_demo_command,
};

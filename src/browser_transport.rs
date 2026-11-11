//! Browser WebTransport client adapter (`browser-transport` feature).
//!
//! [`BrowserSession`] opens a session at a browser route
//! ([`crate::BrowserRoutePrefix`]), reads the welcome from the first incoming
//! unidirectional stream, pumps snapshot datagrams through a
//! [`SnapshotReassembler`], collects command rejections from later
//! unidirectional streams, sends command datagrams, and runs reliable control
//! exchanges over bidirectional streams.
//!
//! The browser's `WebTransport` API is bound directly with `wasm-bindgen`, so
//! consumers do not need `--cfg=web_sys_unstable_apis`. The adapter only works
//! in a browser (`wasm32-unknown-unknown`); on other targets it compiles but
//! every call into JavaScript panics.
//!
//! Reconnecting is a new [`BrowserSession::connect`] at
//! [`crate::BrowserRoutePrefix::reconnect_path`] with the token from the last
//! [`Welcome`], before its reconnect grace runs out.

use crate::control::{ControlResponse, MAX_CONTROL_FRAME_BYTES, decode_control_response};
use crate::control::{ControlWireError, encode_control_request};
use crate::protocol::{
    COMMAND_REJECTION_HEADER_BYTES, CommandRejectionFrame, MAX_COMMAND_REJECTION_PAYLOAD_BYTES,
    ProtocolError, SnapshotFrame, WELCOME_BYTES, Welcome, decode_command_rejection, decode_welcome,
    encode_command,
};
use crate::reassembly::SnapshotReassembler;
use js_sys::{Array, Object, Promise, Reflect, Uint8Array};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::rc::Rc;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::{JsFuture, spawn_local};

/// Reassembled snapshots kept until [`BrowserSession::take_snapshots`]; older ones are dropped.
pub const BROWSER_SNAPSHOT_QUEUE_LIMIT: usize = 64;
/// Command rejections kept until [`BrowserSession::take_rejections`]; older ones are dropped.
pub const BROWSER_REJECTION_QUEUE_LIMIT: usize = 64;
/// One byte past the welcome proves trailing data without reading an unbounded stream.
const MAX_WELCOME_STREAM_BYTES: usize = WELCOME_BYTES + 1;
const MAX_REJECTION_STREAM_BYTES: usize =
    COMMAND_REJECTION_HEADER_BYTES + MAX_COMMAND_REJECTION_PAYLOAD_BYTES + 1;
const MAX_CONTROL_STREAM_BYTES: usize = MAX_CONTROL_FRAME_BYTES + 1;
/// Close code the client sends when it leaves on purpose.
pub const BROWSER_CLIENT_CLOSE_CODE: u32 = 0;

/// Options for [`BrowserSession::connect`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BrowserConnectOptions {
    /// SHA-256 of a self-signed development certificate (ECDSA, valid at most
    /// 14 days, as WebTransport requires). `None` needs a publicly trusted certificate.
    pub server_certificate_hash: Option<[u8; 32]>,
}

/// How a session ended.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BrowserSessionEnd {
    /// Closed cleanly by either side with an application close code.
    Closed { code: u32, reason: String },
    /// The connection failed (handshake, network loss, or abrupt close).
    Lost(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BrowserTransportError {
    /// This browser has no `WebTransport`.
    Unsupported,
    /// A JavaScript call or promise failed; the message is the JavaScript error.
    Js(String),
    /// The host closed the session before its welcome.
    MissingWelcome,
    /// A stream exceeded its protocol limit.
    StreamTooLong {
        maximum: usize,
    },
    Protocol(ProtocolError),
    Control(ControlWireError),
    /// The session already ended.
    Ended(BrowserSessionEnd),
}

impl fmt::Display for BrowserTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported => formatter.write_str("this browser does not support WebTransport"),
            Self::Js(message) => write!(formatter, "WebTransport failed: {message}"),
            Self::MissingWelcome => {
                formatter.write_str("the host closed the session before its welcome")
            }
            Self::StreamTooLong { maximum } => {
                write!(
                    formatter,
                    "stream exceeds the protocol limit of {maximum} bytes"
                )
            }
            Self::Protocol(error) => write!(formatter, "{error}"),
            Self::Control(error) => write!(formatter, "{error}"),
            Self::Ended(BrowserSessionEnd::Closed { code, reason }) => {
                write!(formatter, "session closed ({code}): {reason}")
            }
            Self::Ended(BrowserSessionEnd::Lost(message)) => {
                write!(formatter, "session lost: {message}")
            }
        }
    }
}

impl Error for BrowserTransportError {}

impl From<ProtocolError> for BrowserTransportError {
    fn from(error: ProtocolError) -> Self {
        Self::Protocol(error)
    }
}

impl From<ControlWireError> for BrowserTransportError {
    fn from(error: ControlWireError) -> Self {
        Self::Control(error)
    }
}

impl From<JsValue> for BrowserTransportError {
    fn from(value: JsValue) -> Self {
        Self::Js(describe(&value))
    }
}

/// Accumulates one stream's chunks up to a protocol limit.
#[derive(Debug)]
struct BoundedBuffer {
    bytes: Vec<u8>,
    maximum: usize,
}

impl BoundedBuffer {
    fn new(maximum: usize) -> Self {
        Self {
            bytes: Vec::new(),
            maximum,
        }
    }

    fn push(&mut self, chunk: &[u8]) -> Result<(), BrowserTransportError> {
        if self.bytes.len() + chunk.len() > self.maximum {
            return Err(BrowserTransportError::StreamTooLong {
                maximum: self.maximum,
            });
        }
        self.bytes.extend_from_slice(chunk);
        Ok(())
    }
}

/// Bounded FIFO that drops its oldest entry when full.
fn push_bounded<T>(queue: &mut VecDeque<T>, item: T, limit: usize) {
    if queue.len() == limit {
        queue.pop_front();
    }
    queue.push_back(item);
}

mod js {
    use js_sys::{Object, Promise};
    use wasm_bindgen::prelude::*;

    #[wasm_bindgen]
    extern "C" {
        pub type WebTransport;

        #[wasm_bindgen(constructor, catch)]
        pub fn new(url: &str, options: &Object) -> Result<WebTransport, JsValue>;
        #[wasm_bindgen(method, getter)]
        pub fn ready(this: &WebTransport) -> Promise;
        #[wasm_bindgen(method, getter)]
        pub fn closed(this: &WebTransport) -> Promise;
        #[wasm_bindgen(method, getter)]
        pub fn datagrams(this: &WebTransport) -> DatagramDuplexStream;
        #[wasm_bindgen(method, getter, js_name = incomingUnidirectionalStreams)]
        pub fn incoming_unidirectional_streams(this: &WebTransport) -> ReadableStream;
        #[wasm_bindgen(method, js_name = createBidirectionalStream)]
        pub fn create_bidirectional_stream(this: &WebTransport) -> Promise;
        #[wasm_bindgen(method, catch)]
        pub fn close(this: &WebTransport, info: &Object) -> Result<(), JsValue>;

        pub type DatagramDuplexStream;
        #[wasm_bindgen(method, getter)]
        pub fn readable(this: &DatagramDuplexStream) -> ReadableStream;
        #[wasm_bindgen(method, getter)]
        pub fn writable(this: &DatagramDuplexStream) -> WritableStream;

        /// A `WebTransportBidirectionalStream`.
        pub type BidirectionalStream;
        #[wasm_bindgen(method, getter)]
        pub fn readable(this: &BidirectionalStream) -> ReadableStream;
        #[wasm_bindgen(method, getter)]
        pub fn writable(this: &BidirectionalStream) -> WritableStream;

        pub type ReadableStream;
        #[wasm_bindgen(method, catch, js_name = getReader)]
        pub fn get_reader(this: &ReadableStream) -> Result<Reader, JsValue>;

        /// A `ReadableStreamDefaultReader`.
        pub type Reader;
        #[wasm_bindgen(method)]
        pub fn read(this: &Reader) -> Promise;
        #[wasm_bindgen(method)]
        pub fn cancel(this: &Reader) -> Promise;
        #[wasm_bindgen(method, js_name = releaseLock)]
        pub fn release_lock(this: &Reader);

        pub type WritableStream;
        #[wasm_bindgen(method, catch, js_name = getWriter)]
        pub fn get_writer(this: &WritableStream) -> Result<Writer, JsValue>;

        /// A `WritableStreamDefaultWriter`.
        pub type Writer;
        #[wasm_bindgen(method)]
        pub fn write(this: &Writer, chunk: &js_sys::Uint8Array) -> Promise;
        #[wasm_bindgen(method)]
        pub fn close(this: &Writer) -> Promise;
    }
}

fn describe(value: &JsValue) -> String {
    if let Some(error) = value.dyn_ref::<js_sys::Error>() {
        return String::from(error.message());
    }
    value.as_string().unwrap_or_else(|| format!("{value:?}"))
}

/// One `reader.read()`: `Some(chunk)` or `None` at the end of the stream.
async fn read_chunk(reader: &js::Reader) -> Result<Option<JsValue>, BrowserTransportError> {
    let result = JsFuture::from(reader.read()).await?;
    if Reflect::get(&result, &"done".into())?.is_truthy() {
        return Ok(None);
    }
    Ok(Some(Reflect::get(&result, &"value".into())?))
}

/// Reads a whole byte stream, cancelling it once it exceeds `maximum`.
async fn read_bounded(
    stream: &js::ReadableStream,
    maximum: usize,
) -> Result<Vec<u8>, BrowserTransportError> {
    let reader = stream.get_reader()?;
    let mut buffer = BoundedBuffer::new(maximum);
    while let Some(chunk) = read_chunk(&reader).await? {
        if let Err(error) = buffer.push(&Uint8Array::new(&chunk).to_vec()) {
            let _ = JsFuture::from(reader.cancel()).await;
            return Err(error);
        }
    }
    Ok(buffer.bytes)
}

/// Settles a promise in the background so its rejection is never unhandled.
fn settle(promise: Promise) {
    spawn_local(async move {
        let _ = JsFuture::from(promise).await;
    });
}

#[derive(Default)]
struct Shared {
    reassembler: SnapshotReassembler,
    snapshots: VecDeque<SnapshotFrame>,
    rejections: VecDeque<CommandRejectionFrame>,
    malformed_datagrams: u64,
    end: Option<BrowserSessionEnd>,
}

/// One browser WebTransport session to a hosted match.
///
/// Background tasks (`spawn_local`) feed snapshots and command rejections into
/// queues the game drains each frame. Dropping the session closes it.
pub struct BrowserSession {
    transport: js::WebTransport,
    datagram_writer: js::Writer,
    welcome: Welcome,
    shared: Rc<RefCell<Shared>>,
}

impl BrowserSession {
    /// Opens a session at `url` (an `https://` URL at a browser match or
    /// reconnect route) and waits for the host's welcome.
    pub async fn connect(
        url: &str,
        options: BrowserConnectOptions,
    ) -> Result<Self, BrowserTransportError> {
        if !Reflect::has(&js_sys::global(), &"WebTransport".into()).unwrap_or(false) {
            return Err(BrowserTransportError::Unsupported);
        }
        let init = Object::new();
        if let Some(hash) = options.server_certificate_hash {
            let entry = Object::new();
            Reflect::set(&entry, &"algorithm".into(), &"sha-256".into())?;
            Reflect::set(&entry, &"value".into(), &Uint8Array::from(&hash[..]))?;
            Reflect::set(
                &init,
                &"serverCertificateHashes".into(),
                &Array::of1(&entry),
            )?;
        }
        let transport = js::WebTransport::new(url, &init)?;
        let shared = Rc::new(RefCell::new(Shared::default()));

        // `closed` settles exactly once; recording it first makes every later
        // failure report why the session ended.
        let closed = transport.closed();
        {
            let shared = Rc::clone(&shared);
            spawn_local(async move {
                let end = match JsFuture::from(closed).await {
                    Ok(info) => BrowserSessionEnd::Closed {
                        code: Reflect::get(&info, &"closeCode".into())
                            .ok()
                            .and_then(|code| code.as_f64())
                            .map_or(0, |code| code as u32),
                        reason: Reflect::get(&info, &"reason".into())
                            .ok()
                            .and_then(|reason| reason.as_string())
                            .unwrap_or_default(),
                    },
                    Err(error) => BrowserSessionEnd::Lost(describe(&error)),
                };
                shared.borrow_mut().end = Some(end);
            });
        }

        let opened = async {
            JsFuture::from(transport.ready()).await?;
            let streams = transport.incoming_unidirectional_streams().get_reader()?;
            let Some(first) = read_chunk(&streams).await? else {
                return Err(BrowserTransportError::MissingWelcome);
            };
            let welcome = decode_welcome(
                &read_bounded(&first.unchecked_into(), MAX_WELCOME_STREAM_BYTES).await?,
            )?;
            Ok((welcome, streams))
        };
        let (welcome, streams) = match opened.await {
            Ok(opened) => opened,
            Err(error) => {
                let _ = transport.close(&Object::new());
                return Err(error);
            }
        };

        spawn_local(pump_rejections(streams, Rc::clone(&shared)));
        spawn_local(pump_datagrams(
            transport.datagrams().readable().get_reader()?,
            Rc::clone(&shared),
        ));
        let datagram_writer = transport.datagrams().writable().get_writer()?;
        Ok(Self {
            transport,
            datagram_writer,
            welcome,
            shared,
        })
    }

    /// The welcome this session was admitted with (player id, tick rate, reconnect token).
    pub fn welcome(&self) -> Welcome {
        self.welcome
    }

    /// Reassembled snapshots received since the last call, oldest first.
    pub fn take_snapshots(&self) -> Vec<SnapshotFrame> {
        self.shared.borrow_mut().snapshots.drain(..).collect()
    }

    /// Command rejections received since the last call, oldest first.
    pub fn take_rejections(&self) -> Vec<CommandRejectionFrame> {
        self.shared.borrow_mut().rejections.drain(..).collect()
    }

    /// Datagrams dropped because they did not decode as snapshots or fragments.
    pub fn malformed_datagrams(&self) -> u64 {
        self.shared.borrow().malformed_datagrams
    }

    /// Why the session ended, once it has.
    pub fn end(&self) -> Option<BrowserSessionEnd> {
        self.shared.borrow().end.clone()
    }

    /// Sends one command datagram. Datagrams are unreliable: a write the
    /// browser drops is not an error.
    pub fn send_command(&self, sequence: u32, payload: &[u8]) -> Result<(), BrowserTransportError> {
        if let Some(end) = self.end() {
            return Err(BrowserTransportError::Ended(end));
        }
        let frame = encode_command(sequence, payload)?;
        settle(self.datagram_writer.write(&Uint8Array::from(&frame[..])));
        Ok(())
    }

    /// One reliable control exchange on a fresh bidirectional stream. The host
    /// bounds every exchange with its own deadline and resets the stream when
    /// it passes.
    pub async fn control(&self, payload: &[u8]) -> Result<ControlResponse, BrowserTransportError> {
        if let Some(end) = self.end() {
            return Err(BrowserTransportError::Ended(end));
        }
        let request = encode_control_request(payload)?;
        let stream: js::BidirectionalStream =
            JsFuture::from(self.transport.create_bidirectional_stream())
                .await?
                .unchecked_into();
        let writer = stream.writable().get_writer()?;
        JsFuture::from(writer.write(&Uint8Array::from(&request[..]))).await?;
        JsFuture::from(writer.close()).await?;
        let response = read_bounded(&stream.readable(), MAX_CONTROL_STREAM_BYTES).await?;
        Ok(decode_control_response(&response)?)
    }

    /// Closes the session with [`BROWSER_CLIENT_CLOSE_CODE`]; closing twice is a no-op.
    pub fn close(&self, reason: &str) {
        let info = Object::new();
        let _ = Reflect::set(
            &info,
            &"closeCode".into(),
            &BROWSER_CLIENT_CLOSE_CODE.into(),
        );
        let _ = Reflect::set(&info, &"reason".into(), &reason.into());
        // Closing a session that already ended throws; it is closed either way.
        let _ = self.transport.close(&info);
    }
}

impl Drop for BrowserSession {
    fn drop(&mut self) {
        self.close("client left");
    }
}

async fn pump_datagrams(reader: js::Reader, shared: Rc<RefCell<Shared>>) {
    // Ends with the session; `closed` reports why.
    while let Ok(Some(chunk)) = read_chunk(&reader).await {
        let datagram = Uint8Array::new(&chunk).to_vec();
        let mut shared = shared.borrow_mut();
        match shared.reassembler.accept(&datagram) {
            Ok(Some(snapshot)) => {
                push_bounded(
                    &mut shared.snapshots,
                    snapshot,
                    BROWSER_SNAPSHOT_QUEUE_LIMIT,
                );
            }
            Ok(None) => {}
            Err(_) => shared.malformed_datagrams += 1,
        }
    }
}

/// After the welcome, every host-opened unidirectional stream is one command rejection.
async fn pump_rejections(streams: js::Reader, shared: Rc<RefCell<Shared>>) {
    while let Ok(Some(stream)) = read_chunk(&streams).await {
        let stream: js::ReadableStream = stream.unchecked_into();
        let Ok(bytes) = read_bounded(&stream, MAX_REJECTION_STREAM_BYTES).await else {
            continue;
        };
        if let Ok(rejection) = decode_command_rejection(&bytes) {
            push_bounded(
                &mut shared.borrow_mut().rejections,
                rejection,
                BROWSER_REJECTION_QUEUE_LIMIT,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_buffer_accepts_up_to_its_limit_and_rejects_past_it() {
        let mut buffer = BoundedBuffer::new(MAX_WELCOME_STREAM_BYTES);
        buffer.push(&[1; WELCOME_BYTES]).unwrap();
        buffer.push(&[2]).unwrap();
        assert_eq!(buffer.bytes.len(), MAX_WELCOME_STREAM_BYTES);
        assert_eq!(
            buffer.push(&[3]),
            Err(BrowserTransportError::StreamTooLong {
                maximum: MAX_WELCOME_STREAM_BYTES
            })
        );
        // A rejected chunk leaves the accepted bytes intact.
        assert_eq!(buffer.bytes.len(), MAX_WELCOME_STREAM_BYTES);
    }

    #[test]
    fn welcome_stream_with_trailing_byte_fails_decoding() {
        let welcome = crate::protocol::encode_welcome(Welcome {
            player_id: 7,
            tick_hz: 30,
            max_players: 8,
            current_tick: 99,
            connection_epoch: 1,
            reconnect_token: [0xab; crate::protocol::RECONNECT_TOKEN_BYTES],
            reconnect_grace_ticks: 300,
        });
        let mut buffer = BoundedBuffer::new(MAX_WELCOME_STREAM_BYTES);
        buffer.push(&welcome).unwrap();
        assert_eq!(decode_welcome(&buffer.bytes).unwrap().tick_hz, 30);
        buffer.push(&[0]).unwrap();
        assert!(decode_welcome(&buffer.bytes).is_err());
    }

    #[test]
    fn queues_drop_the_oldest_entry_when_full() {
        let mut queue = VecDeque::new();
        for value in 0..=BROWSER_SNAPSHOT_QUEUE_LIMIT {
            push_bounded(&mut queue, value, BROWSER_SNAPSHOT_QUEUE_LIMIT);
        }
        assert_eq!(queue.len(), BROWSER_SNAPSHOT_QUEUE_LIMIT);
        assert_eq!(queue.front(), Some(&1));
        assert_eq!(queue.back(), Some(&BROWSER_SNAPSHOT_QUEUE_LIMIT));
    }

    #[test]
    fn control_stream_limit_admits_the_largest_response() {
        let response = crate::control::encode_control_response(
            true,
            &[0; crate::control::MAX_CONTROL_PAYLOAD_BYTES],
        )
        .unwrap();
        let mut buffer = BoundedBuffer::new(MAX_CONTROL_STREAM_BYTES);
        buffer.push(&response).unwrap();
        assert!(decode_control_response(&buffer.bytes).unwrap().accepted);
    }
}

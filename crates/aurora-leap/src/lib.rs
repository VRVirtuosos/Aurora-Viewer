//! Native Rust Linden Lab Event API Protocol (LEAP) implementation.
//!
//! Provides length-prefixed LLSD stream framing, in-memory event pump pub/sub registry,
//! standard LEAP management commands (`LLLeapListener`), and background I/O transports
//! for subprocess and stdio communication.

pub mod codec;
pub mod command;
pub mod pump;
pub mod transport;

#[cfg(test)]
mod tests;

use crossbeam_channel::{Receiver, Sender};
use std::io::{Read, Write};
use std::thread::JoinHandle;

pub use codec::{
    CodecError, FrameDecoder, FrameEncoder, FrameError, LeapDecoder, LeapEncoder, LeapMessage,
    COLON, DEFAULT_MAX_PAYLOAD_SIZE, MAX_PREFIX_LEN,
};
pub use command::{
    stamp_reqid, ApiMetadata, ApiRegistry, CommandError, LeapCommandHandler, LeapSession,
    OperationMetadata,
};
pub use pump::{EventPump, EventPumps, ListenerCallback, PumpError, PumpType};
pub use transport::{
    parse_command_line, spawn_reader, spawn_stderr, spawn_writer, ChildGuard, StdioTransport,
    SubprocessTransport, TransportEvent, TransportStatus,
};

/// High-level errors produced by the LEAP subsystem.
#[derive(Debug, thiserror::Error)]
pub enum LeapError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("codec error: {0}")]
    Codec(#[from] CodecError),

    #[error("command error: {0}")]
    Command(#[from] CommandError),

    #[error("pump error: {0}")]
    Pump(#[from] PumpError),

    #[error("transport error: {0}")]
    Transport(String),

    #[error("session disconnected")]
    Disconnected,
}

/// The unified LEAP manager owned by the viewer application.
///
/// Coordinates the LEAP session, event pump registry, core command dispatch,
/// and background I/O threads.
pub struct LeapManager {
    desc: String,
    session: LeapSession,

    inbound_rx: Receiver<TransportEvent>,
    #[allow(dead_code)]
    outbound_tx: Sender<Vec<u8>>,

    child_guard: Option<ChildGuard>,
    #[allow(dead_code)]
    reader_handle: Option<JoinHandle<()>>,
    #[allow(dead_code)]
    writer_handle: Option<JoinHandle<()>>,
    #[allow(dead_code)]
    stderr_handle: Option<JoinHandle<()>>,

    pumps: EventPumps,
    command_handler: LeapCommandHandler,

    is_alive: bool,
    status: TransportStatus,
    last_error: Option<String>,
}

impl LeapManager {
    /// Launch a child process and manage its standard I/O pipes.
    pub fn spawn_subprocess(cmd: &str) -> Result<Self, LeapError> {
        let transport = SubprocessTransport::spawn(cmd).map_err(LeapError::Transport)?;
        Self::init_session(
            transport.child_guard.desc().to_string(),
            LeapSession::new(),
            transport.inbound_rx,
            transport.outbound_tx,
            Some(transport.child_guard),
            Some(transport.reader_handle),
            Some(transport.writer_handle),
            Some(transport.stderr_handle),
        )
    }

    /// Attach directly to standard input/output for external test runners.
    pub fn attach_stdio() -> Result<Self, LeapError> {
        let transport = StdioTransport::attach().map_err(LeapError::Transport)?;
        Self::init_session(
            "stdio".to_string(),
            LeapSession::new(),
            transport.inbound_rx,
            transport.outbound_tx,
            None,
            Some(transport.reader_handle),
            Some(transport.writer_handle),
            None,
        )
    }

    /// Construct LeapManager over generic streams (ideal for unit testing).
    pub fn from_streams<R, W, E>(
        desc: &str,
        reader: R,
        writer: W,
        stderr: Option<E>,
    ) -> Result<Self, LeapError>
    where
        R: Read + Send + 'static,
        W: Write + Send + 'static,
        E: Read + Send + 'static,
    {
        Self::from_streams_with_session(desc, LeapSession::new(), reader, writer, stderr)
    }

    /// Construct LeapManager over generic streams with a predetermined session (for deterministic testing).
    pub fn from_streams_with_session<R, W, E>(
        desc: &str,
        session: LeapSession,
        reader: R,
        writer: W,
        stderr: Option<E>,
    ) -> Result<Self, LeapError>
    where
        R: Read + Send + 'static,
        W: Write + Send + 'static,
        E: Read + Send + 'static,
    {
        let (inbound_tx, inbound_rx) = crossbeam_channel::unbounded();
        let (outbound_tx, outbound_rx) = crossbeam_channel::unbounded();

        let reader_handle = spawn_reader(reader, inbound_tx, desc.to_string());
        let writer_handle = spawn_writer(writer, outbound_rx, desc.to_string());
        let stderr_handle = stderr.map(|e| spawn_stderr(e, desc.to_string()));

        Self::init_session(
            desc.to_string(),
            session,
            inbound_rx,
            outbound_tx,
            None,
            Some(reader_handle),
            Some(writer_handle),
            stderr_handle,
        )
    }

    fn init_session(
        desc: String,
        session: LeapSession,
        inbound_rx: Receiver<TransportEvent>,
        outbound_tx: Sender<Vec<u8>>,
        child_guard: Option<ChildGuard>,
        reader_handle: Option<JoinHandle<()>>,
        writer_handle: Option<JoinHandle<()>>,
        stderr_handle: Option<JoinHandle<()>>,
    ) -> Result<Self, LeapError> {
        let pumps = EventPumps::new();
        let mut command_handler = LeapCommandHandler::new(session.clone());

        // Set outbound sender for external events forwarded from pumps
        let tx_outbound = outbound_tx.clone();
        command_handler.set_outbound_sender(move |source, data| {
            let envelope = LeapEncoder::encode_message(source, data);
            let _ = tx_outbound.send(envelope);
        });

        // Forward any message posted to session.reply_pump across outbound_tx
        let tx_reply = outbound_tx.clone();
        let reply_pump_name = session.reply_pump.clone();
        pumps
            .obtain(&session.reply_pump)
            .listen("leap-reply-forwarder", move |data| {
                let envelope = LeapEncoder::encode_message(&reply_pump_name, data);
                let _ = tx_reply.send(envelope);
                true
            });

        // Transmit initial intro handshake frame
        let intro_frame = session.build_intro_frame();
        outbound_tx
            .send(intro_frame)
            .map_err(|_| LeapError::Disconnected)?;

        log::info!(
            target: "leap",
            "{}: initialized LEAP session. Reply pump: {}, Command pump: {}",
            desc,
            session.reply_pump,
            session.command_pump
        );

        Ok(Self {
            desc,
            session,
            inbound_rx,
            outbound_tx,
            child_guard,
            reader_handle,
            writer_handle,
            stderr_handle,
            pumps,
            command_handler,
            is_alive: true,
            status: TransportStatus::Running,
            last_error: None,
        })
    }

    /// Poll incoming LEAP messages non-blockingly.
    ///
    /// Automatically handles commands sent to the session's `command_pump`.
    /// Returns any subsystem messages destined for the viewer application.
    pub fn try_recv(&mut self) -> Option<LeapMessage> {
        while let Ok(event) = self.inbound_rx.try_recv() {
            match event {
                TransportEvent::Message(msg) => {
                    // Check if targeted at dedicated command pump
                    if msg.pump == self.session.command_pump {
                        if let Some((reply_pump, reply_data)) =
                            self.command_handler.dispatch(&msg.data, &self.pumps)
                        {
                            // If reply requested, post to destination pump
                            self.pumps.post(&reply_pump, &reply_data);
                        }
                        // Command was handled internally; continue draining
                        continue;
                    }

                    // Post to EventPumps registry so attached internal listeners fire
                    self.pumps.obtain(&msg.pump).post(&msg.data);

                    // Return to caller for viewer subsystem handling
                    return Some(msg);
                }
                TransportEvent::Disconnected => {
                    self.is_alive = false;
                    self.status = TransportStatus::Disconnected;
                    log::info!(target: "leap", "{}: transport disconnected", self.desc);
                }
                TransportEvent::ProtocolError(err) => {
                    self.is_alive = false;
                    self.status = TransportStatus::Error;
                    self.last_error = Some(err.clone());
                    log::error!(target: "leap", "{}: transport protocol error: {err}", self.desc);
                    if let Some(child) = &mut self.child_guard {
                        child.terminate();
                    }
                }
            }
        }
        None
    }

    /// Post an event payload to a named viewer event pump.
    ///
    /// Distributes to all registered internal listeners and external LEAP subscribers.
    pub fn post(&self, pump: &str, data: aurora_llsd::Llsd) {
        self.pumps.obtain(pump).post(&data);
    }

    /// Access the underlying EventPumps registry.
    pub fn pumps(&self) -> &EventPumps {
        &self.pumps
    }

    /// Get current session identifiers.
    pub fn session(&self) -> &LeapSession {
        &self.session
    }

    /// Get current reply pump UUID.
    pub fn reply_pump(&self) -> &str {
        &self.session.reply_pump
    }

    /// Get current command pump UUID.
    pub fn command_pump(&self) -> &str {
        &self.session.command_pump
    }

    /// Access the outbound byte transmission channel.
    pub fn outbound_tx(&self) -> &Sender<Vec<u8>> {
        &self.outbound_tx
    }

    /// Access the API introspection registry.
    pub fn apis(&self) -> &ApiRegistry {
        self.command_handler.apis()
    }

    /// Mutably access the API introspection registry.
    pub fn apis_mut(&mut self) -> &mut ApiRegistry {
        self.command_handler.apis_mut()
    }

    /// Register a subsystem API into the introspection registry.
    pub fn register_api(&mut self, api: ApiMetadata) {
        self.command_handler.register_api(api);
    }

    /// Check if the transport connection is still alive.
    pub fn is_alive(&self) -> bool {
        self.is_alive
    }

    /// Current transport status.
    pub fn status(&self) -> TransportStatus {
        self.status
    }

    /// Last reported protocol or transport error, if any.
    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    /// Graceful shutdown and resource reclamation.
    pub fn shutdown(&mut self) {
        if !self.is_alive {
            return;
        }
        self.is_alive = false;
        self.status = TransportStatus::Disconnected;

        if let Some(mut child) = self.child_guard.take() {
            child.terminate();
        }

        log::info!(target: "leap", "{}: LEAP manager shut down cleanly", self.desc);
    }
}

impl Drop for LeapManager {
    fn drop(&mut self) {
        self.shutdown();
    }
}
